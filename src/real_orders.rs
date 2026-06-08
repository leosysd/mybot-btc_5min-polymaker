use crate::config::Config;
use crate::ipc::QuoteIntent;
use crate::AppResult;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use polymarket_client_sdk_v2::auth::{Credentials, Uuid};
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::clob::types::{Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::POLYGON;
use secrecy::ExposeSecret;
use std::str::FromStr;
use std::time::Instant;
use tokio::runtime::Runtime;

pub struct RealOrderClient {
    runtime: Runtime,
    inner: RealOrderClientInner,
}

struct RealOrderClientInner {
    signer: PrivateKeySigner,
    client: polymarket_client_sdk_v2::clob::Client<
        polymarket_client_sdk_v2::auth::state::Authenticated<
            polymarket_client_sdk_v2::auth::Normal,
        >,
    >,
}

#[derive(Debug, Clone)]
pub struct RealOrderAck {
    pub order_id: String,
    pub status: String,
    pub build_ms: u128,
    pub sign_ms: u128,
    pub post_ms: u128,
    pub total_ms: u128,
}

#[derive(Debug, Clone)]
pub struct CancelOrderAck {
    pub canceled: bool,
    pub reason: Option<String>,
}

impl CancelOrderAck {
    pub fn is_terminal_noop(&self) -> bool {
        if self.canceled {
            return true;
        }
        let Some(reason) = &self.reason else {
            return false;
        };
        let reason = reason.to_ascii_lowercase();
        [
            "already",
            "cancel",
            "canceled",
            "cancelled",
            "filled",
            "matched",
            "closed",
            "not found",
            "not open",
            "not active",
            "does not exist",
        ]
        .iter()
        .any(|needle| reason.contains(needle))
    }
}

#[derive(Debug, Clone)]
pub struct UserWsCredentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

impl RealOrderClient {
    pub fn connect(cfg: &Config) -> AppResult<Self> {
        cfg.ensure_real_order_config()?;
        let runtime = Runtime::new()?;
        let inner = runtime.block_on(RealOrderClientInner::connect(cfg))?;
        Ok(Self { runtime, inner })
    }

    pub fn place_buy_limit(&self, quote: &QuoteIntent) -> AppResult<RealOrderAck> {
        self.runtime.block_on(self.inner.place_buy_limit(quote))
    }

    pub fn cancel_order(&self, order_id: &str) -> AppResult<CancelOrderAck> {
        self.runtime.block_on(self.inner.cancel_order(order_id))
    }

    /// Cancel EVERY open order on the account (authoritative cleanup of orphans
    /// that aren't in our local state). Returns how many were canceled.
    pub fn cancel_all(&self) -> AppResult<usize> {
        self.runtime.block_on(self.inner.cancel_all())
    }

    /// Fetch the order ids the exchange currently considers open for this
    /// account. Used to reconcile against our local resting map.
    pub fn open_order_ids(&self) -> AppResult<Vec<String>> {
        self.runtime.block_on(self.inner.open_order_ids())
    }

    /// Keep one hot connection to the CLOB host alive (Cloudflare cuts idle
    /// connections after ~90s), so the next order reuses it and skips the
    /// TCP+TLS handshake. Best called shortly before a burst of orders.
    pub fn prewarm(&self) {
        self.runtime.block_on(self.inner.prewarm());
    }

    /// Warm the SDK metadata cache (tick_size / neg_risk / fee) for a token so
    /// the first order's `build` hits cache instead of 3 cold serial API calls.
    pub fn prime_token(&self, token_id: &str) {
        self.runtime.block_on(self.inner.prime_token(token_id));
    }

    pub fn user_ws_credentials(&self) -> UserWsCredentials {
        self.inner.user_ws_credentials()
    }
}

impl RealOrderClientInner {
    async fn connect(cfg: &Config) -> AppResult<Self> {
        let signer =
            PrivateKeySigner::from_str(cfg.poly_private_key.trim())?.with_chain_id(Some(POLYGON));
        let mut auth = Client::new(&cfg.polymarket_clob_host, ClobConfig::default())?
            .authentication_builder(&signer)
            .signature_type(parse_signature_type(&cfg.poly_signature_type)?);

        if !cfg.poly_funder_address.trim().is_empty() {
            auth = auth.funder(Address::from_str(cfg.poly_funder_address.trim())?);
        }

        if has_l2_credentials(cfg) {
            auth = auth.credentials(Credentials::new(
                Uuid::parse_str(cfg.poly_api_key.trim())?,
                cfg.poly_secret.clone(),
                cfg.poly_passphrase.clone(),
            ));
        }

        let client = auth.authenticate().await?;
        Ok(Self { signer, client })
    }

    async fn place_buy_limit(&self, quote: &QuoteIntent) -> AppResult<RealOrderAck> {
        if quote.token_id.trim().is_empty() {
            return Err(format!("quote {} missing token_id", quote.quote_id).into());
        }
        let token_id = U256::from_str(quote.token_id.trim())?;
        let price = Decimal::from_str(&format!("{:.2}", quote.price))?;
        let size = Decimal::from_str(&format!("{:.2}", quote.size))?;

        // Split into build → sign → post so each phase is timed independently.
        // build hits the metadata cache (warm via prime_token), sign is local
        // EIP-712, post is the round-trip to Polymarket (through Cloudflare).
        let tb = Instant::now();
        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Buy)
            .price(price)
            .size(size)
            .post_only(true)
            .build()
            .await?;
        let build_ms = tb.elapsed().as_millis();

        let tsg = Instant::now();
        let signed = self.client.sign(&self.signer, order).await?;
        let sign_ms = tsg.elapsed().as_millis();

        let tp = Instant::now();
        let response = self.client.post_order(signed).await?;
        let post_ms = tp.elapsed().as_millis();
        let total_ms = build_ms + sign_ms + post_ms;
        eprintln!("[ORDER_LAT] build={build_ms}ms sign={sign_ms}ms post={post_ms}ms 总={total_ms}ms");

        if !response.success {
            return Err(format!(
                "Polymarket order rejected: {}",
                response
                    .error_msg
                    .unwrap_or_else(|| "unknown error".to_string())
            )
            .into());
        }

        Ok(RealOrderAck {
            order_id: response.order_id,
            status: format!("{:?}", response.status),
            build_ms,
            sign_ms,
            post_ms,
            total_ms,
        })
    }

    async fn cancel_order(&self, order_id: &str) -> AppResult<CancelOrderAck> {
        let response = self.client.cancel_order(order_id).await?;
        if response.canceled.iter().any(|id| id == order_id) {
            return Ok(CancelOrderAck {
                canceled: true,
                reason: None,
            });
        }
        Ok(CancelOrderAck {
            canceled: false,
            reason: response.not_canceled.get(order_id).cloned(),
        })
    }

    async fn cancel_all(&self) -> AppResult<usize> {
        let response = self.client.cancel_all_orders().await?;
        Ok(response.canceled.len())
    }

    async fn open_order_ids(&self) -> AppResult<Vec<String>> {
        let request = OrdersRequest::builder().build();
        let page = self.client.orders(&request, None).await?;
        Ok(page.data.iter().map(|order| order.id.to_string()).collect())
    }

    async fn prewarm(&self) {
        let t = Instant::now();
        match self.client.ok().await {
            Ok(_) => eprintln!("[ORDER_PREWARM] warm in {}ms", t.elapsed().as_millis()),
            Err(err) => eprintln!("[ORDER_PREWARM] failed: {err}"),
        }
    }

    async fn prime_token(&self, token_id: &str) {
        let Ok(tid) = U256::from_str(token_id.trim()) else {
            return;
        };
        let t = Instant::now();
        // Serial (not concurrent): keep the pool to a single hot connection so
        // warm and post share it; concurrent calls open extras that CF later
        // cuts, causing random cold-connection latency spikes.
        let s = Instant::now();
        let _ = self.client.tick_size(tid).await;
        let ts = s.elapsed().as_millis();
        let s = Instant::now();
        let _ = self.client.neg_risk(tid).await;
        let nr = s.elapsed().as_millis();
        let s = Instant::now();
        let _ = self.client.fee_rate_bps(tid).await;
        let fee = s.elapsed().as_millis();
        let tail = &token_id[token_id.len().saturating_sub(8)..];
        eprintln!(
            "[CACHE_PREWARM] token=..{tail} tick_size={ts}ms neg_risk={nr}ms fee={fee}ms total={}ms",
            t.elapsed().as_millis()
        );
    }

    fn user_ws_credentials(&self) -> UserWsCredentials {
        let credentials = self.client.credentials();
        UserWsCredentials {
            api_key: credentials.key().to_string(),
            secret: credentials.secret().expose_secret().to_string(),
            passphrase: credentials.passphrase().expose_secret().to_string(),
        }
    }
}

fn has_l2_credentials(cfg: &Config) -> bool {
    !cfg.poly_api_key.trim().is_empty()
        && !cfg.poly_secret.trim().is_empty()
        && !cfg.poly_passphrase.trim().is_empty()
}

fn parse_signature_type(raw: &str) -> AppResult<SignatureType> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "eoa" | "0" => Ok(SignatureType::Eoa),
        "proxy" | "1" => Ok(SignatureType::Proxy),
        "gnosis" | "gnosis_safe" | "safe" | "2" => Ok(SignatureType::GnosisSafe),
        "poly1271" | "poly_1271" | "3" => Ok(SignatureType::Poly1271),
        other => Err(format!("unknown POLY_SIGNATURE_TYPE: {other}").into()),
    }
}
