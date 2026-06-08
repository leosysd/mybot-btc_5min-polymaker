use crate::config::Config;
use crate::ipc::QuoteIntent;
use crate::AppResult;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use polymarket_client_sdk_v2::auth::{Credentials, Uuid};
use polymarket_client_sdk_v2::clob::types::{Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::POLYGON;
use secrecy::ExposeSecret;
use std::str::FromStr;
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
        let response = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Buy)
            .price(price)
            .size(size)
            .post_only(true)
            .build_sign_and_post(&self.signer)
            .await?;

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
