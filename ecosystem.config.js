const path = require("path");

const bin = path.join(__dirname, "target", "release", "polymaker");

const app = (name, role) => ({
  name,
  script: bin,
  args: role,
  cwd: __dirname,
  interpreter: "none",
  instances: 1,
  exec_mode: "fork",
  autorestart: true,
  watch: false,
  max_restarts: 100,
  min_uptime: "3s",
  restart_delay: 250,
  kill_timeout: 2000,
  env: {
    RUST_BACKTRACE: "1",
  },
  error_file: path.join(__dirname, "run", `${name}-error.log`),
  out_file: path.join(__dirname, "run", `${name}-out.log`),
  merge_logs: true,
  time: true,
});

module.exports = {
  apps: [
    app("polymaker-quote-engine", "quote-engine"),
    app("polymaker-order-gateway", "order-gateway"),
    app("polymaker-risk-ledger", "risk-ledger"),
    app("polymaker-collector", "collector"),
  ],
};
