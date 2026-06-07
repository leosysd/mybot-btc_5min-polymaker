const path = require("path");
const fs = require("fs");

const releaseBin = path.join(__dirname, "polymaker");
const sourceBin = path.join(__dirname, "target", "release", "polymaker");
const bin = fs.existsSync(releaseBin) ? releaseBin : sourceBin;
const runDir = path.join(__dirname, "run");
fs.mkdirSync(runDir, { recursive: true });

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
    POLYMAKER_HOME: __dirname,
  },
  error_file: path.join(runDir, `${name}-error.log`),
  out_file: path.join(runDir, `${name}-out.log`),
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
