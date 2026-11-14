module.exports = {
  apps: [{
    name: "gproxy",
    script: "./gproxy",
    exec_mode: "fork",
    interpreter: "none",
    cwd: ".",
    watch: false,
    max_memory_restart: "16G",
    exp_backoff_restart_delay: 0,
    kill_timeout: 1000,
    wait_ready: false,
    listen_timeout: 0,
    instances: 1,
    autorestart: true,
    max_restarts: 80000,
    restart_delay: 0,
    min_uptime: 100,
    force: true,
    stop_exit_codes: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    ignore_watch: ["node_modules", "logs"],
    error_file: "/dev/null",
    out_file: "/dev/null",
    merge_logs: true,
    log_date_format: "YYYY-MM-DD HH:mm:ss",
    env: {
      NODE_ENV: "production"
    },
    log_type: "raw",
    max_size: "10M",
    max_logs: 10,
    compress: true,
    // cron_restart: "*/5 * * * *",   // 每5分钟自动重启一次
    watch_delay: 1000,
    shutdown_with_message: false,   // 不等待优雅退出
    treekill: true                 // 确保杀死所有子进程
  }]
}

