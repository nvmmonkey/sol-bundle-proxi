# 🚀 Jito Proxy — High-Throughput gRPC/HTTP Bundle Relay for Solana

A Go proxy that fans Solana [Jito](https://jito.wtf) bundle submissions out across
all eight block-engine regions over multiplexed gRPC, with per-proxy/per-region rate
limiting, automatic rate-limit backoff, and SOCKS5 proxy or IP-subnet source rotation.

Built to sustain **~28,000 concurrent in-flight bundles** against a measured 0.6 s RTT.
See [`PERFORMANCE_OPTIMIZATIONS.md`](PERFORMANCE_OPTIMIZATIONS.md) and
[`OPTIMIZATION_SUMMARY.md`](OPTIMIZATION_SUMMARY.md) for the tuning work and rationale.

### Highlights

- **Dual transport** — transparent gRPC relay plus an HTTP reverse proxy, or both at once
- **Region fan-out** — one dedicated port per block-engine region (8011–8019), or round-robin
- **Rate-limit backoff** — parses Jito's `Retry after Nms`, pauses that region, auto-resumes
- **Source rotation** — SOCKS5 proxy pools or direct IP-subnet binding for egress diversity
- **Fire-and-forget mode** — optional, trades delivery confirmation for latency
- **Lazy init** — fast startup; connection pools warm on demand

## 📁 Project Structure

```
sol-bundle-proxi/
├── main.go                      # Proxy server: gRPC relay, HTTP reverse proxy, rate limiting
├── cmd/proxy-tester/main.go     # Standalone SOCKS5 proxy health-checker
├── config.example.yaml          # Annotated config — copy to config.yaml
├── proxy-list.example.txt       # SOCKS5 proxy list format
├── proxy-subnet.example.txt     # IP-subnet (CIDR) binding format
├── start-proxy.sh               # Launch helper (ulimit tuning + flags)
├── filter_proxies.sh            # Bulk proxy liveness filter
├── ecosystem.config.js          # PM2 process definition
├── PERFORMANCE_OPTIMIZATIONS.md # Throughput tuning notes
└── OPTIMIZATION_SUMMARY.md      # Change log of optimizations
```

---

## 🔐 Security & Configuration

This repository contains **no credentials**. Before running, create your own local files
(all are listed in `.gitignore` and will never be committed):

| File | Purpose | How to create |
|---|---|---|
| `config.yaml` | Ports, regions, limits | `cp config.example.yaml config.yaml` |
| `proxy-list.txt` | SOCKS5 egress pool | `cp proxy-list.example.txt proxy-list.txt` |
| `proxy-subnet.txt` | IP-binding CIDRs | `cp proxy-subnet.example.txt proxy-subnet.txt` |
| `keypair.txt` | **Secret.** Base58 Solana private key, only for Jito auth mode | Export from your wallet |

> ⚠️ `keypair.txt` is a private key. It is read at runtime only, is never logged in full
> (only a truncated pubkey is printed), and is git-ignored. Never commit it. The proxy
> relays **already-signed** transactions and does not sign on your behalf — auth mode is
> solely for identifying a searcher to the block engine.
>
> All IP addresses in this repo are [RFC 5737](https://datatracker.ietf.org/doc/html/rfc5737)
> documentation placeholders (`203.0.113.x`) or private ranges — substitute your own.

---

## 🔧 Setup

### Step 1: Clone and build

```bash
git clone https://github.com/nvmmonkey/sol-bundle-proxi.git
cd sol-bundle-proxi
go mod download

# Linux (deployment target — uses setrlimit for high FD counts)
GOOS=linux GOARCH=amd64 go build -o gproxy main.go

# Proxy health-checker
go build -o proxy-tester ./cmd/proxy-tester
```

> **Note:** `main.go` raises `RLIMIT_NOFILE` via `syscall.Setrlimit`, which is
> Linux-only. Cross-compile with `GOOS=linux` (as above) or build on Linux.

### Step 2: Create your config

```bash
cp config.example.yaml config.yaml
cp proxy-list.example.txt proxy-list.txt     # or proxy-subnet.txt for IP binding
```

Then edit `config.yaml` — it is fully annotated.

### Step 3: Dependencies (already pinned in `go.mod`)

This project depends on [`weeaa/jito-go`](https://github.com/weeaa/jito-go)
(Apache-2.0) for the Jito block-engine protobufs and searcher auth service,
pinned to a specific upstream commit:

```
github.com/weeaa/jito-go v0.1.1-0.20250730155021-3fe69f58b988
```


## ⚙️ Configuration Reference

All runtime behaviour is driven by `config.yaml` (loaded in `main.go`); there are no
compile-time constants to edit. Copy the annotated template and adjust:

```bash
cp config.example.yaml config.yaml
```

Key settings:

| Key | Meaning |
|---|---|
| `proxy_mode` | `"http"`, `"grpc"`, or `"both"` |
| `grpc_transparent_port` | Round-robin gRPC port across all regions |
| `grpc_use_region_ports` | Expose one dedicated port per region (8011–8019) |
| `grpc_port_mapping` | Port → region index map |
| `regions` | Block-engine hostnames to relay to |
| `max_concurrent_bundles` | In-flight bundle cap (default 28000) |
| `fire_and_forget` | Return immediately instead of awaiting confirmation |
| `enable_rate_limit_backoff` | Honour Jito's `Retry after Nms` per region |
| `proxy_success_cooldown_ms` | Per-proxy pacing (1000 ms ≈ 5 req/s/proxy) |
| `proxy_list_file` | Path to the SOCKS5 pool |
| `solana_rpc_url` | Primary RPC (local node recommended) |

### Egress source files

Pick **one** strategy — SOCKS5 pool or IP binding:

**`proxy-list.txt`** — SOCKS5 proxies, one per line:

```text
# Format: IP:PORT  or  IP:PORT:USERNAME:PASSWORD
127.0.0.1:1080
127.0.0.1:1081
```

**`proxy-subnet.txt`** — bind directly to local IPs by CIDR:

```text
# Format: CIDR notation
10.0.0.0/24
10.0.1.0/24
```

Filter a large pool down to the live entries with `./filter_proxies.sh`, then validate
with the bundled checker:

```bash
go build -o proxy-tester ./cmd/proxy-tester && ./proxy-tester
```

### Auth mode (optional)

Jito auth is off by default (no-auth avoids per-key rate-limit tracking). To enable it,
place a base58 Solana private key in `keypair.txt` — git-ignored, read at runtime, never
logged in full. The proxy relays already-signed transactions and never signs for you.

---

## 🎮 Usage Modes

### **Mode 1: HTTP Only (Original Proxy)**
```yaml
# config.yaml
proxy_mode: http
```
- Runs HTTP reverse proxy on ports 6001-6009
- Each port maps to a different Jito region
- Good for existing tools that expect HTTP endpoints

### **Mode 2: gRPC Only (Bundle Sending)**
```yaml
# config.yaml
proxy_mode: grpc
```
- Runs gRPC bundle sender on port 9000
- Accepts POST requests with transaction bundles
- Automatically rotates proxies and regions

### **Mode 3: Both (Recommended)**
```yaml
# config.yaml
proxy_mode: both
```
- Runs both HTTP proxy AND gRPC service
- Maximum flexibility
- HTTP on ports 6001-6009
- gRPC API on port 9000

---

## 🏃 Running the Service

### Option A: Direct Run (Development)

```bash
# Build
go build -o gproxy main.go

# Run (reads ./config.yaml)
./gproxy
```

### Option B: Using PM2 (Production)

This repo ships a tuned [`ecosystem.config.js`](ecosystem.config.js) — fork mode,
16 GB memory ceiling, aggressive auto-restart, raw rotated logs. Paths are relative,
so run PM2 from the repo root.

Run with PM2:
```bash
# First build
GOOS=linux GOARCH=amd64 go build -o gproxy main.go

# Start with PM2
pm2 start ecosystem.config.js

# Monitor
pm2 logs gproxy
pm2 monit

# Stop
pm2 stop gproxy

# Restart
pm2 restart gproxy
```

---

## 📡 Using the gRPC Service

### Send Bundle via HTTP API

```bash
# Example: Send a bundle
curl -X POST http://localhost:9000/ \
  -H "Content-Type: application/json" \
  -d '{
    "transactions": [
      "BASE58_ENCODED_TRANSACTION_1",
      "BASE58_ENCODED_TRANSACTION_2"
    ]
  }'
```

### Response Format

```json
{
  "success": true,
  "message": "Bundle发送成功"
}
```

---

## 🔍 Testing & Verification

### Test HTTP Proxy (Port 6001 = New York)

```bash
# Test connection
curl -x socks5://your_proxy:port https://ny.mainnet.block-engine.jito.wtf:6001/health
```

### Test gRPC Service

```bash
# Health check (will fail if no transactions provided, but shows it's running)
curl -X POST http://localhost:9000/
```

### Monitor Logs

Look for these log messages:
- `✅ 已通过代理 X.X.X.X 连接到 NewYork (无认证)`
- `✅ Bundle发送成功`
- `gRPC统计 - 总请求: X, 成功: Y, 失败: Z`

---

## ⚙️ Configuration Options

### Choose Connection Method

The code automatically chooses:
1. **SOCKS5 Proxy** (if `proxy-list.txt` exists and has proxies)
2. **IP Binding** (if `proxy-subnet.txt` exists)
3. **Direct Connection** (fallback)

### Adjust Retry Logic

In `main.go`, find:
```go
err := h.grpcMgr.SendBundle(ctx, txns, 3)  // Change "3" to adjust retries
```

### Change Regions

Edit `JitoRegions` array in code:
```go
var JitoRegions = []JitoRegion{
    {"NewYork", "ny.mainnet.block-engine.jito.wtf:443"},
    {"London", "london.mainnet.block-engine.jito.wtf:443"},
    // Add or remove regions as needed
}
```

---

## 🐛 Troubleshooting

### "No proxies available"
- Check `proxy-list.txt` exists and has valid proxies
- Format: `IP:PORT` or `IP:PORT:USER:PASS`

### "Cannot bind to IP"
- Check `proxy-subnet.txt` has valid CIDR subnets
- Ensure your network interface has IPs in those subnets
- Run `ip addr` (Linux) or `ipconfig` (Windows) to verify

### "gRPC connection failed"
- Verify proxies are working: `curl -x socks5://proxy:port https://google.com`
- Check firewall settings
- Try direct connection mode (comment out proxy-list.txt)

### "Context deadline exceeded"
- Increase timeout in code: `context.WithTimeout(ctx, 30*time.Second)`
- Check network latency to Jito endpoints

---

## 📊 Monitoring

### Log Output

You'll see periodic stats:
```
gRPC统计 - 总请求: 150, 成功: 145, 失败: 5
端口 6001 - 总请求: 1250, 成功: 1240, 失败: 10
```

### PM2 Monitoring
```bash
pm2 monit      # Real-time monitoring
pm2 logs       # View logs
pm2 info gproxy  # Detailed info
```

---

## 🔐 Security Best Practices

1. **Never commit sensitive files:**
   - `.env`
   - `proxy-list.txt`
   - Private keys

2. **Use environment variables:**
   ```bash
   export JITO_AUTH_KEY="your_key"
   ```

3. **Verify proxy trustworthiness:**
   - Only use proxies from trusted providers
   - They can see all your traffic

4. **Monitor for leaks:**
   - Check logs for exposed keys
   - Use `grep` to search for sensitive data

---

## 📈 Performance Tuning

### Increase File Descriptors (Linux)
```bash
ulimit -n 65535
```

### Adjust Go Settings
```go
runtime.GOMAXPROCS(runtime.NumCPU())  // Already in code
```

### Connection Pool Tuning
In code, adjust:
```go
MaxIdleConns:        5000,  // Increase for more concurrent connections
MaxIdleConnsPerHost: 2000,
```

---

## 🎯 Quick Start Checklist

- [ ] `git clone` and `go mod download`
- [ ] `cp config.example.yaml config.yaml` and review the settings
- [ ] `cp proxy-list.example.txt proxy-list.txt` (or create `proxy-subnet.txt`)
- [ ] Set `proxy_mode` in `config.yaml` (`http` / `grpc` / `both`)
- [ ] (Optional) add `keypair.txt` for Jito auth mode — never commit it
- [ ] Build: `GOOS=linux GOARCH=amd64 go build -o gproxy main.go`
- [ ] Validate the proxy pool: `go build -o proxy-tester ./cmd/proxy-tester && ./proxy-tester`
- [ ] Run: `./gproxy` (or `pm2 start ecosystem.config.js`)
- [ ] Test gRPC: `curl -X POST http://localhost:9000/`
- [ ] Monitor logs for success/failure

---

## 🚨 Important Notes

1. **No-Auth Mode = Lower Priority**
   - Jito may rate limit or deprioritize no-auth bundles
   - Consider getting official Jito API access for production

2. **Rate Limit Bypass Ethics**
   - Use responsibly
   - Don't abuse Jito's infrastructure
   - Consider their ToS

3. **Proxy vs IP Binding**
   - **Proxies**: Better for bypassing rate limits (different IPs)
   - **IP Binding**: Better if you own multiple IPs on same server

4. **Region Selection**
   - Code automatically rotates through regions
   - Choose regions closest to you for lower latency

---

## 📞 Need Help?

Common issues and solutions:
- Build errors → Run `go mod tidy`
- Import errors → Run `go get` again
- Port conflicts → Change port numbers in code
- Proxy failures → Test proxies individually first

---

Good luck with your MEV operations! 🎰


客户端永远成功的原因：
# Fire-and-Forget模式 (true = 立即返回，false = 等待确认)
fire_and_forget: true
代码逻辑如下：因为代理程序在还没尝试发送给 Jito 之前，就先告诉客户端“我收到了，我会处理的”，所以客户端永远看不到失败。
	if config.FireAndForget {
		// 1. 开启一个后台协程去真正发送交易（客户端不需要等这个）
		go h.sendBundleAsyncWithRetry(txns, region)

		// 2. 立即构造一个“假”的成功响应
		resp := JitoRPCResponse{
			JSONRPC: "2.0",
			ID:      rpcReq.ID,
			Result:  "bundle_submitted", // <--- 无论后台成功失败，这里固定返回成功
		}
		
		// 3. 发送给客户端
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		
		// 4. 统计数据 +1 成功
		atomic.AddUint64(&globalStats.success, 1)
	}


go build -o gproxy main.go


fail 计数器的 5 个触发场景
   1️⃣ 连接池为空（第 1208 行）
   if clientsLen == 0 {    atomic.AddUint64(&p.debugCounters.failGet, 1)    return nil, fmt.Errorf("no clients in pool")}
   触发条件：连接池没有任何 client
   可能性：几乎不可能（你有3000个client）
   2️⃣ 500ms 超时（第 1228 行）⭐ 主要原因
   Sin
   if time.Since(startTime) > timeout {  // 500ms    atomic.AddUint64(&p.debugCounters.failGet, 1)    return nil, fmt.Errorf("timeout: no available clients after %v", timeout)}
   触发条件：在 500ms 内没有找到可用代理
   场景：
   请求到达时，大量代理都在冷却中
   采样了多批（maxHops=200）代理，但都被 AllowNow() 拒绝
   反复等待后仍然超过 500ms
   3️⃣ 队列连续3次为空（第 1283 行）
   if consecutiveEmptyLoops >= 3 {    atomic.AddUint64(&p.debugCounters.failGet, 1)    return nil, fmt.Errorf("queue empty after %d attempts", consecutiveEmptyLoops)}
   触发条件：连续3次 PopReady 都没有拿到代理
   可能性：极低（你有3000个代理在队列中）
   4️⃣ 剩余时间不足以等待（第 1305 行）⭐ 次要原因
   if delta > remainingTime {    atomic.AddUint64(&p.debugCounters.failGet, 1)    return nil, fmt.Errorf("timeout: next available in %v, remaining %v", delta, remainingTime)}
   触发条件：
   已经等待了一段时间（比如 450ms）
   队列顶部最早的代理还需要 100ms 才可用
   但剩余时间只有 50ms (500ms - 450ms)
   delta (100ms) > remainingTime (50ms) → fail
   5️⃣ Fallback 逻辑失败（第 1353 行）
   // Fallback 扫描32个client后仍然失败atomic.AddUint64(&p.debugCounters.failGet, 1)return nil, fmt.Errorf("no available clients (all locked or in cooldown)")
   触发条件：readyQueue 为 nil，走 fallback 逻辑
   可能性：不会触发（你的代码有 readyQueue）

Success 统计的完整逻辑
   1. 数据结构
   有两个独立的统计系统：
   A. RegionStats（区域统计）- 用于最终输出表格
   type RegionStats struct {    totalRequests uint64  // 总请求数（成功+失败）    successCount  uint64  // 成功次数    rttSumUs      uint64  // RTT 总和（微秒）    rttMinUs      uint64  // 最小 RTT    rttMaxUs      uint64  // 最大 RTT}
   B. globalStats（全局统计）- 仅用于内部跟踪
   type Stats struct {    success     uint64  // 全局成功计数（未在表格中显示）    failed      uint64  // 全局失败计数    retried     uint64  // 重试成功计数}
   2. Success 记录的 3 个入口点
   
   入口3：实际发送成功（第 2544-2550 行）
      // 在 sendBundleAsyncWithRetry 中
      bundleResp, startTime, bundleErr := client.SendBundle(txns)
      rtt := time.Since(startTime).Seconds() * 1000

      if bundleErr == nil {
         // ✅ 记录到 RegionStats（用于表格输出）
         if val, ok := regionStatsMap.Load(region.Name); ok {
            val.(*RegionStats).RecordRequest(true, rtt)  // ✅ success++, total++
         }
         
         // ✅ 记录到 globalStats
         atomic.AddUint64(&globalStats.success, 1)  // ✅ globalStats++
         
         if attempt > 0 {
            atomic.AddUint64(&globalStats.retried, 1)
         }
         return
      }
   3. RegionStats 的记录逻辑（第 1521 行）
      func (rs *RegionStats) RecordRequest(success bool, rttMs float64) {
         // ✅ 无论成功失败，total++ 
         atomic.AddUint64(&rs.totalRequests, 1)
         
         // ✅ 只有成功才 success++
         if success {
            atomic.AddUint64(&rs.successCount, 1)
         }
         
         // ✅ 记录 RTT 数据
         rttUs := uint64(rttMs * 1000)
         atomic.AddUint64(&rs.rttSumUs, rttUs)
         // ... 更新 min/max
      }

   🎯 关键理解
   表格中的 count 是什么？
   count = totalRequests = 成功数 + 失败数
   在代码中：
   第 1634 行：total, success, ... := rs.GetAndReset()
   第 1665 行：输出 total 作为 count
   表格中的 succ (%) 是什么？
   succ (%) = success / total * 100
   在代码中：
   第 1661 行：successRate := float64(stat.success) / float64(stat.total) * 100
   什么时候记录成功？
   只在一个地方记录到 RegionStats：
   ✅ client.SendBundle() 返回 nil 错误（第 2544 行）
   ✅ 调用 RecordRequest(true, rtt)
   注意：
   Fire-and-Forget 模式：第 2449 行的 globalStats.success++ 不影响表格输出
   同步模式：第 2465 行的 globalStats.success++ 不影响表格输出
   表格只显示 RegionStats，它只在实际发送成功时记录
   📈 你的日志解读
   | NewYork(8011) | 163.39 | 0.04 | 392.05 | 93.23 | 6216 |
   count = 6216：5秒内总共尝试发送了 6216 次（成功+失败）
   succ (%) = 93.23%：其中 93.23% 成功 = 约 5795 次成功
   失败：6216 - 5795 = 约 421 次失败
   这个失败数 是指 client.SendBundle() 返回错误，可能原因：
   Jito 服务器拒绝
   网络错误
   代理连接失败
   超时
   与 debug 中的 fail=8654 不同：
   debug fail = GetAvailableClient() 失败（拿不到代理）
   表格中的失败 = SendBundle() 失败（拿到代理但发送失败）

   📋 详细的成功条件链
      1️⃣ gRPC 调用成功（第 1029 行）
      resp, err := c.searcherService.SendBundle(c.ctx, &jito_pb.SendBundleRequest{Bundle: bundle})
      resp, err := c.searcherService.SendBundle(c.ctx, &jito_pb.SendBundleRequest{Bundle: bundle})

      err == nil 意味着：
      ✅ Jito 服务器接收了这个 bundle
      ✅ 服务器返回了一个 UUID（bundleResp.GetUuid()）
      ✅ 没有返回错误码或错误消息