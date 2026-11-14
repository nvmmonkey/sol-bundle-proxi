package main

// ==================== Jito HTTP Proxy - Production Ready + FastJSON ====================
//
// 🎯 Optimizations Applied:
//   - ✅ FastJSON (jsoniter) - 3-4ms savings per request
//   - Transport pooling (reuse connections, prevent port exhaustion)
//   - Active/Passive cooldown system
//   - Rate limit detection and backoff
//   - Fire-and-forget mode
//   - Connection reuse for 10k-60k TPS
//   - Lock-free state tracking
//   - ✅ FIXED: Restored gRPC connection observer to prevent goroutine leaks
//  2025年11月23日 22:36 完成主流程，第一个能用的版本，bbi 账号上了不少单子，socks5 代理模式
//  2025年11月24日 00:46 增加统计功能，可以通过 pm2 logs 查看统计信息
//  2025年11月24日 11:26 增加统计频率，改为 5s 输出一次 
//  2025年11月24日 用堆实现的 ReadyQueue，75% 代理可用版本，每 5 秒输出一行各 region 成功建好的 gRPC client 数
//  2025年11月24日 11:36 因为发送请求非常少，增加 debug 功能，
		// gRPC debug: NewYork miss=139619 limited=1350 wait=356793(avg2.18ms) fail=114783
		// 调试计数（单个 region，5 秒内）：
		// miss≈140k：PopReady 拿到的代理对应的 client nil 或不健康，直接被丢回队列。这表示 ReadyQueue 里大部分代理没有可用的 client，队列在大量空转。
		// limited≈1.5k：被 limiter 拒绝的次数，占比很小。
		// wait≈360k 次，平均等待 ≈2ms：说明大多数 PopReady 直接命中"未到时间"的代理，然后短暂等待/再试，调度成本很高。
		// fail≈114k：GetAvailableClient 最终返回失败的次数（每 5 秒），表示大量请求直接没拿到 client。
	//移除 IsHealthy 中的空闲超时检查：gRPC 连接本身有保活机制 (KeepAlive)，只要 conn.GetState() 是 Ready 或 Idle 即可使用。不要人为判定空闲即死亡。
	//取消了连接健康判断,保留限速/冷却逻辑。有健康判断的判断可以保证成功率接近 100%。可能整体成功数会更高一点。没有进行细致测试，为了方便使用，直接使用 去掉健康判断的版本
	//取消了连接健康判断,保留限速/冷却逻辑。有健康判断的判断可以保证成功率接近 100%。可能整体成功数会更高一点。没有进行细致测试，为了方便使用，直接使用 去掉健康判断的版本
	// 关闭 fire_and_forget 模式，改为同步模式，可以直接通过 bot那边的统计把两个发送渠道数据合并了
//  2025年11月25日 11:00 取消 IsAvailable 和 NextAvailableIn 的判断，直接返回 true，当前每个代理在被 PopReady 取出时，
// 立即把它的下一次可用时间设为 now + perProxyInterval（配置里是 1s），再放回 ReadyQueue。所以同一个代理同一地区至少间隔 1 秒才会再次被取用。
// 另外，如果发送失败触发 SetCooldown（Jito 的 Retry-After 等被动冷却），NextAvailableIn 会返回更长的时间，下一次回队列时会用这个更长的值，
// 等于在 1s 基础上延长冷却。主动限速的 limiter 检查已经移除，但 ReadyQueue 的 1 秒间隔仍在生效。
//  2025年11月25日 11:10 顶部统计改为每 5 秒输出一次，并且只输出有数据的 region，没有数据的 region 不输出
//  2025年11月25日 11:20 从固定重试次数改为超时控制 + 智能等待，避免固定重试次数导致的问题，快速失败，超时时间改为 200ms

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gagliardetto/solana-go"
	jsoniter "github.com/json-iterator/go" // ✅ FastJSON
	"github.com/mr-tron/base58"
	jito_pb "github.com/weeaa/jito-go/pb"
	"github.com/weeaa/jito-go/pkg"
	"golang.org/x/net/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v3"
)

// ✅ FastJSON: Drop-in replacement for encoding/json
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// ==================== 配置结构 ====================

type Config struct {
	ProxyMode              string         `yaml:"proxy_mode"`
	GRPCTransparentPort    int            `yaml:"grpc_transparent_port"`
	GRPCUseRegionPorts     bool           `yaml:"grpc_use_region_ports"`
	FireAndForget          bool           `yaml:"fire_and_forget"`
	MaxConcurrentBundles   int            `yaml:"max_concurrent_bundles"`
	ConnectionMaxIdle      string         `yaml:"connection_max_idle_time"`
	DebugMode              bool           `yaml:"debug_mode"`
	BindUnifiedPorts       bool           `yaml:"bind_unified_ports"`
	EnableRateLimitBackoff bool           `yaml:"enable_rate_limit_backoff"`
	MaxRetryAttempts       int            `yaml:"max_retry_attempts"`
	ProxyRateLimitInterval string         `yaml:"proxy_rate_limit_interval"`
	HTTPRetryEnabled       bool           `yaml:"http_retry_enabled"`
	HTTPMaxRetries         int            `yaml:"http_max_retries"`
	HTTPRequestTimeout     string         `yaml:"http_request_timeout"`
	HTTPReadTimeout        string         `yaml:"http_read_timeout"`
	HTTPWriteTimeout       string         `yaml:"http_write_timeout"`
	HTTPFireAndForget      bool           `yaml:"http_fire_and_forget"`
	SolanaRPCURL           string         `yaml:"solana_rpc_url"`
	SolanaRPCFallback      string         `yaml:"solana_rpc_fallback"`
	ProxyListFile          string         `yaml:"proxy_list_file"`
	HTTPPorts              map[int]string `yaml:"http_ports"`
	UnifiedPorts           map[int]string `yaml:"unified_ports"`
	Regions                []RegionConfig `yaml:"regions"`
	GRPCPortMapping        map[int]int    `yaml:"grpc_port_mapping"`
	TipAccounts            []string       `yaml:"tip_accounts"`
}

type RegionConfig struct {
	Name           string `yaml:"name"`
	BlockEngineURL string `yaml:"block_engine_url"`
}

var config Config
var connectionMaxIdleTime time.Duration
var proxyRateLimitInterval time.Duration
var httpRequestTimeout time.Duration
var httpReadTimeout time.Duration
var httpWriteTimeout time.Duration

// ==================== Object Pools ====================

var packetSlicePool = sync.Pool{
	New: func() interface{} {
		packets := make([]*jito_pb.Packet, 0, 5)
		return &packets
	},
}

// ==================== Rate Limiters & Ready Queue (Optimized) ====================

// ===== 限流器定义 =====
type Limiter interface {
	AllowNow() bool
	NextAvailableIn() time.Duration
	ApplyBackoff(d time.Duration)
}

var (
	rateLimitMode    = "hard"          // "hard" or "token_bucket"
	perProxyInterval time.Duration     // Hard interval (loaded from config)
	bucketCapacity   = 1               // Token bucket
	bucketRefill     = 1 * time.Second // Token bucket refill
)

// 硬间隔限流
type HardIntervalLimiter struct {
	mu           sync.Mutex
	lastAllow    time.Time
	backoffUntil time.Time
}

func (l *HardIntervalLimiter) AllowNow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if !l.backoffUntil.IsZero() && now.Before(l.backoffUntil) {
		return false
	}
	if l.lastAllow.IsZero() || now.Sub(l.lastAllow) >= perProxyInterval {
		l.lastAllow = now
		return true
	}
	return false
}

func (l *HardIntervalLimiter) NextAvailableIn() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if !l.backoffUntil.IsZero() && now.Before(l.backoffUntil) {
		return l.backoffUntil.Sub(now)
	}
	if l.lastAllow.IsZero() {
		return 0
	}
	elapsed := now.Sub(l.lastAllow)
	if elapsed >= perProxyInterval {
		return 0
	}
	return perProxyInterval - elapsed
}

func (l *HardIntervalLimiter) ApplyBackoff(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	until := time.Now().Add(d)
	if until.After(l.backoffUntil) {
		l.backoffUntil = until
	}
}

// 令牌桶限流
type TokenBucketLimiter struct {
	mu           sync.Mutex
	tokens       int
	lastRefill   time.Time
	backoffUntil time.Time
}

func (l *TokenBucketLimiter) refill() {
	now := time.Now()
	if l.lastRefill.IsZero() {
		l.tokens = bucketCapacity
		l.lastRefill = now
		return
	}
	elapsed := now.Sub(l.lastRefill)
	if elapsed <= 0 {
		return
	}
	n := int(elapsed / bucketRefill)
	if n > 0 {
		l.tokens += n
		if l.tokens > bucketCapacity {
			l.tokens = bucketCapacity
		}
		l.lastRefill = l.lastRefill.Add(time.Duration(n) * bucketRefill)
	}
}

func (l *TokenBucketLimiter) AllowNow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	if !l.backoffUntil.IsZero() && time.Now().Before(l.backoffUntil) {
		return false
	}
	if l.tokens > 0 {
		l.tokens--
		return true
	}
	return false
}

func (l *TokenBucketLimiter) NextAvailableIn() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	now := time.Now()
	var backoffRem time.Duration
	if !l.backoffUntil.IsZero() && now.Before(l.backoffUntil) {
		backoffRem = l.backoffUntil.Sub(now)
	}
	if l.tokens > 0 {
		return backoffRem
	}
	since := now.Sub(l.lastRefill)
	rem := bucketRefill - since
	if rem < 0 {
		rem = 0
	}
	if backoffRem > rem {
		return backoffRem
	}
	return rem
}

func (l *TokenBucketLimiter) ApplyBackoff(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	until := time.Now().Add(d)
	if until.After(l.backoffUntil) {
		l.backoffUntil = until
	}
}

// ===== Ready Queue (Priority Queue) =====
type readyItem struct {
	proxy string
	next  time.Time
	index int
}

type readyHeap []*readyItem

func (h readyHeap) Len() int           { return len(h) }
func (h readyHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }
func (h readyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *readyHeap) Push(x interface{}) {
	item := x.(*readyItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *readyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}
func (h readyHeap) Peek() *readyItem {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type ReadyQueue struct {
	mu    sync.Mutex
	h     readyHeap
	index map[string]*readyItem
}

func NewReadyQueue(proxies []string) *ReadyQueue {
	rq := &ReadyQueue{
		h:     make(readyHeap, 0, len(proxies)),
		index: make(map[string]*readyItem, len(proxies)),
	}
	now := time.Now()
	for _, p := range proxies {
		if p == "" {
			continue
		}
		item := &readyItem{proxy: p, next: now}
		rq.index[p] = item
		rq.h = append(rq.h, item)
	}
	heap.Init(&rq.h)
	return rq
}

func (rq *ReadyQueue) PopReady(now time.Time) (string, bool) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	top := rq.h.Peek()
	if top == nil || top.next.After(now) {
		return "", false
	}
	item := heap.Pop(&rq.h).(*readyItem)
	delete(rq.index, item.proxy)
	return item.proxy, true
}

func (rq *ReadyQueue) Update(proxy string, next time.Time) {
	if proxy == "" {
		return
	}
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if it, ok := rq.index[proxy]; ok && it != nil {
		it.next = next
		heap.Fix(&rq.h, it.index)
		return
	}
	item := &readyItem{proxy: proxy, next: next}
	heap.Push(&rq.h, item)
	rq.index[proxy] = item
}

func (rq *ReadyQueue) NextDelta(now time.Time) time.Duration {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	top := rq.h.Peek()
	if top == nil {
		return 0
	}
	if top.next.After(now) {
		return top.next.Sub(now)
	}
	return 0
}

// ==================== Per-Proxy-Per-Region State Tracker (LOCK-FREE) ====================

type ProxyRegionState struct {
	limiters sync.Map // map[string]Limiter
}

var globalProxyState *ProxyRegionState

func NewProxyRegionState() *ProxyRegionState {
	return &ProxyRegionState{}
}

func normalizeProxyIdentifier(proxyString string) string {
	return strings.TrimSpace(proxyString)
}

func (prs *ProxyRegionState) getLimiter(proxyString, regionName string) Limiter {
	proxyID := normalizeProxyIdentifier(proxyString)
	key := proxyID + ":" + regionName
	if val, ok := prs.limiters.Load(key); ok {
		return val.(Limiter)
	}
	var newLimiter Limiter
	if rateLimitMode == "token_bucket" {
		newLimiter = &TokenBucketLimiter{}
	} else {
		newLimiter = &HardIntervalLimiter{}
	}
	actual, _ := prs.limiters.LoadOrStore(key, newLimiter)
	return actual.(Limiter)
}

func (prs *ProxyRegionState) SetCooldown(proxyString, regionName string, startTime time.Time, delayMs int) {
	proxyID := normalizeProxyIdentifier(proxyString)

	// 计算实际需要等待的时间
	// delayMs 是服务器要求的Retry-After，或者我们根据错误类型设定的延迟
	// 这里的 SetCooldown 在原逻辑中是直接设置 "可用时间 = Now + delay"
	// 在新 Limiter 逻辑中，我们调用 ApplyBackoff

	limiter := prs.getLimiter(proxyID, regionName)
	limiter.ApplyBackoff(time.Duration(delayMs) * time.Millisecond)

	if config.DebugMode {
		log.Printf("❄️  [%s] Proxy %s backoff applied: %dms", regionName, proxyID, delayMs)
	}
}

func (prs *ProxyRegionState) IsAvailable(proxyString, regionName string) bool {
	proxyID := normalizeProxyIdentifier(proxyString)

	// Check Limiter
	limiter := prs.getLimiter(proxyID, regionName)
	return limiter.AllowNow()
}

func (prs *ProxyRegionState) NextAvailableIn(proxyString, regionName string) time.Duration {
	proxyID := normalizeProxyIdentifier(proxyString)
	limiter := prs.getLimiter(proxyID, regionName)
	return limiter.NextAvailableIn()
}

func extractBackoffMs(errMsg string) int {
	patterns := []string{
		`Retry after (\d+)ms`,
		`retry_after[\":]?\s*(\d+)`,
		`retryAfter[\":]?\s*(\d+)`,
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		matches := re.FindStringSubmatch(errMsg)
		if len(matches) >= 2 {
			ms, err := strconv.Atoi(matches[1])
			if err == nil {
				return ms
			}
		}
	}
	return 30000
}

// Helper function to check if error is Jito's global rate limit
func isJitoGlobalRateLimit(body string) bool {
	bodyLower := strings.ToLower(body)
	return strings.Contains(bodyLower, "globally rate limited") ||
		strings.Contains(bodyLower, "network congested")
}

func isRateLimitResponse(statusCode int, body string) (bool, int) {
	bodyLower := strings.ToLower(body)

	// ✅ CRITICAL: Jito's global/random rate limit is NOT proxy-specific
	// Don't set cooldown for "globally rate limited" or "network congested"
	if strings.Contains(bodyLower, "globally rate limited") ||
		strings.Contains(bodyLower, "network congested") {
		return false, 0 // Don't treat as rate limit for cooldown purposes
	}

	if statusCode == 429 {
		backoffMs := extractBackoffMs(body)
		return true, backoffMs
	}

	if strings.Contains(bodyLower, "rate limit") ||
		strings.Contains(bodyLower, "too many requests") ||
		strings.Contains(bodyLower, "retry after") {
		backoffMs := extractBackoffMs(body)
		return true, backoffMs
	}

	return false, 0
}

// ==================== 加载配置 ====================

func loadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}

	duration, err := time.ParseDuration(config.ConnectionMaxIdle)
	if err != nil {
		return fmt.Errorf("解析连接空闲时间失败: %w", err)
	}
	connectionMaxIdleTime = duration

	rateLimitInterval, err := time.ParseDuration(config.ProxyRateLimitInterval)
	if err != nil {
		return fmt.Errorf("解析代理速率限制间隔失败: %w", err)
	}
	proxyRateLimitInterval = rateLimitInterval
	perProxyInterval = rateLimitInterval // Set global rate limit interval

	// Parse HTTP timeouts
	if config.HTTPRequestTimeout != "" {
		timeout, err := time.ParseDuration(config.HTTPRequestTimeout)
		if err != nil {
			return fmt.Errorf("解析HTTP请求超时失败: %w", err)
		}
		httpRequestTimeout = timeout
	} else {
		httpRequestTimeout = 30 * time.Second
	}

	if config.HTTPReadTimeout != "" {
		timeout, err := time.ParseDuration(config.HTTPReadTimeout)
		if err != nil {
			return fmt.Errorf("解析HTTP读取超时失败: %w", err)
		}
		httpReadTimeout = timeout
	} else {
		httpReadTimeout = 60 * time.Second
	}

	if config.HTTPWriteTimeout != "" {
		timeout, err := time.ParseDuration(config.HTTPWriteTimeout)
		if err != nil {
			return fmt.Errorf("解析HTTP写入超时失败: %w", err)
		}
		httpWriteTimeout = timeout
	} else {
		httpWriteTimeout = 60 * time.Second
	}

	return nil
}

func formatGRPCTarget(raw string) string {
	if strings.Contains(raw, "://") {
		return raw
	}
	return "passthrough:///" + raw
}

func normalizeDialTarget(raw string) string {
	clean := strings.Map(func(r rune) rune {
		if r < 32 {
			return -1
		}
		return r
	}, raw)
	clean = strings.TrimSpace(clean)

	prefixes := []string{
		"passthrough:///",
		"dns:///",
		"tcp:///",
		"tcp://",
		"tcp/",
		"unix:///",
		"unix://",
		"unix/",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(clean, p) {
			clean = strings.TrimPrefix(clean, p)
			break
		}
	}

	clean = strings.TrimPrefix(clean, "//")
	clean = strings.Trim(clean, "/")
	return clean
}

func sanitizeDialAddress(addr string) string {
	addr = strings.Map(func(r rune) rune {
		if r < 32 {
			return -1
		}
		return r
	}, addr)
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return addr
	}

	prefixes := []string{
		"passthrough:///",
		"dns:///",
		"tcp:///",
		"tcp://",
		"tcp/",
		"unix:///",
		"unix://",
		"unix/",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(addr, p) {
			addr = strings.TrimPrefix(addr, p)
			break
		}
	}

	if strings.Contains(addr, "://") {
		if parsed, err := url.Parse(addr); err == nil {
			if parsed.Host != "" {
				if parsed.Port() == "" && parsed.Path != "" {
					return strings.TrimPrefix(parsed.Path, "/")
				}
				return parsed.Host
			}
		}
	}

	addr = strings.TrimPrefix(addr, "//")
	addr = strings.Trim(addr, "/")
	return addr
}

// ==================== ✅ OPTIMIZED: 简化的 gRPC 连接创建（移除独立监控） ====================

func CreateAndObserveGRPCConn(ctx context.Context, chErr chan error, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}

	// ✅ REMOVED: 独立的状态监控 goroutine
	// 依赖 gRPC 内置的连接管理和 IsHealthy() 被动检测
	// 这样可以避免每个连接一个 goroutine 的开销（5k 连接 = 5k goroutines）

	return conn, nil
}

// ==================== SOCKS5代理拨号器 ====================

func createSOCKS5Dialer(proxyStr string) (func(context.Context, string) (net.Conn, error), error) {
	if proxyStr == "" {
		return nil, nil
	}

	parts := strings.Split(proxyStr, ":")
	var proxyAddr string
	var auth *proxy.Auth

	switch len(parts) {
	case 2:
		proxyAddr = proxyStr
		auth = nil
	case 4:
		proxyAddr = net.JoinHostPort(parts[0], parts[1])
		if parts[2] != "" || parts[3] != "" {
			auth = &proxy.Auth{
				User:     parts[2],
				Password: parts[3],
			}
		}
	default:
		return nil, fmt.Errorf("invalid proxy format")
	}

	// ✅ CRITICAL FIX: Create base dialer with timeout
	baseDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	socksDialer, err := proxy.SOCKS5("tcp", proxyAddr, auth, baseDialer)
	if err != nil {
		return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
	}

	// ✅ CRITICAL FIX: Ensure we only pass clean "host:port" to SOCKS5
	// The SOCKS5 Dial signature is: Dial(network, address string)
	// network MUST be exactly "tcp", "tcp4", or "tcp6"
	return func(ctx context.Context, addr string) (net.Conn, error) {
		cleanAddr := sanitizeDialAddress(addr)

		// Use type assertion to get the underlying Dial method
		if contextDialer, ok := socksDialer.(interface {
			DialContext(ctx context.Context, network, address string) (net.Conn, error)
		}); ok {
			return contextDialer.DialContext(ctx, "tcp", cleanAddr)
		}

		// Fallback: use Dial without context
		return socksDialer.Dial("tcp", cleanAddr)
	}, nil
}

// ==================== Transport Pool Manager (CRITICAL FOR PRODUCTION) ====================

type TransportManager struct {
	transports map[string]*http.Transport
	mu         sync.RWMutex

	// ✅ NEW: Health monitoring like gRPC
	lastCheck  time.Time
	checkMutex sync.Mutex
}

func NewTransportManager() *TransportManager {
	tm := &TransportManager{
		transports: make(map[string]*http.Transport),
		lastCheck:  time.Now(),
	}

	// ✅ NEW: Background health check like gRPC connection observer
	go tm.healthCheckLoop()

	return tm
}

func (tm *TransportManager) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		tm.mu.Lock()
		for key, transport := range tm.transports {
			// Close truly idle connections to prevent stale connections
			transport.CloseIdleConnections()

			if config.DebugMode {
				log.Printf("🔍 [Transport] Cleaned idle connections for: %s", key)
			}
		}
		tm.mu.Unlock()
	}
}

func (tm *TransportManager) GetTransport(proxyStr, regionName string) *http.Transport {
	key := proxyStr + ":" + regionName

	// Fast path: read lock
	tm.mu.RLock()
	if transport, exists := tm.transports[key]; exists {
		tm.mu.RUnlock()
		return transport
	}
	tm.mu.RUnlock()

	// Slow path: write lock
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Double-check after acquiring write lock
	if transport, exists := tm.transports[key]; exists {
		return transport
	}

	// ✅ OPTIMIZED: Create transport with aggressive settings matching gRPC
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			// ✅ NEW: Session ticket caching like gRPC credentials
			ClientSessionCache: tls.NewLRUClientSessionCache(128),
		},
		DialContext: (&net.Dialer{
			Timeout:   httpRequestTimeout,
			KeepAlive: 30 * time.Second,
			// ✅ NEW: DualStack for IPv4/IPv6 like gRPC
			DualStack: true,
		}).DialContext,

		// ✅ Existing optimizations (keep these)
		MaxIdleConns:          10000,
		MaxIdleConnsPerHost:   10000,
		MaxConnsPerHost:       10000,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     false, // CRITICAL
		DisableCompression:    false,

		// ✅ NEW: Connection pooling optimizations
		ResponseHeaderTimeout: 30 * time.Second,

		// ✅ NEW: Force HTTP/2 when possible (like gRPC uses HTTP/2)
		ForceAttemptHTTP2: false,

		// ✅ NEW: Write buffer pool to reduce allocations
		WriteBufferSize: 32 * 1024,
		ReadBufferSize:  32 * 1024,
	}

	// Add SOCKS5 proxy if provided
	if proxyStr != "" {
		socksDialer, err := createSOCKS5Dialer(proxyStr)
		if err == nil && socksDialer != nil {
			transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return socksDialer(ctx, sanitizeDialAddress(addr))
			}
		} else if config.DebugMode && err != nil {
			log.Printf("⚠️ 创建SOCKS5拨号器失败: %v", err)
		}
	}

	tm.transports[key] = transport

	if config.DebugMode {
		log.Printf("✅ [Transport] Created new transport pool for: %s (HTTP/2: enabled)", key)
	}

	return transport
}

func (tm *TransportManager) Close() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for key, transport := range tm.transports {
		transport.CloseIdleConnections()
		delete(tm.transports, key)
	}

	log.Println("✅ Transport池已关闭")
}

var globalTransportManager *TransportManager

// ==================== Jito客户端 (gRPC) ====================

type JitoClient struct {
	conn            *grpc.ClientConn
	searcherService jito_pb.SearcherServiceClient
	ctx             context.Context
	cancel          context.CancelFunc
	errChan         chan error
	lastUsed        time.Time
	proxyString     string
	regionName      string
	mu              sync.Mutex
	closeOnce       sync.Once
}

func NewJitoClient(ctx context.Context, regionName, blockEngineURL, proxyString string) (*JitoClient, error) {
	clientCtx, cancel := context.WithCancel(ctx)

	// ✅ CRITICAL: Extract clean host:port for actual dialing
	dialTarget := normalizeDialTarget(blockEngineURL)

	var tlsServerName string
	if host, _, err := net.SplitHostPort(blockEngineURL); err == nil && host != "" {
		tlsServerName = host
	} else {
		tlsServerName = blockEngineURL
	}

	tlsConfig := &tls.Config{
		ServerName: tlsServerName,
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	}

	if proxyString != "" {
		// ✅ CRITICAL FIX: golang.org/x/net/proxy SOCKS5 has a bug where it concatenates
		// network and address in a way that breaks DNS resolution. We must use a direct
		// connection to the SOCKS5 proxy and perform the handshake ourselves.
		parts := strings.Split(proxyString, ":")
		var proxyAddr string
		var username, password string

		switch len(parts) {
		case 2:
			proxyAddr = proxyString
		case 4:
			proxyAddr = net.JoinHostPort(parts[0], parts[1])
			username = parts[2]
			password = parts[3]
		default:
			cancel()
			return nil, fmt.Errorf("invalid proxy format: %s", proxyString)
		}

		// Create a custom dialer that performs SOCKS5 handshake manually
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			// Step 1: Connect to SOCKS5 proxy
			d := &net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}
			conn, err := d.DialContext(ctx, "tcp", proxyAddr)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to SOCKS5 proxy: %w", err)
			}

			// Step 2: SOCKS5 handshake
			// Parse target host and port
			host, portStr, err := net.SplitHostPort(dialTarget)
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("invalid dial target: %w", err)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("invalid port: %w", err)
			}

			// Authentication method negotiation
			if username != "" || password != "" {
				// Method: Username/Password
				if _, err := conn.Write([]byte{5, 1, 2}); err != nil {
					conn.Close()
					return nil, err
				}
			} else {
				// Method: No authentication
				if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
					conn.Close()
					return nil, err
				}
			}

			// Read method selection response
			buf := make([]byte, 2)
			if _, err := io.ReadFull(conn, buf); err != nil {
				conn.Close()
				return nil, err
			}
			if buf[0] != 5 {
				conn.Close()
				return nil, fmt.Errorf("invalid SOCKS version: %d", buf[0])
			}

			// Handle authentication if required
			if buf[1] == 2 {
				// Username/Password authentication
				authReq := []byte{1, byte(len(username))}
				authReq = append(authReq, []byte(username)...)
				authReq = append(authReq, byte(len(password)))
				authReq = append(authReq, []byte(password)...)
				if _, err := conn.Write(authReq); err != nil {
					conn.Close()
					return nil, err
				}
				if _, err := io.ReadFull(conn, buf[:2]); err != nil {
					conn.Close()
					return nil, err
				}
				if buf[1] != 0 {
					conn.Close()
					return nil, fmt.Errorf("SOCKS5 authentication failed")
				}
			} else if buf[1] != 0 {
				conn.Close()
				return nil, fmt.Errorf("unsupported SOCKS5 auth method: %d", buf[1])
			}

			// Send CONNECT request
			req := []byte{5, 1, 0, 3, byte(len(host))}
			req = append(req, []byte(host)...)
			req = append(req, byte(port>>8), byte(port&0xff))
			if _, err := conn.Write(req); err != nil {
				conn.Close()
				return nil, err
			}

			// Read CONNECT response
			resp := make([]byte, 4)
			if _, err := io.ReadFull(conn, resp); err != nil {
				conn.Close()
				return nil, err
			}
			if resp[1] != 0 {
				conn.Close()
				return nil, fmt.Errorf("SOCKS5 connect failed: %d", resp[1])
			}

			// Read bound address (we don't need it, but must read it)
			switch resp[3] {
			case 1: // IPv4
				io.ReadFull(conn, make([]byte, 4+2))
			case 3: // Domain name
				var addrLen [1]byte
				io.ReadFull(conn, addrLen[:])
				io.ReadFull(conn, make([]byte, int(addrLen[0])+2))
			case 4: // IPv6
				io.ReadFull(conn, make([]byte, 16+2))
			}

			return conn, nil
		}))
	}

	// ✅ CRITICAL: Must use a dummy target that won't trigger DNS resolution
	// Since we're providing a custom dialer, gRPC won't actually use this target for dialing,
	// but it still needs a valid format to pass internal validation.
	// Using the IP address format prevents DNS lookups.
	dummyTarget := "passthrough:///" + dialTarget

	chErr := make(chan error, 1)
	conn, err := CreateAndObserveGRPCConn(clientCtx, chErr, dummyTarget, opts...)
	if err != nil {
		cancel()
		return nil, err
	}

	searcherService := jito_pb.NewSearcherServiceClient(conn)

	return &JitoClient{
		conn:            conn,
		searcherService: searcherService,
		ctx:             clientCtx,
		cancel:          cancel,
		errChan:         chErr,
		lastUsed:        time.Now(),
		proxyString:     proxyString,
		regionName:      regionName,
	}, nil
}

func (c *JitoClient) SendBundle(transactions []*solana.Transaction) (*jito_pb.SendBundleResponse, time.Time, error) {
	c.mu.Lock()
	c.lastUsed = time.Now()
	c.mu.Unlock()

	startTime := time.Now()

	bundle, err := c.assembleBundle(transactions)
	if err != nil {
		return nil, startTime, err
	}

	resp, err := c.searcherService.SendBundle(c.ctx, &jito_pb.SendBundleRequest{Bundle: bundle})

	if err != nil && config.EnableRateLimitBackoff {
		errMsg := err.Error()
		if strings.Contains(errMsg, "Rate limit exceeded") {
			backoffMs := extractBackoffMs(errMsg)
			globalProxyState.SetCooldown(c.proxyString, c.regionName, startTime, backoffMs)
		}
	}

	return resp, startTime, err
}

func (c *JitoClient) assembleBundle(transactions []*solana.Transaction) (*jito_pb.Bundle, error) {
	packetsPtr := packetSlicePool.Get().(*[]*jito_pb.Packet)
	packets := (*packetsPtr)[:0]
	defer packetSlicePool.Put(packetsPtr)

	for i, tx := range transactions {
		packet, err := pkg.ConvertTransactionToProtobufPacket(tx)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", i, err)
		}
		packets = append(packets, &packet)
	}

	result := make([]*jito_pb.Packet, len(packets))
	copy(result, packets)
	return &jito_pb.Bundle{Packets: result, Header: nil}, nil
}

func (c *JitoClient) IsHealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// ✅ FIX: 防止空指针解引用；空闲超时交由 gRPC 内部管理
	if c.conn == nil {
		return false
	}

	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

func (c *JitoClient) Close() error {
	var err error

	c.closeOnce.Do(func() {
		// 保护性检查，避免在关闭后再次使用
		if c.cancel != nil {
			c.cancel()
		}
		if c.errChan != nil {
			close(c.errChan)
			c.errChan = nil
		}

		c.mu.Lock()
		if c.conn != nil {
			err = c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
	})

	return err
}

// ==================== 连接池 (gRPC) ====================

type ConnectionPool struct {
	region        RegionConfig
	clients       []*JitoClient
	currentIdx    uint64
	mu            sync.RWMutex
	proxySelector *ProxySelector
	ctx           context.Context
	readyQueue    *ReadyQueue
	clientByProxy map[string]*JitoClient
	debugCounters *poolDebugCounters
}

// 调试计数器（仅用于诊断，避免改动业务逻辑）
type poolDebugCounters struct {
	missClient   uint64 // PopReady 命中无 client 或不健康
	limited      uint64 // IsAvailable 拒绝
	waitCount    uint64 // NextDelta 等待次数
	waitTotalNs  uint64 // NextDelta 等待总时长
	failGet      uint64 // GetAvailableClient 直接失败次数
}

type poolDebugSnapshot struct {
	missClient  uint64
	limited     uint64
	waitCount   uint64
	waitTotalNs uint64
	failGet     uint64
}

func (pdc *poolDebugCounters) snapshotAndReset() poolDebugSnapshot {
	if pdc == nil {
		return poolDebugSnapshot{}
	}
	return poolDebugSnapshot{
		missClient:  atomic.SwapUint64(&pdc.missClient, 0),
		limited:     atomic.SwapUint64(&pdc.limited, 0),
		waitCount:   atomic.SwapUint64(&pdc.waitCount, 0),
		waitTotalNs: atomic.SwapUint64(&pdc.waitTotalNs, 0),
		failGet:     atomic.SwapUint64(&pdc.failGet, 0),
	}
}

const (
	clientAcquireTimeout   = 5 * time.Second
	clientAcquireSleepStep = 50 * time.Millisecond
)

func NewConnectionPool(ctx context.Context, region RegionConfig, proxySelector *ProxySelector, poolSize int, proxies []string) *ConnectionPool {
	pool := &ConnectionPool{
		region:        region,
		clients:       make([]*JitoClient, 0, poolSize),
		proxySelector: proxySelector,
		ctx:           ctx,
		clientByProxy: make(map[string]*JitoClient),
		debugCounters: &poolDebugCounters{},
	}

	if len(proxies) > 0 {
		pool.readyQueue = NewReadyQueue(proxies)
	}

	successCount := 0
	failCount := 0

	for i := 0; i < poolSize && successCount < poolSize; i++ {
		var proxyString string

		// ✅ Use ProxySelector if available, otherwise we need logic to pick from pool?
		// Wait, ConnectionPool creates connections at startup.
		// But wait, ConnectionPool uses NewJitoClient which takes a proxy string.
		// We should use the proxySelector passed to NewConnectionPool.
		if proxySelector != nil {
			proxyString = proxySelector.GetNext()
		}

		client, err := NewJitoClient(ctx, region.Name, region.BlockEngineURL, proxyString)
		if err != nil {
			failCount++
			if config.DebugMode {
				log.Printf("⚠️  [%s] 创建客户端失败 (%d次): %v", region.Name, failCount, err)
			}

			if failCount > poolSize {
				log.Printf("❌ [%s] 连接池初始化失败次数过多，停止尝试", region.Name)
				break
			}
			continue
		}

		pool.clients = append(pool.clients, client)
		if client.proxyString != "" {
			pool.clientByProxy[client.proxyString] = client
		}
		successCount++
	}

	if len(pool.clients) == 0 {
		log.Printf("❌ [%s] 警告: 连接池没有健康连接!", region.Name)
	}

	// ✅ REMOVED: healthCheck goroutine 已移除，依赖被动检测

	return pool
}

func (p *ConnectionPool) GetAvailableClient() (*JitoClient, error) {
	clientsLen := len(p.clients) // 只读切片长度，无需锁
	if clientsLen == 0 {
		if p.debugCounters != nil {
			atomic.AddUint64(&p.debugCounters.failGet, 1)
		}
		return nil, fmt.Errorf("no clients in pool")
	}

	// 优先使用 ReadyQueue（每 region 独立的可用时间堆），避免大并发线性扫描
	if p.readyQueue != nil {
		now := time.Now()
		startTime := now
		
		// ✅ 方案A：平衡快速失败和充分等待
		// 延长超时以处理burst流量，增加采样提高命中率
		timeout := 500 * time.Millisecond  // 延长到500ms，处理突发流量
		maxHops := 200  // 增加到200，采样率6.7%（200/3000）
		consecutiveEmptyLoops := 0  // 连续空循环计数器

		for {
			// ✅ 超时检查：防止无限阻塞
			if time.Since(startTime) > timeout {
				if p.debugCounters != nil {
					atomic.AddUint64(&p.debugCounters.failGet, 1)
				}
				return nil, fmt.Errorf("timeout: no available clients after %v", timeout)
			}

			foundAny := false
			foundAvailable := false  // 是否找到通过AllowNow的代理
			
			for i := 0; i < maxHops; i++ {
				proxyStr, ok := p.readyQueue.PopReady(now)
				if !ok {
					break
				}
				
				foundAny = true

				client := p.clientByProxy[proxyStr]
				if client == nil {
					// 找不到对应的 client，丢回队列稍后再试
					if p.debugCounters != nil {
						atomic.AddUint64(&p.debugCounters.missClient, 1)
					}
					p.readyQueue.Update(proxyStr, now.Add(10*time.Second))
					continue
				}

			// ✅ 原子性检查并占用时间槽
			if globalProxyState != nil {
				limiter := globalProxyState.getLimiter(proxyStr, p.region.Name)
				if !limiter.AllowNow() {
					// 被拒绝，计算下次可用时间并放回
					nextAvailableTime := now.Add(limiter.NextAvailableIn())
					p.readyQueue.Update(proxyStr, nextAvailableTime)
					if p.debugCounters != nil {
						atomic.AddUint64(&p.debugCounters.limited, 1)
					}
					continue
				}
			// AllowNow() 返回 true，已占用时间槽
			foundAvailable = true
		}

		// ✅ 关键修复：设置下次可用时间为1秒后（与limiter保持一致）
		// 避免代理在50ms后被重复采样导致大量limited
		nextAvailable := now.Add(perProxyInterval)  // 使用配置的间隔（1秒）
		p.readyQueue.Update(proxyStr, nextAvailable)
		return client, nil
			}

			// ✅ 快速失败策略
			if !foundAny {
				consecutiveEmptyLoops++
				// 连续3次都没从队列取到代理，直接失败
				if consecutiveEmptyLoops >= 3 {
					if p.debugCounters != nil {
						atomic.AddUint64(&p.debugCounters.failGet, 1)
					}
					return nil, fmt.Errorf("queue empty after %d attempts", consecutiveEmptyLoops)
				}
				time.Sleep(5 * time.Millisecond)
				now = time.Now()
				continue
			}
			
			consecutiveEmptyLoops = 0  // 重置计数器
			
		// ✅ 改进的等待策略：平衡等待和超时
		if foundAny && !foundAvailable {
			delta := p.readyQueue.NextDelta(now)
			if delta > 0 {
				// ✅ 放宽等待限制：最多200ms，并考虑剩余超时时间
				remainingTime := timeout - time.Since(startTime)
				maxWait := 200 * time.Millisecond
				
				// 如果剩余时间不够，直接失败
				if delta > remainingTime {
					if p.debugCounters != nil {
						atomic.AddUint64(&p.debugCounters.failGet, 1)
					}
					return nil, fmt.Errorf("timeout: next available in %v, remaining %v", delta, remainingTime)
				}
				
				// 限制单次等待不超过200ms
				if delta > maxWait {
					delta = maxWait
				}
				
				if p.debugCounters != nil {
					atomic.AddUint64(&p.debugCounters.waitCount, 1)
					atomic.AddUint64(&p.debugCounters.waitTotalNs, uint64(delta.Nanoseconds()))
				}
				time.Sleep(delta)
				now = time.Now()
				continue
			}
		}
			
			// 短暂sleep后重试
			time.Sleep(1 * time.Millisecond)
			now = time.Now()
		}
	}

	// Fallback: 原常数窗口扫描
	maxScan := 32
	if clientsLen < maxScan {
		maxScan = clientsLen
	}
	maxRetries := 2

	for attempt := 0; attempt <= maxRetries; attempt++ {
		for i := 0; i < maxScan; i++ {
			idx := atomic.AddUint64(&p.currentIdx, 1) % uint64(clientsLen)
			client := p.clients[idx]
			if client == nil {
				continue
			}

			return client, nil
		}

		break
	}

	if p.debugCounters != nil {
		atomic.AddUint64(&p.debugCounters.failGet, 1)
	}
	return nil, fmt.Errorf("no available clients (all locked or in cooldown)")
}

// ✅ REMOVED: 健康检查已移除，依赖被动检测（IsHealthy()）
// 失败的连接在 GetAvailableClient() 中被自然跳过

func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, client := range p.clients {
		client.Close()
	}
	p.clients = nil
}

// ==================== 连接池管理器 (gRPC) ====================

type PoolManager struct {
	pools map[string]*ConnectionPool
	mu    sync.RWMutex
	ctx   context.Context
}

var globalPoolManager *PoolManager

func InitPoolManager(ctx context.Context, proxySelector *ProxySelector, proxies []string) {
	globalPoolManager = &PoolManager{
		pools: make(map[string]*ConnectionPool),
		ctx:   ctx,
	}

	// Initialize region stats (pre-allocate to avoid runtime allocation)
	for _, region := range config.Regions {
		regionStatsMap.Store(region.Name, &RegionStats{})
	}

	// 连接池大小等于代理数量
	poolSize := 1
	if proxySelector != nil && len(proxySelector.proxies) > 0 {
		poolSize = len(proxySelector.proxies)
	}

	for _, region := range config.Regions {
		pool := NewConnectionPool(ctx, region, proxySelector, poolSize, proxies)
		globalPoolManager.pools[region.Name] = pool
		log.Printf("✅ [%s] gRPC连接池已初始化 (%d个连接)", region.Name, poolSize)
	}
}

func (pm *PoolManager) GetPool(regionName string) (*ConnectionPool, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pool, exists := pm.pools[regionName]
	if !exists {
		return nil, fmt.Errorf("pool not found for region: %s", regionName)
	}

	return pool, nil
}

func (pm *PoolManager) Close() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for name, pool := range pm.pools {
		pool.Close()
		log.Printf("✅ [%s] gRPC连接池已关闭", name)
	}
}

// ==================== 代理选择器 ====================

type ProxySelector struct {
	proxies []string
	current uint64
}

func NewProxySelector(proxies []string) *ProxySelector {
	return &ProxySelector{
		proxies: proxies,
	}
}

func (ps *ProxySelector) GetNext() string {
	if len(ps.proxies) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&ps.current, 1) % uint64(len(ps.proxies))
	return ps.proxies[idx]
}

func (ps *ProxySelector) GetAvailableProxy(regionName string) string {
	if len(ps.proxies) == 0 {
		return ""
	}

	attempts := len(ps.proxies)
	for i := 0; i < attempts; i++ {
		proxyStr := ps.GetNext()
		if globalProxyState.IsAvailable(proxyStr, regionName) {
			return proxyStr
		}
	}

	return ps.GetNext()
}

func loadProxyServers(filename string) ([]string, error) {
	if filename == "" {
		return []string{}, nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// ✅ CRITICAL FIX: Detect and convert UTF-16 LE encoding
	// Check for UTF-16 LE BOM (0xFF 0xFE)
	if len(data) >= 2 && data[0] == 0xFF && data[1] == 0xFE {
		log.Println("⚠️  检测到 UTF-16 LE 编码的代理列表，正在转换为 UTF-8...")
		// Convert UTF-16 LE to UTF-8
		utf16Data := make([]uint16, 0, len(data)/2)
		for i := 2; i < len(data); i += 2 {
			if i+1 < len(data) {
				utf16Data = append(utf16Data, uint16(data[i])|uint16(data[i+1])<<8)
			}
		}
		// Convert UTF-16 to runes, then to string
		runes := make([]rune, 0, len(utf16Data))
		for _, u := range utf16Data {
			runes = append(runes, rune(u))
		}
		data = []byte(string(runes))
		log.Println("✅ UTF-16 LE 转换完成")
	}

	lines := strings.Split(string(data), "\n")
	proxies := make([]string, 0)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Remove any remaining null bytes
		line = strings.ReplaceAll(line, "\x00", "")
		if line != "" && !strings.HasPrefix(line, "#") {
			proxies = append(proxies, line)
		}
	}

	log.Printf("✅ 已加载 %d 个代理服务器", len(proxies))
	return proxies, nil
}

// ==================== 统计 ====================

// Per-region statistics (lock-free for high performance)
type RegionStats struct {
	totalRequests uint64 // atomic
	successCount  uint64 // atomic
	rttSumUs      uint64 // atomic, microseconds
	rttMinUs      uint64 // atomic, microseconds (0 means uninitialized)
	rttMaxUs      uint64 // atomic, microseconds
}

func (rs *RegionStats) RecordRequest(success bool, rttMs float64) {
	// Only 3-5 atomic operations, no locks, no allocations
	atomic.AddUint64(&rs.totalRequests, 1)
	if success {
		atomic.AddUint64(&rs.successCount, 1)
	}
	
	rttUs := uint64(rttMs * 1000) // Convert to microseconds
	atomic.AddUint64(&rs.rttSumUs, rttUs)
	
	// Update min (lock-free compare-and-swap)
	for {
		currentMin := atomic.LoadUint64(&rs.rttMinUs)
		if currentMin != 0 && rttUs >= currentMin {
			break
		}
		if atomic.CompareAndSwapUint64(&rs.rttMinUs, currentMin, rttUs) {
			break
		}
	}
	
	// Update max (lock-free compare-and-swap)
	for {
		currentMax := atomic.LoadUint64(&rs.rttMaxUs)
		if rttUs <= currentMax {
			break
		}
		if atomic.CompareAndSwapUint64(&rs.rttMaxUs, currentMax, rttUs) {
			break
		}
	}
}

func (rs *RegionStats) GetAndReset() (total, success uint64, avgMs, minMs, maxMs float64) {
	// Atomically swap to 0 and get old values
	total = atomic.SwapUint64(&rs.totalRequests, 0)
	success = atomic.SwapUint64(&rs.successCount, 0)
	sumUs := atomic.SwapUint64(&rs.rttSumUs, 0)
	minUs := atomic.SwapUint64(&rs.rttMinUs, 0)
	maxUs := atomic.SwapUint64(&rs.rttMaxUs, 0)
	
	if total > 0 {
		avgMs = float64(sumUs) / float64(total) / 1000.0
		minMs = float64(minUs) / 1000.0
		maxMs = float64(maxUs) / 1000.0
	}
	
	return
}

// Global region stats map (initialized once, no runtime allocation)
var regionStatsMap sync.Map // map[string]*RegionStats

type Stats struct {
	success     uint64
	failed      uint64
	retried     uint64
	httpSuccess uint64
	httpFailed  uint64
	httpRetried uint64
}

var globalStats Stats

func printStats() {
	// 统计频率 5s 输出一次
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// 额外输出各 region 成功建立的 gRPC client 数量
		if globalPoolManager != nil {
			globalPoolManager.mu.RLock()
			clientCounts := make([]string, 0, len(config.Regions))
			debugCounters := make([]string, 0, len(config.Regions))
			for _, region := range config.Regions {
				if pool, ok := globalPoolManager.pools[region.Name]; ok && pool != nil {
					clientCounts = append(clientCounts, fmt.Sprintf("%s=%d", region.Name, len(pool.clientByProxy)))
					snap := pool.debugCounters.snapshotAndReset()
					var waitAvgMs float64
					if snap.waitCount > 0 {
						waitAvgMs = float64(snap.waitTotalNs) / float64(snap.waitCount) / 1e6
					}
					debugCounters = append(debugCounters, fmt.Sprintf("%s miss=%d limited=%d wait=%d(avg%.2fms) fail=%d",
						region.Name, snap.missClient, snap.limited, snap.waitCount, waitAvgMs, snap.failGet))
				} else {
					clientCounts = append(clientCounts, fmt.Sprintf("%s=%d", region.Name, 0))
					debugCounters = append(debugCounters, fmt.Sprintf("%s miss=0 limited=0 wait=0(avg0ms) fail=0", region.Name))
				}
			}
			globalPoolManager.mu.RUnlock()
			log.Printf("📊 gRPC clients: %s", strings.Join(clientCounts, " | "))
			log.Printf("📊 gRPC debug: %s", strings.Join(debugCounters, " | "))
		}

		// Collect stats (this happens in background, not in hot path)
		type regionStat struct {
			name        string
			port        int
			total       uint64
			success     uint64
			avgMs       float64
			minMs       float64
			maxMs       float64
		}
		
		var stats []regionStat
		hasData := false
		
		// Iterate over regions in config order for consistent output
		for _, region := range config.Regions {
			if val, ok := regionStatsMap.Load(region.Name); ok {
				rs := val.(*RegionStats)
				total, success, avgMs, minMs, maxMs := rs.GetAndReset()
				
				if total > 0 {
					hasData = true
					stats = append(stats, regionStat{
						name:    region.Name,
						port:    getPortForRegion(region.Name),
						total:   total,
						success: success,
						avgMs:   avgMs,
						minMs:   minMs,
						maxMs:   maxMs,
					})
				}
			}
		}
		
		if !hasData {
			continue
		}
		
		// Print table
		log.Println("+---------------------------+--------------+--------------+--------------+------------+------------+")
		log.Println("|          Engine           |   avg (ms)   |   min (ms)   |   max (ms)   |  succ (%)  | count      |")
		log.Println("+---------------------------+--------------+--------------+--------------+------------+------------+")
		
		for _, stat := range stats {
			successRate := float64(stat.success) / float64(stat.total) * 100
			engineLabel := fmt.Sprintf("%s(%d)", stat.name, stat.port)
			
			log.Printf("| %-25s | %12.2f | %12.2f | %12.2f | %10.2f | %-10d |",
				engineLabel, stat.avgMs, stat.minMs, stat.maxMs, successRate, stat.total)
		}
		
		log.Println("+---------------------------+--------------+--------------+--------------+------------+------------+")
	}
}

// Helper function to get port for region name
func getPortForRegion(regionName string) int {
	// Map region names to ports based on config.GRPCPortMapping
	for port, idx := range config.GRPCPortMapping {
		if idx < len(config.Regions) && config.Regions[idx].Name == regionName {
			return port
		}
	}
	return config.GRPCTransparentPort // fallback
}

// ==================== Response Capture Writer ====================

type responseCapturingWriter struct {
	http.ResponseWriter
	statusCode  int
	body        *bytes.Buffer
	written     *bool
	headerWrote bool
	captureBody bool
}

func newResponseCapturingWriter(w http.ResponseWriter, written *bool, captureBody bool) *responseCapturingWriter {
	return &responseCapturingWriter{
		ResponseWriter: w,
		statusCode:     200,
		body:           &bytes.Buffer{},
		written:        written,
		headerWrote:    false,
		captureBody:    captureBody,
	}
}

func (rcw *responseCapturingWriter) WriteHeader(statusCode int) {
	if !rcw.headerWrote {
		rcw.statusCode = statusCode
		rcw.headerWrote = true
		*rcw.written = true
		rcw.ResponseWriter.WriteHeader(statusCode)
	}
}

func (rcw *responseCapturingWriter) Write(b []byte) (int, error) {
	if !rcw.headerWrote {
		rcw.WriteHeader(http.StatusOK)
	}
	*rcw.written = true

	// Only capture body if explicitly needed
	if rcw.captureBody {
		rcw.body.Write(b)
	}

	return rcw.ResponseWriter.Write(b)
}

// ==================== HTTP Proxy Handler (Fire-and-Forget) ====================

// Helper to get next proxy from ReadyQueue or fallback
func (h *HTTPProxyHandler) getNextProxy() string {
	if h.readyQueue == nil {
		// Fallback to simple selector if no queue
		if h.proxySelector != nil {
			return h.proxySelector.GetAvailableProxy(h.regionName)
		}
		return ""
	}

	now := time.Now()
	// Try to pop a ready proxy
	// We try up to N times to find one that passes the global check too
	maxHops := 5
	for i := 0; i < maxHops; i++ {
		if p, ok := h.readyQueue.PopReady(now); ok {
			// We got a candidate that thinks it's ready locally.
			// Let's check local state (Limiter)
			if h.proxyState.IsAvailable(p, h.regionName) {
				return p
			} else {
				// Not available (maybe locked)
				// Push back with a short delay
				h.readyQueue.Update(p, now.Add(100*time.Millisecond))
			}
		} else {
			break
		}
	}

	// If we couldn't find a ready one, return empty (caller handles retry/wait)
	return ""
}

// Helper to update proxy status after use
func (h *HTTPProxyHandler) updateProxyStatus(proxyStr string, nextAvailable time.Time) {
	if h.readyQueue != nil && proxyStr != "" {
		h.readyQueue.Update(proxyStr, nextAvailable)
	}
}

type HTTPProxyHandler struct {
	targetURL        string
	regionName       string
	proxySelector    *ProxySelector
	transportManager *TransportManager
	readyQueue       *ReadyQueue
	proxyState       *ProxyRegionState // ✅ Local independent state per port
}

func (h *HTTPProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// ✅ FIX 1: Read body BEFORE fire-and-forget
	if config.HTTPFireAndForget {
		// Read body synchronously while request is still valid
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			atomic.AddUint64(&globalStats.httpFailed, 1)
			return
		}
		r.Body.Close()

		// Return success immediately
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"accepted"}`))
		atomic.AddUint64(&globalStats.httpSuccess, 1)

		// ✅ Pass bodyBytes instead of r
		go h.processRequestAsyncWithBody(bodyBytes, r.Header, r.URL.Path, r.Method)
		return
	}

	if config.HTTPRetryEnabled {
		h.serveWithRetry(w, r)
	} else {
		h.serveSingleAttempt(w, r)
	}
}

func (h *HTTPProxyHandler) processRequestAsyncWithBody(bodyBytes []byte, headers http.Header, urlPath string, method string) {
	defer func() {
		if rec := recover(); rec != nil {
			atomic.AddUint64(&globalStats.httpFailed, 1)
			if config.DebugMode {
				log.Printf("❌ [HTTP:%s] Panic recovered: %v", h.regionName, rec)
			}
		}
	}()

	targetURL, _ := url.Parse(h.targetURL)
	maxAttempts := 1
	if config.HTTPRetryEnabled {
		maxAttempts = config.HTTPMaxRetries + 1
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		var proxyStr string
		if h.readyQueue != nil {
			proxyStr = h.getNextProxy()
		} else if h.proxySelector != nil && len(h.proxySelector.proxies) > 0 {
			proxyStr = h.proxySelector.GetAvailableProxy(h.regionName)
		}

		transport := h.transportManager.GetTransport(proxyStr, h.regionName)

		reqClone, _ := http.NewRequest(method, targetURL.String()+urlPath, bytes.NewReader(bodyBytes))
		for k, v := range headers {
			reqClone.Header[k] = v
		}

		// ✅ Capture startTime BEFORE request (gRPC style)
		startTime := time.Now()

		client := &http.Client{
			Transport: transport,
			Timeout:   httpRequestTimeout,
		}

		resp, reqErr := client.Do(reqClone)

		// ✅ Set cooldown AFTER request using formula (accounts for proxy latency)
		if reqErr != nil {
			if proxyStr != "" && !isJitoGlobalRateLimit(reqErr.Error()) {
				h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, 10000)
				// Update local queue
				if limiter := h.proxyState.getLimiter(proxyStr, h.regionName); limiter != nil {
					h.updateProxyStatus(proxyStr, time.Now().Add(limiter.NextAvailableIn()))
				}
			}
			// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
			if attempt < maxAttempts-1 {
				continue
			}
			return
		}

		// ✅ ALWAYS read body for 2xx responses (for UUID logging)
		shouldReadBody := (resp.StatusCode >= 200 && resp.StatusCode < 300) ||
			(config.EnableRateLimitBackoff && (resp.StatusCode == 429 || resp.StatusCode >= 500))

		var bodyStr string
		if shouldReadBody {
			respBody, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr == nil {
				bodyStr = string(respBody)

				// Check rate limiting first (if enabled)
				if config.EnableRateLimitBackoff && (resp.StatusCode == 429 || resp.StatusCode >= 500) {
					if isRateLimit, backoffMs := isRateLimitResponse(resp.StatusCode, bodyStr); isRateLimit {
						if proxyStr != "" {
							h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, backoffMs)
						}
						if config.DebugMode {
							log.Printf("⏱️  [HTTP:%s] Rate limit detected, cooldown %dms", h.regionName, backoffMs)
						}
						// ✅ OPTIMIZED: 移除 sleep，cooldown 已设置，立即切换代理
						if attempt < maxAttempts-1 {
							continue
						}
						return
					}
				}

				// ✅ SUCCESS: No additional cooldown, limiter handles rate limiting
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {

					var jitoResp map[string]interface{}
					if json.Unmarshal(respBody, &jitoResp) == nil {
						if uuid, ok := jitoResp["result"].(string); ok {
							if attempt > 0 {
								atomic.AddUint64(&globalStats.httpRetried, 1)
								if config.DebugMode {
									log.Printf("✅ [HTTP:%s] 重试成功 (尝试 %d) UUID: %s", h.regionName, attempt+1, uuid)
								}
							} else {
								if config.DebugMode {
									log.Printf("✅ [HTTP:%s] UUID: %s", h.regionName, uuid)
								}
							}
						} else {
							if config.DebugMode {
								log.Printf("✅ [HTTP:%s] Success (attempt %d)", h.regionName, attempt+1)
							}
						}
					}
					return
				}
			}
		} else {
			resp.Body.Close()
		}

		// Non-success, non-rate-limit case
		if proxyStr != "" && !isJitoGlobalRateLimit(bodyStr) {
			globalProxyState.SetCooldown(proxyStr, h.regionName, startTime, 1500)
		}

		// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
		if attempt < maxAttempts-1 {
			// 不再 sleep，立即切换
		}
	}

	atomic.AddUint64(&globalStats.httpFailed, 1)
}

func (h *HTTPProxyHandler) processRequestAsync(r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			atomic.AddUint64(&globalStats.httpFailed, 1)
			if config.DebugMode {
				log.Printf("❌ [HTTP:%s] Panic recovered: %v", h.regionName, rec)
			}
		}
	}()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		atomic.AddUint64(&globalStats.httpFailed, 1)
		return
	}
	r.Body.Close()

	targetURL, _ := url.Parse(h.targetURL)
	maxAttempts := 1
	if config.HTTPRetryEnabled {
		maxAttempts = config.HTTPMaxRetries + 1
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		var proxyStr string
		if h.readyQueue != nil {
			proxyStr = h.getNextProxy()
		} else if h.proxySelector != nil && len(h.proxySelector.proxies) > 0 {
			proxyStr = h.proxySelector.GetAvailableProxy(h.regionName)
		}

		transport := h.transportManager.GetTransport(proxyStr, h.regionName)

		reqClone, _ := http.NewRequest(r.Method, targetURL.String()+r.URL.Path, bytes.NewReader(bodyBytes))
		for k, v := range r.Header {
			reqClone.Header[k] = v
		}

		startTime := time.Now()
		client := &http.Client{
			Transport: transport,
			Timeout:   httpRequestTimeout,
		}

		resp, reqErr := client.Do(reqClone)

		if reqErr != nil {
			if proxyStr != "" && !isJitoGlobalRateLimit(reqErr.Error()) {
				h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, 10000)
				// Update local queue
				if limiter := h.proxyState.getLimiter(proxyStr, h.regionName); limiter != nil {
					h.updateProxyStatus(proxyStr, time.Now().Add(limiter.NextAvailableIn()))
				}
			}
			// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
			if attempt < maxAttempts-1 {
				continue
			}
			return
		}

		// ✅ ALWAYS read body for 2xx responses (for UUID logging)
		// Only skip reading for error responses when rate limiting is disabled
		shouldReadBody := (resp.StatusCode >= 200 && resp.StatusCode < 300) ||
			(config.EnableRateLimitBackoff && (resp.StatusCode == 429 || resp.StatusCode >= 500))

		var bodyStr string
		if shouldReadBody {
			respBody, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr == nil {
				bodyStr = string(respBody)

				// Check rate limiting first (if enabled)
				if config.EnableRateLimitBackoff && (resp.StatusCode == 429 || resp.StatusCode >= 500) {
					if isRateLimit, backoffMs := isRateLimitResponse(resp.StatusCode, bodyStr); isRateLimit {
						if proxyStr != "" {
							h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, backoffMs)
						}
						if config.DebugMode {
							log.Printf("⏱️  [HTTP:%s] Rate limit detected, cooldown %dms", h.regionName, backoffMs)
						}
						// ✅ OPTIMIZED: 移除 sleep，cooldown 已设置，立即切换代理
						if attempt < maxAttempts-1 {
							continue
						}
						return
					}
				}

				// ✅ SUCCESS: Extract and log UUID (only in debug mode)
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					var jitoResp map[string]interface{}
					if json.Unmarshal(respBody, &jitoResp) == nil {
						if uuid, ok := jitoResp["result"].(string); ok {
							if attempt > 0 {
								atomic.AddUint64(&globalStats.httpRetried, 1)
								if config.DebugMode {
									log.Printf("✅ [HTTP:%s] 重试成功 (尝试 %d) UUID: %s", h.regionName, attempt+1, uuid)
								}
							} else {
								if config.DebugMode {
									log.Printf("✅ [HTTP:%s] UUID: %s", h.regionName, uuid)
								}
							}
						} else {
							// UUID not found in expected format
							log.Printf("✅ [HTTP:%s] Success (attempt %d)", h.regionName, attempt+1)
						}
					}
					return
				}
			}
		} else {
			// ✅ OPTIMIZATION: Don't read body for 4xx/5xx errors when rate limiting disabled
			resp.Body.Close()
		}

		// ✅ FIXED: Non-success, non-rate-limit case - check for global rate limit
		if proxyStr != "" && !isJitoGlobalRateLimit(bodyStr) {
			h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, 1000)
			// Update local queue if applicable
			if limiter := h.proxyState.getLimiter(proxyStr, h.regionName); limiter != nil {
				h.updateProxyStatus(proxyStr, time.Now().Add(limiter.NextAvailableIn()))
			}
		}

		// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
		if attempt < maxAttempts-1 {
			// 不再 sleep，立即切换
		}
	}

	atomic.AddUint64(&globalStats.httpFailed, 1)
}

func (h *HTTPProxyHandler) serveSingleAttempt(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	var proxyStr string
	if h.readyQueue != nil {
		proxyStr = h.getNextProxy()
	} else if h.proxySelector != nil && len(h.proxySelector.proxies) > 0 {
		proxyStr = h.proxySelector.GetAvailableProxy(h.regionName)
	}

	targetURL, err := url.Parse(h.targetURL)
	if err != nil {
		http.Error(w, "Invalid target URL", http.StatusInternalServerError)
		atomic.AddUint64(&globalStats.httpFailed, 1)
		return
	}

	transport := h.transportManager.GetTransport(proxyStr, h.regionName)

	responseWritten := false
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = transport
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetURL.Scheme
		req.URL.Host = targetURL.Host
		req.Host = targetURL.Host
		if targetURL.Path != "" {
			req.URL.Path = singleJoiningSlash(targetURL.Path, req.URL.Path)
		}
	}

	var responseErr error

	// ✅ CRITICAL OPTIMIZATION: Only capture body if rate limit detection enabled
	if config.EnableRateLimitBackoff {
		rcw := newResponseCapturingWriter(w, &responseWritten, true) // Capture enabled

		proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, handlerErr error) {
			responseErr = handlerErr
			if config.DebugMode {
				log.Printf("❌ [HTTP:%s] Error: %v", h.regionName, handlerErr)
			}
		}

		proxy.ServeHTTP(rcw, r)

		if responseWritten {
			bodyStr := rcw.body.String()
			if isRateLimit, backoffMs := isRateLimitResponse(rcw.statusCode, bodyStr); isRateLimit {
				if proxyStr != "" {
					globalProxyState.SetCooldown(proxyStr, h.regionName, startTime, backoffMs)
				}
				atomic.AddUint64(&globalStats.httpFailed, 1)
				return
			}
		}
	} else {
		// ✅ FASTEST PATH: Direct proxy without any capture
		proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, handlerErr error) {
			responseErr = handlerErr
		}

		proxy.ServeHTTP(w, r)
		responseWritten = true
	}

	if responseErr != nil {
		atomic.AddUint64(&globalStats.httpFailed, 1)
		if !responseWritten {
			http.Error(w, fmt.Sprintf("Proxy error: %v", responseErr), http.StatusBadGateway)
		}
		return
	}

	atomic.AddUint64(&globalStats.httpSuccess, 1)
}

func (h *HTTPProxyHandler) serveWithRetry(w http.ResponseWriter, r *http.Request) {
	maxAttempts := config.HTTPMaxRetries + 1
	var lastErr error
	var lastStatusCode int

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		atomic.AddUint64(&globalStats.httpFailed, 1)
		return
	}
	defer r.Body.Close()

	targetURL, err := url.Parse(h.targetURL)
	if err != nil {
		http.Error(w, "Invalid target URL", http.StatusInternalServerError)
		atomic.AddUint64(&globalStats.httpFailed, 1)
		return
	}

	responseWritten := false
	success := false

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if responseWritten {
			break
		}

		var proxyStr string
		if h.readyQueue != nil {
			proxyStr = h.getNextProxy()
		} else if h.proxySelector != nil && len(h.proxySelector.proxies) > 0 {
			proxyStr = h.proxySelector.GetAvailableProxy(h.regionName)
		}

		transport := h.transportManager.GetTransport(proxyStr, h.regionName)

		reqClone := r.Clone(r.Context())
		reqClone.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		reqClone.ContentLength = int64(len(bodyBytes))

		startTime := time.Now()
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		proxy.Transport = transport
		proxy.Director = func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.Host = targetURL.Host
			if targetURL.Path != "" {
				req.URL.Path = singleJoiningSlash(targetURL.Path, req.URL.Path)
			}
		}

		// ✅ Only capture if rate limit detection enabled
		rcw := newResponseCapturingWriter(w, &responseWritten, config.EnableRateLimitBackoff)
		var responseErr error

		proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, handlerErr error) {
			responseErr = handlerErr
		}

		proxy.ServeHTTP(rcw, reqClone)

		lastStatusCode = rcw.statusCode

		if config.EnableRateLimitBackoff && responseWritten {
			bodyStr := rcw.body.String()
			if isRateLimit, backoffMs := isRateLimitResponse(rcw.statusCode, bodyStr); isRateLimit {
				if proxyStr != "" {
					h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, backoffMs)
				}
				responseWritten = false // Allow retry
				if attempt < maxAttempts-1 {
					time.Sleep(time.Duration(backoffMs) * time.Millisecond)
					continue
				}
				break
			}
		}

		if responseErr == nil && responseWritten && lastStatusCode >= 200 && lastStatusCode < 300 {
			success = true
			if attempt > 0 {
				atomic.AddUint64(&globalStats.httpRetried, 1)
			}
			break
		}

		if responseWritten {
			success = false
			break
		}

		lastErr = responseErr
		if proxyStr != "" && !isJitoGlobalRateLimit(rcw.body.String()) {
			cooldownMs := 1000
			if responseErr != nil {
				cooldownMs = 1000
			}
			h.proxyState.SetCooldown(proxyStr, h.regionName, startTime, cooldownMs)
		}

		// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
		if attempt < maxAttempts-1 {
			// 不再 sleep，立即切换
		}
	}

	if success {
		atomic.AddUint64(&globalStats.httpSuccess, 1)
	} else {
		atomic.AddUint64(&globalStats.httpFailed, 1)
		if !responseWritten {
			if lastErr != nil {
				http.Error(w, fmt.Sprintf("All retry attempts failed: %v", lastErr), http.StatusBadGateway)
			} else {
				http.Error(w, fmt.Sprintf("All retry attempts failed with status %d", lastStatusCode), lastStatusCode)
			}
		}
	}
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// ==================== gRPC Handler ====================

var bundleSemaphore chan struct{}

type SimpleGRPCHandler struct {
	proxySelector *ProxySelector
	regionIndex   int
	currentRegion uint64
	readyQueue    *ReadyQueue       // ✅ Added
	proxyState    *ProxyRegionState // ✅ Added
}

type JitoRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      interface{}   `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type JitoRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (h *SimpleGRPCHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var rpcReq JitoRPCRequest
	if err := json.Unmarshal(body, &rpcReq); err != nil {
		http.Error(w, "Invalid JSON-RPC request", http.StatusBadRequest)
		return
	}

	switch rpcReq.Method {
	case "sendBundle":
		h.handleSendBundle(w, &rpcReq)
	case "getTipAccounts":
		h.handleGetTipAccounts(w, &rpcReq)
	case "getRegions":
		h.handleGetRegions(w, &rpcReq)
	default:
		h.sendError(w, rpcReq.ID, -32601, "Method not found")
	}
}

type txDecodeResult struct {
	tx  *solana.Transaction
	idx int
	err error
}

func decodeSingleTransaction(txInterface interface{}) (*solana.Transaction, error) {
	txStr, ok := txInterface.(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction format")
	}

	var txBytes []byte
	var err error

	txBytes, err = base64.StdEncoding.DecodeString(txStr)
	if err != nil {
		txBytes, err = base58.Decode(txStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode transaction")
		}
	}

	tx, err := solana.TransactionFromBytes(txBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction")
	}

	return tx, nil
}

func (h *SimpleGRPCHandler) handleSendBundle(w http.ResponseWriter, rpcReq *JitoRPCRequest) {
	if len(rpcReq.Params) == 0 {
		atomic.AddUint64(&globalStats.failed, 1)
		h.sendError(w, rpcReq.ID, -32602, "Invalid params")
		return
	}

	txnsInterface, ok := rpcReq.Params[0].([]interface{})
	if !ok {
		atomic.AddUint64(&globalStats.failed, 1)
		h.sendError(w, rpcReq.ID, -32602, "Invalid transaction array")
		return
	}

	txns := make([]*solana.Transaction, len(txnsInterface))

	if len(txnsInterface) > 2 {
		var wg sync.WaitGroup
		resultChan := make(chan txDecodeResult, len(txnsInterface))

		for i, txInterface := range txnsInterface {
			wg.Add(1)
			go func(idx int, txIface interface{}) {
				defer wg.Done()
				tx, err := decodeSingleTransaction(txIface)
				resultChan <- txDecodeResult{tx: tx, idx: idx, err: err}
			}(i, txInterface)
		}

		wg.Wait()
		close(resultChan)

		for result := range resultChan {
			if result.err != nil {
				atomic.AddUint64(&globalStats.failed, 1)
				h.sendError(w, rpcReq.ID, -32602, fmt.Sprintf("Failed to decode tx %d: %v", result.idx, result.err))
				return
			}
			txns[result.idx] = result.tx
		}
	} else {
		for i, txInterface := range txnsInterface {
			tx, err := decodeSingleTransaction(txInterface)
			if err != nil {
				atomic.AddUint64(&globalStats.failed, 1)
				h.sendError(w, rpcReq.ID, -32602, fmt.Sprintf("Failed to decode tx %d: %v", i, err))
				return
			}
			txns[i] = tx
		}
	}

	var region RegionConfig
	if h.regionIndex >= 0 {
		region = config.Regions[h.regionIndex]
	} else {
		next := atomic.AddUint64(&h.currentRegion, 1) % uint64(len(config.Regions))
		region = config.Regions[next]
	}

	if config.FireAndForget {
		go h.sendBundleAsyncWithRetry(txns, region)

		resp := JitoRPCResponse{
			JSONRPC: "2.0",
			ID:      rpcReq.ID,
			Result:  "bundle_submitted",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		atomic.AddUint64(&globalStats.success, 1)
	} else {
		bundleID, err := h.sendBundleSyncWithRetry(txns, region)
		if err != nil {
			atomic.AddUint64(&globalStats.failed, 1)
			h.sendError(w, rpcReq.ID, -32000, fmt.Sprintf("Bundle failed: %v", err))
			return
		}

		resp := JitoRPCResponse{
			JSONRPC: "2.0",
			ID:      rpcReq.ID,
			Result:  bundleID,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		atomic.AddUint64(&globalStats.success, 1)
	}
}

// Helper to get next proxy from ReadyQueue or fallback (Duplicate logic for gRPC handler to avoid import cycle or complex refactor)
func (h *SimpleGRPCHandler) getNextProxy(regionName string) string {
	if h.readyQueue == nil {
		if h.proxySelector != nil {
			return h.proxySelector.GetAvailableProxy(regionName)
		}
		return ""
	}

	now := time.Now()
	maxHops := 5
	for i := 0; i < maxHops; i++ {
		if p, ok := h.readyQueue.PopReady(now); ok {
			// We got a candidate that thinks it's ready locally.
			// Let's check local state (Limiter)
			if h.proxyState.IsAvailable(p, regionName) {
				return p
			} else {
				// Not available (maybe locked)
				// Push back with a short delay
				h.readyQueue.Update(p, now.Add(100*time.Millisecond))
			}
		} else {
			break
		}
	}
	return ""
}

// Helper to update proxy status
func (h *SimpleGRPCHandler) updateProxyStatus(proxyStr string, nextAvailable time.Time) {
	if h.readyQueue != nil && proxyStr != "" {
		h.readyQueue.Update(proxyStr, nextAvailable)
	}
}

func (h *SimpleGRPCHandler) sendBundleAsyncWithRetry(txns []*solana.Transaction, region RegionConfig) {
	bundleSemaphore <- struct{}{}
	defer func() {
		<-bundleSemaphore
		if r := recover(); r != nil {
			atomic.AddUint64(&globalStats.failed, 1)
		}
	}()

	pool, err := globalPoolManager.GetPool(region.Name)
	if err != nil {
		atomic.AddUint64(&globalStats.failed, 1)
		return
	}

	maxAttempts := config.MaxRetryAttempts + 1
	
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// gRPC pool logic uses globalProxyState inside GetAvailableClient
		client, err := pool.GetAvailableClient()
		if err != nil {
			if attempt < maxAttempts-1 {
				// ✅ OPTIMIZED: 移除 sleep 和高频日志
				continue
			} else {
				// Don't record stats if we never got a client (not a real send failure)
				atomic.AddUint64(&globalStats.failed, 1)
				if config.DebugMode {
					log.Printf("❌ [%s] 所有重试失败: %v", region.Name, err)
				}
				return
			}
		}

		bundleResp, startTime, bundleErr := client.SendBundle(txns)
		
		// Calculate RTT for this attempt
		rtt := time.Since(startTime).Seconds() * 1000 // milliseconds

		if bundleErr == nil {
			// ✅ SUCCESS: Record stats immediately
			if val, ok := regionStatsMap.Load(region.Name); ok {
				val.(*RegionStats).RecordRequest(true, rtt)
			}

			atomic.AddUint64(&globalStats.success, 1)
			if attempt > 0 {
				atomic.AddUint64(&globalStats.retried, 1)
			}

			// Only log successful UUID in debug mode
			if config.DebugMode {
				if attempt > 0 {
					log.Printf("✅ [%s] 重试成功 (尝试 %d/%d) UUID: %s", region.Name, attempt+1, maxAttempts, bundleResp.GetUuid())
				} else {
					log.Printf("✅ [%s] UUID: %s", region.Name, bundleResp.GetUuid())
				}
			}
			return
		}

		// ✅ OPTIMIZED: 移除高频重试日志，只在最后失败时记录
		if config.DebugMode && attempt == maxAttempts-1 {
			log.Printf("❌ [%s] 所有 %d 次尝试均失败: %v", region.Name, maxAttempts, bundleErr)
		}

		// ✅ ERROR: Set cooldown (already fixed with isJitoGlobalRateLimit check)
		if bundleErr != nil && !strings.Contains(bundleErr.Error(), "Rate limit") && !isJitoGlobalRateLimit(bundleErr.Error()) {
			if h.proxyState != nil {
				h.proxyState.SetCooldown(client.proxyString, region.Name, startTime, 1000)
			} else {
				globalProxyState.SetCooldown(client.proxyString, region.Name, startTime, 1000)
			}
		}

		// Record failure on last attempt only
		if attempt == maxAttempts-1 {
			if val, ok := regionStatsMap.Load(region.Name); ok {
				val.(*RegionStats).RecordRequest(false, rtt)
			}
		}

		// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
		if attempt < maxAttempts-1 {
			// 不再 sleep，立即切换
		}
	}

	atomic.AddUint64(&globalStats.failed, 1)
	if config.DebugMode {
		log.Printf("❌ [%s] 所有 %d 次尝试均失败", region.Name, maxAttempts)
	}
}

func (h *SimpleGRPCHandler) sendBundleSyncWithRetry(txns []*solana.Transaction, region RegionConfig) (string, error) {
	pool, err := globalPoolManager.GetPool(region.Name)
	if err != nil {
		return "", err
	}

	maxAttempts := config.MaxRetryAttempts + 1
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		client, err := pool.GetAvailableClient()
		if err != nil {
			lastErr = err
			// ✅ OPTIMIZED: 移除 sleep，立即重试
			if attempt < maxAttempts-1 {
				continue
			}
			// Don't record stats if we never got a client (not a real send failure)
			break
		}

		bundleResp, startTime, bundleErr := client.SendBundle(txns)
		
		// Calculate RTT for this attempt
		rtt := time.Since(startTime).Seconds() * 1000 // milliseconds

		if bundleErr == nil {
			// ✅ SUCCESS: Record stats immediately
			if val, ok := regionStatsMap.Load(region.Name); ok {
				val.(*RegionStats).RecordRequest(true, rtt)
			}

			if attempt > 0 {
				atomic.AddUint64(&globalStats.retried, 1)
			}
			return bundleResp.GetUuid(), nil
		}

		// ✅ ERROR: Set cooldown
		if bundleErr != nil && !strings.Contains(bundleErr.Error(), "Rate limit") && !isJitoGlobalRateLimit(bundleErr.Error()) {
			if h.proxyState != nil {
				h.proxyState.SetCooldown(client.proxyString, region.Name, startTime, 1000)
			} else {
				globalProxyState.SetCooldown(client.proxyString, region.Name, startTime, 1000)
			}
		}
		lastErr = bundleErr

		// Record failure on last attempt only
		if attempt == maxAttempts-1 {
			if val, ok := regionStatsMap.Load(region.Name); ok {
				val.(*RegionStats).RecordRequest(false, rtt)
			}
		}

		// ✅ OPTIMIZED: 移除 sleep，立即重试下一个代理
		if attempt < maxAttempts-1 {
			// 不再 sleep，立即切换
		}
	}

	return "", lastErr
}

func (h *SimpleGRPCHandler) handleGetTipAccounts(w http.ResponseWriter, rpcReq *JitoRPCRequest) {
	resp := JitoRPCResponse{JSONRPC: "2.0", ID: rpcReq.ID, Result: config.TipAccounts}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *SimpleGRPCHandler) handleGetRegions(w http.ResponseWriter, rpcReq *JitoRPCRequest) {
	regions := make([]map[string]string, 0)
	for _, r := range config.Regions {
		regions = append(regions, map[string]string{"name": r.Name, "url": r.BlockEngineURL})
	}
	resp := JitoRPCResponse{JSONRPC: "2.0", ID: rpcReq.ID, Result: regions}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *SimpleGRPCHandler) sendError(w http.ResponseWriter, id interface{}, code int, message string) {
	resp := JitoRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &RPCError{Code: code, Message: message},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func extractRegionFromURL(targetURL string) string {
	if strings.Contains(targetURL, "ny.mainnet") {
		return "NewYork"
	} else if strings.Contains(targetURL, "amsterdam") {
		return "Amsterdam"
	} else if strings.Contains(targetURL, "frankfurt") {
		return "Frankfurt"
	} else if strings.Contains(targetURL, "tokyo") {
		return "Tokyo"
	} else if strings.Contains(targetURL, "slc.mainnet") {
		return "SaltLakeCity"
	} else if strings.Contains(targetURL, "london") {
		return "London"
	} else if strings.Contains(targetURL, "singapore") {
		return "Singapore"
	} else if strings.Contains(targetURL, "dublin") {
		return "Dublin"
	}
	return "Mainnet"
}

// ==================== System Optimization ====================

func optimizeSystemLimits() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
		oldLimit := rLimit.Cur
		rLimit.Cur = rLimit.Max
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
			log.Printf("✅ 文件描述符限制: %d -> %d", oldLimit, rLimit.Cur)
		}
	}

	log.Println("💡 建议运行以下命令优化TCP (需要root):")
	log.Println("   sudo sysctl -w net.ipv4.ip_local_port_range='1024 65535'")
	log.Println("   sudo sysctl -w net.ipv4.tcp_tw_reuse=1")
	log.Println("   sudo sysctl -w net.core.somaxconn=4096")
}

// ==================== 主函数 ====================

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	configFile := "config.yaml"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	if err := loadConfig(configFile); err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	log.Printf("🚀 启动Jito代理 (OPTIMIZED: FastJSON) - 模式: %s", config.ProxyMode)
	runtime.GOMAXPROCS(runtime.NumCPU())
	optimizeSystemLimits()

	globalProxyState = NewProxyRegionState()
	globalTransportManager = NewTransportManager()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxyList, _ := loadProxyServers(config.ProxyListFile)
	var proxySelector *ProxySelector
	if len(proxyList) > 0 {
		log.Printf("✅ 已加载 %d 个SOCKS5代理", len(proxyList))
		proxySelector = NewProxySelector(proxyList)
	}

	bundleSemaphore = make(chan struct{}, config.MaxConcurrentBundles)

	if config.ProxyMode == "grpc" || config.ProxyMode == "both" {
		log.Printf("🔄 初始化gRPC连接池...")
		InitPoolManager(ctx, proxySelector, proxyList)
	}

	go printStats()

	servers := make([]*http.Server, 0)

	// HTTP REVERSE PROXY MODE
	if config.ProxyMode == "http" || config.ProxyMode == "both" {
		log.Println("🌐 启动HTTP反向代理 (Transport池已启用)...")
		for port, target := range config.HTTPPorts {
			regionName := extractRegionFromURL(target)

			// ✅ Create Local Ready Queue & State for this handler
			var rq *ReadyQueue
			if len(proxyList) > 0 {
				rq = NewReadyQueue(proxyList)
			}

		handler := &HTTPProxyHandler{
			targetURL:        target,
			regionName:       regionName,
			proxySelector:    proxySelector,
			transportManager: globalTransportManager,
			readyQueue:       rq,
			proxyState:       globalProxyState, // ✅ 统一使用全局状态，支持跨区域共享代理
		}
			server := &http.Server{
				Addr:         fmt.Sprintf(":%d", port),
				Handler:      handler,
				ReadTimeout:  httpReadTimeout,
				WriteTimeout: httpWriteTimeout,
				IdleTimeout:  90 * time.Second,
			}
			servers = append(servers, server)
			go func(p int, t string, r string) {
				mode := "同步"
				if config.HTTPFireAndForget {
					mode = "Fire-and-Forget"
				}
				log.Printf("✅ HTTP端口 %d -> %s [%s] 模式=%s", p, t, r, mode)
				server.ListenAndServe()
			}(port, target, regionName)
		}
	}

	// gRPC TRANSPARENT PROXY MODE
	if config.ProxyMode == "grpc" || config.ProxyMode == "both" {
		log.Println("🚀 启动gRPC透明代理...")

		// ✅ Create Local Ready Queue & State for autoHandler
		var autoRq *ReadyQueue
		if len(proxyList) > 0 {
			autoRq = NewReadyQueue(proxyList)
		}

	autoHandler := &SimpleGRPCHandler{
		proxySelector: proxySelector,
		regionIndex:   -1,
		readyQueue:    autoRq,
		proxyState:    globalProxyState, // ✅ 统一使用全局状态
	}
		autoServer := &http.Server{
			Addr:    fmt.Sprintf(":%d", config.GRPCTransparentPort),
			Handler: autoHandler,
		}
		servers = append(servers, autoServer)
		go func() {
			log.Printf("✅ gRPC端口 %d (自动轮询)", config.GRPCTransparentPort)
			autoServer.ListenAndServe()
		}()

		if config.GRPCUseRegionPorts {
			for port, regionIdx := range config.GRPCPortMapping {
				// ✅ Create Local Ready Queue & State for each region handler
				var regionRq *ReadyQueue
				if len(proxyList) > 0 {
					regionRq = NewReadyQueue(proxyList)
				}

			handler := &SimpleGRPCHandler{
				proxySelector: proxySelector,
				regionIndex:   regionIdx,
				readyQueue:    regionRq,
				proxyState:    globalProxyState, // ✅ 统一使用全局状态
			}
				server := &http.Server{
					Addr:    fmt.Sprintf(":%d", port),
					Handler: handler,
				}
				servers = append(servers, server)
				go func(p int, region string) {
					log.Printf("✅ gRPC端口 %d -> %s", p, region)
					server.ListenAndServe()
				}(port, config.Regions[regionIdx].Name)
			}
		}
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Printf("📋 ✅ FastJSON (jsoniter) - 3-4ms faster per request")
	log.Printf("📋 HTTP: Transport池 | 连接复用 | Active/Passive冷却")
	log.Printf("📋 gRPC: 连接观察器 | 自动重连 | 防止goroutine泄漏")
	log.Printf("📋 Fire-and-Forget: %v | 重试: %v (%d次)",
		config.HTTPFireAndForget, config.HTTPRetryEnabled, config.HTTPMaxRetries)
	log.Printf("📋 超时: 请求=%v | 读=%v | 写=%v",
		httpRequestTimeout, httpReadTimeout, httpWriteTimeout)
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("✅ 服务已启动")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 关闭服务...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	for _, server := range servers {
		server.Shutdown(shutdownCtx)
	}

	if globalPoolManager != nil {
		globalPoolManager.Close()
	}

	if globalTransportManager != nil {
		globalTransportManager.Close()
	}

	log.Println("✅ 服务已关闭")
}
