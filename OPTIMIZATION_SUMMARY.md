# gRPC Seed Proxy 性能优化总结

## 修改日期
2025-11-25

## 问题描述
- **症状**：4200个代理，每个区域每秒3000个请求（总24000 req/s），但实际每5秒只处理200-1400个请求
- **日志显示**：wait次数高达20-26万次，平均等待13-28ms
- **理论容量**：4200代理 × 1次/秒 × 8区域 = 33,600 req/s
- **实际需求**：3000 req/s × 8区域 = 24,000 req/s（远低于理论容量）

## 根本原因
1. **状态管理混乱**：每个 handler 创建独立的 ProxyRegionState，但 ConnectionPool 读取的是 globalProxyState，导致状态不同步
2. **提前锁定问题**：代理在返回给调用者前就被锁定1秒，导致大量并发请求进入等待循环
3. **并发竞态条件**：使用 NextAvailableIn()（只读）而不是 AllowNow()（原子更新），可能导致1秒内重复使用同一代理

## 解决方案

### 方案1：统一状态管理
**目标**：所有地方使用同一个 globalProxyState，保证状态同步

**修改位置**：
1. `main.go:2726` - HTTP handler 创建
2. `main.go:2761` - gRPC autoHandler 创建  
3. `main.go:2785` - gRPC region handler 创建

**修改内容**：
```go
// 原代码
proxyState: NewProxyRegionState(), // ❌ 每个handler独立状态

// 新代码
proxyState: globalProxyState, // ✅ 统一使用全局状态
```

**效果**：
- ✅ 支持跨区域代理共享（同一代理可同时被8个区域使用）
- ✅ 基于区域的独立限速（key = "proxy:region"）
- ✅ 状态读写一致，无数据不同步问题

### 方案2：正确的并发保护 + 缩短预占时间
**目标**：防止1秒内重复使用同一代理，同时提高吞吐量

**修改位置**：`main.go:1226-1254` - GetAvailableClient() 函数

**关键修改**：
```go
// 原代码（有并发竞态风险）
nextAvailable := now.Add(perProxyInterval) // 立即锁定1秒
if globalProxyState != nil {
    if extra := globalProxyState.NextAvailableIn(proxyStr, p.region.Name); extra > perProxyInterval {
        nextAvailable = now.Add(extra)
    }
}
p.readyQueue.Update(proxyStr, nextAvailable)
return client, nil

// 新代码（并发安全）
if globalProxyState != nil {
    if !globalProxyState.IsAvailable(proxyStr, p.region.Name) {
        // 还在冷却中，放回队列
        nextAvailableTime := now.Add(globalProxyState.NextAvailableIn(proxyStr, p.region.Name))
        p.readyQueue.Update(proxyStr, nextAvailableTime)
        if p.debugCounters != nil {
            atomic.AddUint64(&p.debugCounters.limited, 1)
        }
        continue
    }
    // IsAvailable() 返回 true，已原子性地占用时间槽
}

// 只预占50ms，防止瞬间重复Pop
nextAvailable := now.Add(50 * time.Millisecond)
p.readyQueue.Update(proxyStr, nextAvailable)
return client, nil
```

**并发保护机制**：
1. `IsAvailable()` 内部调用 `AllowNow()`，会原子性地更新 `lastAllow = now`
2. 其他并发线程调用 `IsAvailable()` 时会检测到 `now.Sub(lastAllow) < 1秒`，返回 false
3. ReadyQueue 只预占 50ms（防止瞬间重复Pop），真正的1秒限速由 Limiter 保证

## 预期效果

### 修改前
- 处理量：200-1400 req/5s（每区域）
- wait次数：228,840次（NewYork）
- 平均等待：13.30ms
- 成功率：81-98%
- 总CPU浪费：50+ 分钟的等待时间

### 修改后（预期）
- 处理量：15,000 req/5s（每区域，接近理论上限）
- wait次数：< 1,000次
- 平均等待：< 1ms
- 成功率：95-99%
- 吞吐量提升：**10-75倍**

## 业务保证

### ✅ 严格的1秒限速
- 每个代理在每个区域严格1秒发送1次
- 通过 Limiter 的 `AllowNow()` 原子操作保证
- 不会触发 Jito 的惩罚机制

### ✅ 跨区域代理共享
- 同一代理可以同时被多个区域使用
- `proxy1:NewYork` 和 `proxy1:Amsterdam` 是独立的限速器
- 8个区域可以充分利用全部4200个代理

### ✅ 主动冷却（1秒）
- 配置：`proxy_rate_limit_interval: "1s"`
- 实现：`HardIntervalLimiter.AllowNow()` 检查 `lastAllow` 间隔

### ✅ 被动冷却（Retry-After）
- Jito 返回的 Retry-After 通过 `SetCooldown()` 设置
- `ApplyBackoff()` 会设置 `backoffUntil` 时间
- 优先级高于主动冷却

## 测试建议

1. **验证1秒限速**：监控同一代理在同一区域的调用间隔
2. **验证吞吐量**：观察5秒内处理的请求数是否接近15,000
3. **验证wait次数**：debug计数器的wait应该降到1000以下
4. **验证成功率**：保持95%+的成功率

## 监控指标

观察以下日志变化：
```
📊 gRPC debug: NewYork miss=? limited=? wait=?(avg?ms) fail=?
```

预期变化：
- `wait`: 228,840 → < 1,000
- `avg`: 13.30ms → < 1ms
- `fail`: 828 → < 50
- `count`: 448 → 15,000+

