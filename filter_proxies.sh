#!/bin/bash
# 后台运行并打印日志
# nohup ./filter_proxies.sh > /tmp/filter.log 2>&1 &
#    tail -f /tmp/filter.log
#  2025年11月26日 10:00 过滤可用代理
echo "🚀 开始并行测试所有代理（并发数：100）..."
input_file="./proxy-list.txt"
output_good="./proxies_good.txt"
output_bad="./proxies_bad.txt"
temp_good="/tmp/proxies_good_$$.txt"
temp_bad="/tmp/proxies_bad_$$.txt"
progress_file="/tmp/proxy_progress_$$.txt"

# 清空输出文件
> "$output_good"
> "$output_bad"
> "$temp_good"
> "$temp_bad"
echo "0" > "$progress_file"

# 并发数
MAX_PARALLEL=100

# 总代理数
total=$(grep -c . "$input_file")
echo "📊 总代理数: $total"
echo ""

# 测试单个代理的函数
test_proxy() {
    local proxy="$1"
    local temp_good="$2"
    local temp_bad="$3"
    local progress_file="$4"
    
    if [ -z "$proxy" ]; then return; fi
    
    IFS=':' read -r host port user pass <<< "$proxy"
    
    # 测试连接（3秒超时，加快速度）
    if timeout 3 curl -x socks5h://${user}:${pass}@${host}:${port} \
        -s -o /dev/null -w "%{http_code}" \
        https://mainnet.block-engine.jito.wtf 2>/dev/null | grep -q "404\|200\|503"; then
        echo "$proxy" >> "$temp_good"
    else
        echo "$proxy" >> "$temp_bad"
    fi
    
    # 更新进度（原子操作）
    local current=$(( $(cat "$progress_file") + 1 ))
    echo "$current" > "$progress_file"
}

export -f test_proxy

# 启动进度监控
(
    while [ -f "$progress_file" ]; do
        current=$(cat "$progress_file" 2>/dev/null || echo "0")
        good=$(wc -l < "$temp_good" 2>/dev/null || echo "0")
        bad=$(wc -l < "$temp_bad" 2>/dev/null || echo "0")
        percent=$((current * 100 / total))
        
        printf "\r⏳ 进度: %d/%d (%d%%) | ✅ 好: %d | ❌ 坏: %d" \
            "$current" "$total" "$percent" "$good" "$bad"
        
        sleep 2
    done
) &
monitor_pid=$!

# 并行处理
cat "$input_file" | xargs -P $MAX_PARALLEL -I {} bash -c "test_proxy '{}' '$temp_good' '$temp_bad' '$progress_file'"

# 停止进度监控
sudo rm -f "$progress_file"
wait "$monitor_pid" 2>/dev/null

# 合并结果
cat "$temp_good" > "$output_good"
cat "$temp_bad" > "$output_bad"

# 清理临时文件
sudo rm -f "$temp_good" "$temp_bad"

# 最终统计
good=$(wc -l < "$output_good")
bad=$(wc -l < "$output_bad")

echo ""
echo ""
echo "========== ✅ 完成 =========="
echo "✅ 可用代理: $good (保存到 $output_good)"
echo "❌ 不可用代理: $bad (保存到 $output_bad)"
echo "📊 可用率: $(awk "BEGIN {printf \"%.2f%%\", $good*100/$total}")"
echo ""
echo "🔄 使用可用代理："
echo "  cp $output_good ./proxy-list.txt"
echo "  pm2 restart gproxy"
echo ""

