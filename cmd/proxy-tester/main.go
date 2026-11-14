// 测试socks5代理服务器可用： go run proxy_tester.go
package main

import (
	"bufio"
	"context"
	"fmt"
	"golang.org/x/net/proxy"
	"os"
	"strings"
	"sync"
	"time"
)

type ProxyResult struct {
	Address string
	Working bool
	Latency time.Duration
	Error   string
}

func testProxy(address string) ProxyResult {
	result := ProxyResult{Address: address}
	
	// 设置超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建SOCKS5代理拨号器
	dialer, err := proxy.SOCKS5("tcp", address, nil, proxy.Direct)
	if err != nil {
		result.Error = fmt.Sprintf("创建代理拨号器失败: %v", err)
		return result
	}

	// 记录开始时间
	start := time.Now()

	// 尝试通过代理连接到目标服务器
	conn, err := dialer.(proxy.ContextDialer).DialContext(ctx, "tcp", "www.google.com:80")
	if err != nil {
		result.Error = fmt.Sprintf("连接失败: %v", err)
		return result
	}
	defer conn.Close()

	result.Working = true
	result.Latency = time.Since(start)
	return result
}

func main() {
	// 读取代理列表文件
	file, err := os.Open("proxy-list.txt")
	if err != nil {
		fmt.Printf("打开文件失败: %v\n", err)
		return
	}
	defer file.Close()

	// 创建输出文件
	outWorking, err := os.Create("working_proxies.txt")
	if err != nil {
		fmt.Printf("创建可用代理输出文件失败: %v\n", err)
		return
	}
	defer outWorking.Close()
	workingWriter := bufio.NewWriter(outWorking)
	defer workingWriter.Flush()

	outFailed, err := os.Create("failed_proxies.txt")
	if err != nil {
		fmt.Printf("创建不可用代理输出文件失败: %v\n", err)
		return
	}
	defer outFailed.Close()
	failedWriter := bufio.NewWriter(outFailed)
	defer failedWriter.Flush()

	var proxies []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if addr := strings.TrimSpace(scanner.Text()); addr != "" {
			proxies = append(proxies, addr)
		}
	}

	// 创建工作池
	workers := 50
	results := make(chan ProxyResult, len(proxies))
	var wg sync.WaitGroup
	
	// 创建任务通道
	tasks := make(chan string, len(proxies))
	
	// 填充任务
	for _, addr := range proxies {
		tasks <- addr
	}
	close(tasks)
	
	// 启动工作协程
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range tasks {
				results <- testProxy(addr)
			}
		}()
	}

	// 等待所有测试完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 统计结果
	var working, failed int
	fmt.Println("代理测试结果:")
	fmt.Println("==========================================")
	
	for result := range results {
		if result.Working {
			working++
			fmt.Printf("✅ %s - 延迟: %v\n", result.Address, result.Latency)
			// 仅写入地址，按行分隔
			fmt.Fprintln(workingWriter, result.Address)
		} else {
			failed++
			fmt.Printf("❌ %s - %s\n", result.Address, result.Error)
			// 仅写入地址，按行分隔
			fmt.Fprintln(failedWriter, result.Address)
		}
	}

	fmt.Println("==========================================")
	fmt.Printf("测试完成: 总计 %d 个代理\n", len(proxies))
	fmt.Printf("可用: %d 个\n", working)
	fmt.Printf("不可用: %d 个\n", failed)
}