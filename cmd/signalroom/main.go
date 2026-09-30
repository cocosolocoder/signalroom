package main

import (
	"fmt"
	"os"
	"time"

	"github.com/cocosolocoder/signalroom/internal/events"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "demo" {
		fmt.Fprintln(os.Stderr, "usage: signalroom demo")
		os.Exit(2)
	}
	if err := runDemo(); err != nil {
		fmt.Fprintf(os.Stderr, "signalroom: %v\n", err)
		os.Exit(1)
	}
}

func runDemo() error {
	store := events.NewStore()
	base := time.Date(2026, time.October, 1, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	fixtures := []events.Event{
		{ID: "evt-001", Service: "gateway", Severity: "critical", Message: "错误率超过阈值", At: base},
		{ID: "evt-002", Service: "checkout", Severity: "warning", Message: "请求延迟持续升高", At: base.Add(90 * time.Second)},
		{ID: "evt-003", Service: "gateway", Severity: "info", Message: "值班工程师已确认告警", At: base.Add(3 * time.Minute)},
	}
	for _, event := range fixtures {
		if err := store.Add(event); err != nil {
			return err
		}
	}

	fmt.Println("Signalroom 服务可观测与事件响应工作台")
	fmt.Println("当前事件时间线：")
	for _, event := range store.Query(events.Query{}) {
		fmt.Printf("%s  %-8s %-8s %s\n", event.At.Format("15:04:05"), event.Service, event.Severity, event.Message)
	}
	fmt.Printf("已接入事件：%d，严重事件：%d\n", len(fixtures), len(store.Query(events.Query{Severity: "critical"})))
	return nil
}
