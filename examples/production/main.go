package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	wecomaibot "github.com/Area1m/wecom-aibot-go"
)

func main() {
	botID := os.Getenv("WECOM_BOT_ID")
	secret := os.Getenv("WECOM_BOT_SECRET")
	if botID == "" || secret == "" {
		log.Fatal("请先设置环境变量 WECOM_BOT_ID 和 WECOM_BOT_SECRET")
	}

	bot, err := wecomaibot.NewClient(wecomaibot.Config{
		BotID:                botID,
		Secret:               secret,
		MaxReconnectAttempts: -1,
		ReconnectIntervalMS:  1000,
		HeartbeatIntervalMS:  30000,
	})
	if err != nil {
		log.Fatalf("创建客户端失败: %v", err)
	}

	registerLifecycleLogs(bot)
	registerBusinessHandlers(bot)
	startHealthServer(bot)

	if err := bot.Connect(); err != nil {
		log.Fatalf("连接失败: %v", err)
	}

	log.Println("机器人已启动，等待消息")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("收到退出信号，开始优雅停机")
	bot.Disconnect()
	log.Println("机器人已退出")
}

func registerLifecycleLogs(bot *wecomaibot.Client) {
	bot.OnConnected(func(ctx context.Context) {
		log.Println("WebSocket 已连接")
	})

	bot.OnAuthenticated(func(ctx context.Context) {
		log.Println("认证成功")
	})

	bot.OnDisconnected(func(ctx context.Context, reason string) {
		log.Printf("连接断开: %s", reason)
	})

	bot.OnReconnecting(func(ctx context.Context, attempt int) {
		log.Printf("触发重连，第 %d 次", attempt)
	})

	bot.OnError(func(ctx context.Context, err error) {
		log.Printf("SDK 错误: %v", err)
	})
}

func registerBusinessHandlers(bot *wecomaibot.Client) {
	bot.OnText(func(ctx context.Context, msg wecomaibot.TextMessage) {
		safeGo("文本消息处理", func() {
			streamID := wecomaibot.GenerateReqID("stream")

			if err := replyWithRetry(bot, msg, streamID, "正在处理中...", false); err != nil {
				log.Printf("发送流式首包失败: %v", err)
				return
			}

			result := "收到你的消息: " + msg.Text.Content
			if err := replyWithRetry(bot, msg, streamID, result, true); err != nil {
				log.Printf("发送流式结束包失败: %v", err)
				return
			}
		})
	})

	bot.OnEnterChat(func(ctx context.Context, msg wecomaibot.EventMessage) {
		safeGo("欢迎语处理", func() {
			_, err := bot.ReplyWelcomeText(msg, "您好，我是企业微信智能机器人。")
			if err != nil {
				log.Printf("发送欢迎语失败: %v", err)
			}
		})
	})
}

func replyWithRetry(bot *wecomaibot.Client, msg wecomaibot.TextMessage, streamID string, content string, finish bool) error {
	const maxAttempts = 2
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		_, err := bot.ReplyStreamByID(msg, streamID, content, finish, nil, nil)
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("ReplyStreamByID 失败，第 %d/%d 次: %v", attempt, maxAttempts, err)
		if attempt < maxAttempts {
			time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
		}
	}

	if msg.ChatID != "" {
		_, fallbackErr := bot.SendMarkdown(msg.ChatID, "系统繁忙，请稍后重试")
		if fallbackErr != nil {
			log.Printf("降级发送失败: %v", fallbackErr)
		}
	}

	return lastErr
}

func startHealthServer(bot *wecomaibot.Client) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if bot.IsConnected() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("disconnected"))
	})

	go func() {
		if err := http.ListenAndServe(":8080", mux); err != nil {
			log.Printf("健康检查服务退出: %v", err)
		}
	}()
}

func safeGo(name string, fn func()) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("%s发生 panic: %v", name, rec)
			}
		}()
		fn()
	}()
}
