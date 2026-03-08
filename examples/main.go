package main

import (
	"context"
	"log"
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

	bot := wecomaibot.NewClient(wecomaibot.Config{
		BotID:  botID,
		Secret: secret,
	})

	bot.OnAuthenticated(func(ctx context.Context) {
		log.Println("认证成功")
	})

	bot.OnText(func(ctx context.Context, msg wecomaibot.TextMessage) {
		log.Printf("收到文本消息: %s", msg.Text.Content)

		streamID := wecomaibot.GenerateReqID("stream")
		_, _ = bot.ReplyStreamByID(msg, streamID, "正在思考中...", false, nil, nil)

		go func() {
			time.Sleep(time.Second)
			_, err := bot.ReplyStreamByID(msg, streamID, "你好，这是 Go SDK 的流式回复。", true, nil, nil)
			if err != nil {
				log.Printf("发送流式结束消息失败: %v", err)
			}
		}()
	})

	bot.OnEnterChat(func(ctx context.Context, msg wecomaibot.EventMessage) {
		_, err := bot.ReplyWelcomeText(msg, "您好！我是企业微信智能助手（Go SDK）。")
		if err != nil {
			log.Printf("发送欢迎语失败: %v", err)
		}
	})

	if err := bot.Connect(); err != nil {
		log.Fatalf("连接失败: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	bot.Disconnect()
}
