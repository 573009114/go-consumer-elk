package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

// LogJson json格式
type LogJson struct {
	Tag     string    `json:"tag"`
	Level   string    `json:"level"`
	File    string    `json:"file"`
	Time    time.Time `json:"@timestamp"`
	Message string    `json:"message"`
}

type taskProcessor interface {
	AddTask(key string, val []byte)
}

// MyConsumer 可关闭的带任务处理器的消费者
type MyConsumer struct {
	processor taskProcessor
	ctx       context.Context
}

// NewMyConsumer 构造
func NewMyConsumer(p taskProcessor, ctx context.Context) *MyConsumer {
	c := &MyConsumer{
		processor: p,
		ctx:       ctx,
	}

	return c
}

// Setup 启动
func (consumer *MyConsumer) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("[main] consumer.Setup memberID=[%s]", s.MemberID())
	return nil
}

// Cleanup 当退出时
func (consumer *MyConsumer) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("[main] consumer.Cleanup memberID=[%s]", s.MemberID())
	return nil
}

// ConsumeClaim 消费日志
func (consumer *MyConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			js := &LogJson{}
			if err := json.Unmarshal(message.Value, js); nil != err {
				fmt.Fprintf(os.Stderr, "[MyConsumer] ConsumeClaim json.Unmarshal err=[%s] topic=[%s] key=[%s] val=[%s]\n", err.Error(), message.Topic, message.Key, string(message.Value))
			} else {
				index := fmt.Sprintf("%s-%s", message.Topic, js.Time.Format("2006.01.02"))
				consumer.processor.AddTask(index, message.Value)
				session.MarkMessage(message, "")
			}

		case <-consumer.ctx.Done():
			return nil
		}
	}

}
