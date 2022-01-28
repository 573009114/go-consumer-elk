package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/olivere/elastic"
)

// Config 配置
type Config struct {
	MaxMessage int `xml:"max_msg"`          // 最大缓冲
	WorkerNum  int `xml:"worker_number"`    // 线程个数
	BatchSize  int `xml:"batch_size"`       // 每个批次最大条数
	TickTime   int `xml:"tick_millisecond"` // 处理频率
}

type task struct {
	key string
	val []byte
}

// Worker 消息处理器
type Worker struct {
	msgQ   chan *task
	client *elastic.Client
	wg     sync.WaitGroup
	config *Config
}

// NewWorker 构造
func NewWorker(client *elastic.Client, cfg *Config) *Worker {
	w := &Worker{
		client: client,
		config: cfg,
		msgQ:   make(chan *task, cfg.MaxMessage),
	}

	return w
}

// Run 开工
func (w *Worker) Run(ctx context.Context) {

	// 线程数
	thread := w.config.WorkerNum
	if thread <= 0 {
		thread = runtime.NumCPU()
	}

	// ticker
	tickTime := time.Duration(w.config.TickTime) * time.Millisecond
	if tickTime <= 0 {
		tickTime = time.Duration(100) * time.Millisecond
	}

	// 启动
	for i := 0; i < thread; i++ {
		w.wg.Add(1)
		time.Sleep(tickTime / time.Duration(thread))
		go func(idx int) {
			// 构造一个service，server可以反复使用
			service := w.client.Bulk()
			service.Refresh("wait_for")
			defer service.Reset()

			log.Printf("[elastic_worker] worker[%d] start", idx)
			defer w.wg.Done()

			// ticker
			ticker := time.NewTicker(tickTime)

			defer ticker.Stop()

		LOOP:
			for {
				select {
				case <-ctx.Done():

					log.Printf("[elastic_worker] worker[%d] is quiting", idx)
					// 要把通道里的全部执行完才能退出
					for {
						if num := w.process(service); num > 0 {
							log.Printf("[elastic_worker] worker[%d] process batch [%d] when quiting", idx, num)
						} else {
							break LOOP
						}
						time.Sleep(tickTime)
					}

				case <-ticker.C:
					if num := w.process(service); num > 0 {
						log.Printf("[elastic_worker] worker[%d] process batch [%d] ", idx, num)
					}
				}
			}

			log.Printf("[elastic_worker] worker[%d] stop", idx)
		}(i)
	}
}

// AddTask 添加任务，goroutine safe
func (w *Worker) AddTask(key string, val []byte) {
	t := &task{
		key: key,
		val: val,
	}
	w.msgQ <- t
}

// process 处理任务
func (w *Worker) process(service *elastic.BulkService) int {
	//service.Reset()

	// 每个批次最多w.config.BatchSize个
LOOP:
	for i := 0; i < w.config.BatchSize; i++ {
		// 有任务就加到这个批次，没任务就退出
		select {
		case m := <-w.msgQ:
			req := elastic.NewBulkIndexRequest().Index(m.key).Type("doc").Doc(json.RawMessage(m.val))
			service.Add(req)
		default:
			break LOOP
		}
	}

	total := service.NumberOfActions()
	if total > 0 {
		if resp, err := service.Do(context.Background()); nil != err {
			panic(err)
		} else {
			if resp.Errors {
				for _, v := range resp.Failed() {
					fmt.Println("service.Do failed", v)
				}
				panic("resp.Errors")
			}
		}
	}

	return total
}

// Close 关闭 需要外面的context关闭，和等待msgQ任务被执行完毕
func (w *Worker) Close() {
	w.wg.Wait()
	if n := len(w.msgQ); n > 0 {
		log.Printf("[elastic_worker] worker Close remain msg[%d]", n)
	}
}
