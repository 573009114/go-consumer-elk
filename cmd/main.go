package main

import (
	"context"
	"encoding/xml"
	"flag"
	"fmt"
	"go-consumer-elk/application/controllers"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/olivere/elastic"
)

// Consumer Consumer配置
type ConsumerConfig struct {
	Topic       []string `xml:"topic"`
	Broker      string   `xml:"broker"`
	Partition   int32    `xml:"partition"`
	Replication int16    `xml:"replication"`
	Group       string   `xml:"group"`
	Version     string   `xml:"version"`
}

// Config 配置
type Config struct {
	Consumer   ConsumerConfig     `xml:"consumer"`
	ElasticURL string             `xml:"elastic_url"`
	Filters    []string           `xml:"filter"`
	Worker     controllers.Config `xml:"elastic_worker"`
}

var (
	configFile = "" // 配置路径
	initTopic  = false
	listTopic  = false
	delTopic   = ""
	cfg        = &Config{}
	web        = ""
)

func init() {
	flag.StringVar(&configFile, "config", "config/cfg.xml", "config file ")
	flag.BoolVar(&initTopic, "init", initTopic, "create topic")
	flag.BoolVar(&listTopic, "list", listTopic, "list topic")
	flag.StringVar(&delTopic, "del", delTopic, "delete topic")
}

var (
	elasticClient *elastic.Client
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	defer time.Sleep(time.Second)

	// 获取host名字
	hostName, err := os.Hostname()
	if nil != err {
		hostName = "[beats]"
	}

	// 加载配置
	if contents, err := ioutil.ReadFile(configFile); err != nil {
		panic(err)
	} else {
		if err = xml.Unmarshal(contents, cfg); err != nil {
			panic(err)
		}
	}

	// sarama的logger
	sarama.Logger = log.New(os.Stdout, fmt.Sprintf("[%s]", hostName), log.LstdFlags)

	// 指定kafka版本，一定要支持kafka集群
	version, err := sarama.ParseKafkaVersion(cfg.Consumer.Version)
	if err != nil {
		panic(err)
	}
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.ClientID = hostName

	// 工具
	if tool(cfg, config) {
		return
	} else {
		initTopic = true
		tool(cfg, config)
	}

	// 启动elastic客户端
	urls := strings.Split(cfg.ElasticURL, ",")
	if cli, err := elastic.NewClient(elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(60*time.Second),
		elastic.SetGzip(true),
	); err != nil {
		panic(err)
	} else {
		elasticClient = cli
		// ping检查
		if ret, _, err := elasticClient.Ping(urls[0]).Do(context.Background()); nil != err {
			panic(err)
		} else {
			log.Printf("elasticClient.Ping %+v", ret)
		}

		defer elasticClient.Stop()
	}

	// ctx
	ctx, cancel := context.WithCancel(context.Background())

	// Worker
	worker := controllers.NewWorker(elasticClient, &cfg.Worker)
	worker.Run(ctx)

	defer worker.Close()

	// kafka consumer client
	kafkaClient, err := sarama.NewConsumerGroup(strings.Split(cfg.Consumer.Broker, ","), cfg.Consumer.Group, config)
	if err != nil {
		panic(err)
	}

	consumer := controllers.NewMyConsumer(worker, ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := kafkaClient.Consume(ctx, cfg.Consumer.Topic, consumer)
				if err != nil {
					log.Printf("[main] client.Consume error=[%s]", err.Error())
					time.Sleep(time.Second)
				}
			}
		}
	}()

	// os signal
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	//time.Sleep(time.Second * 4)
	sig := <-sigterm
	log.Printf("[main] os sig=[%v]", sig)

	cancel()
	log.Printf("[main] cancel")
	if err := kafkaClient.Close(); nil != err {
		log.Printf("[main] kafkaClient close error=[%s]", err.Error())
	}

	log.Printf("[main] beats quit")
}

func tool(cfg *Config, config *sarama.Config) bool {
	if initTopic || listTopic || len(delTopic) > 0 {
		ca, err := sarama.NewClusterAdmin(strings.Split(cfg.Consumer.Broker, ","), config)
		if nil != err {
			panic(err)
		}

		if len(delTopic) > 0 { // 删除Topic
			if err := ca.DeleteTopic(delTopic); nil != err {
				panic(err)
			}
			log.Printf("delete ok topic=[%s]\n", delTopic)
		} else if initTopic { // 初始化Topic
			if detail, err := ca.ListTopics(); nil != err {
				panic(err)
			} else {
				for _, v := range cfg.Consumer.Topic {
					if d, ok := detail[v]; ok {
						if cfg.Consumer.Partition > d.NumPartitions {
							if err := ca.CreatePartitions(v, cfg.Consumer.Partition, nil, false); nil != err {
								panic(err)
							}
							log.Println("alter topic ok", v, cfg.Consumer.Partition)
						}

					} else {
						if err := ca.CreateTopic(v, &sarama.TopicDetail{NumPartitions: cfg.Consumer.Partition, ReplicationFactor: cfg.Consumer.Replication}, false); nil != err {
							panic(err)
						}
						log.Println("create topic ok", v)
					}
				}
			}
		}

		// 显示Topic列表
		if detail, err := ca.ListTopics(); nil != err {
			log.Println("ListTopics error", err)
		} else {
			for k := range detail {
				log.Printf("[%s] %+v", k, detail[k])
			}
		}

		if err := ca.Close(); nil != err {
			panic(err)
		}

		return true
	}
	return false
}
