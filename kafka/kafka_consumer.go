package kafka

import (
	"github.com/IBM/sarama"
	"github.com/always-web/go-pkg/logger"
	"github.com/eapache/go-resiliency/breaker"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	// KafkaConsumerConnected 消费者已经连接
	KafkaConsumerConnected string = "connected"

	// KafkaConsumerDisconnected 消费者已经断开
	KafkaConsumerDisconnected string = "disconnected"
)

type Consumer struct {
	hosts      []string
	topics     []string
	config     *sarama.Config
	consumer   sarama.Consumer
	status     string
	groupId    string
	breaker    *breaker.Breaker
	reConnect  chan bool
	statusLock sync.Mutex
	exit       bool
}

func (c *Consumer) keepConnect() {
	//for !c.exit {
	//	select {
	//	case <-c.reConnect:
	//		if c.status != KafkaConsumerDisconnected {
	//			break
	//		}
	//		logger.Warn("KafkaConsumer reconnecting", zap.Any(c.groupId, c.topics))
	//		var consumer sarama.Consumer
	//	breakLoop:
	//		for {
	//			err := c.breaker.Run(func() (err error) {
	//				consumer, err = sarama.NewConsumer(c.hosts, c.config)
	//				return
	//			})
	//
	//		}
	//	}
	//}
}

// KafkaMessageHandler 消费者回掉函数
type KafkaMessageHandler func(msg *sarama.ConsumerMessage) (bool, error)

// getKafkaDefaultConsumerConfig 消费者配置
func getKafkaDefaultConsumerConfig() (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 1024 * 1024 * 2
	config.Version = sarama.V2_8_2_0
	return
}

func StartKafkaConsumer(hosts, topics []string, groupId string, config *sarama.Config, f KafkaMessageHandler) (*Consumer, error) {
	var err error
	if config == nil {
		config = getKafkaDefaultConsumerConfig()
	}
	consumer := &Consumer{
		hosts:     hosts,
		topics:    topics,
		config:    config,
		status:    KafkaConsumerDisconnected,
		groupId:   groupId,
		breaker:   breaker.New(3, 1, 3*time.Second),
		reConnect: make(chan bool),
		exit:      false,
	}
	if consumer.consumer, err = sarama.NewConsumer(hosts, config); err != nil {
		return nil, err
	}
	consumer.status = KafkaConsumerConnected
	logger.Info("kafka consumer started,", zap.Any(groupId, topics))
	go consumer.keepConnect()
	//go consumer.consumerMessage(f)
	return consumer, err
}
