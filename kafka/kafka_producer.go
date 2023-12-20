package kafka

import (
	"github.com/IBM/sarama"
	"github.com/always-web/go-pkg/logger"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type kafkaProducer struct {
	Name       string
	Hosts      []string
	Config     *sarama.Config
	Status     string
	Breaker    *breaker.Breaker
	ReConnect  chan bool
	StatusLock sync.Mutex
}

// KafkaMsg 发送消息的实体
type KafkaMsg struct {
	Topic     string
	KeyBytes  []byte
	DataBytes []byte
}

// SyncProducer 同步生产者
type SyncProducer struct {
	kafkaProducer
	SyncProducer sarama.SyncProducer
}

func GetKafkaSyncProducer(name string) *SyncProducer {
	if producer, ok := KafkaSyncProducers[name]; ok {
		return producer
	}
	KafkaStdLogger.Println("InitSyncKafkaProducer must be called !")
	return nil
}

// keepConnect 检查同步生产者的连接状态，如果断开则尝试重连
func (syncProducer *SyncProducer) keepConnect() {
	defer func() {
		KafkaStdLogger.Println("sync producer keep connect exit. name is " + syncProducer.Name)
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == KafkaProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = KafkaProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		case <-syncProducer.ReConnect:
			if syncProducer.Status != KafkaProducerDisconnected {
				break
			}
			KafkaStdLogger.Println("Kafka syncProducer ReConnecting... name " + syncProducer.Name)
			var producer sarama.SyncProducer
		syncBreakLoop:
			for {
				err := syncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewSyncProducer(syncProducer.Hosts, syncProducer.Config)
					return
				})
				switch {
				case err == nil:
					syncProducer.StatusLock.Lock()
					if syncProducer.Status == KafkaProducerDisconnected {
						syncProducer.SyncProducer = producer
						syncProducer.Status = KafkaProducerConnectd
					}
					syncProducer.StatusLock.Unlock()
					KafkaStdLogger.Println("Kafka syncProducer ReConnected,name: ", syncProducer.Name)
					break syncBreakLoop
				case errors.Is(err, breaker.ErrBreakerOpen):
					KafkaStdLogger.Println("Kafka connect fail, breaker is open")
					// 2s 后重连， 此时 breaker 刚好 half close
					if syncProducer.Status == KafkaProducerConnectd {
						time.AfterFunc(2*time.Second, func() {
							KafkaStdLogger.Println("Kafka begin to Reconnect, because of ErrBreakerOpen ")
							syncProducer.ReConnect <- true
						})
					}
					break syncBreakLoop
				default:
					KafkaStdLogger.Println("Kafka Reconnect error, name: ", syncProducer.Name, err)
				}
			}
		}
	}
}

// check 检查生产者状态
func (syncProducer *SyncProducer) check() {
	defer func() {
		KafkaStdLogger.Println("syncProducer check exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == KafkaProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = KafkaProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		}
	}
}

// AsyncProducer 异步生产者
type AsyncProducer struct {
	kafkaProducer
	AsyncProducer sarama.AsyncProducer
}

func GetKafkaAsyncProducer(name string) *AsyncProducer {
	if producer, ok := KafkaAsyncProducers[name]; ok {
		return producer
	}
	KafkaStdLogger.Println("InitAsyncKafkaProducer must be called !")
	return nil
}

func (asyncProducer *AsyncProducer) keepConnect() {
	defer func() {
		KafkaStdLogger.Println("asyncProducer keepConnect exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if asyncProducer.Status == KafkaProducerClosed {
			return
		}
		select {
		case s := <-signals:
			KafkaStdLogger.Println("kafka async producer receive system signal:" + s.String() + "; name:" + asyncProducer.Name)
			asyncProducer.Status = KafkaProducerClosed
			return
		case <-asyncProducer.ReConnect:
			if asyncProducer.Status != KafkaProducerDisconnected {
				break
			}
			KafkaStdLogger.Println("kafka asyncProducer ReConnecting... name" + asyncProducer.Name)
			var producer sarama.AsyncProducer
		asyncBreakLoop:
			for {
				err := asyncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewAsyncProducer(asyncProducer.Hosts, asyncProducer.Config)
					return err
				})
				switch {
				case err == nil:
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == KafkaProducerDisconnected {
						asyncProducer.AsyncProducer = producer
						asyncProducer.Status = KafkaProducerConnectd
					}
					asyncProducer.StatusLock.Unlock()
					logger.Info("kafka asyncProducer ReConnected, name:" + asyncProducer.Name)
					break asyncBreakLoop
				case errors.Is(err, breaker.ErrBreakerOpen):
					KafkaStdLogger.Println("kafka connect fail, breaker is open")
					if asyncProducer.Status == KafkaProducerDisconnected {
						time.AfterFunc(2*time.Second, func() {
							KafkaStdLogger.Println("kafka begin to ReConnect, because of ErrBreakerOpen")
							asyncProducer.ReConnect <- true
						})
					}
					break asyncBreakLoop
				default:
					KafkaStdLogger.Println("kafka ReConnect error, name:"+asyncProducer.Name, zap.Error(err))
				}
			}
		}
	}
}

func (asyncProducer *AsyncProducer) check() {
	defer func() {
		KafkaStdLogger.Println("asyncProducer check exited")
	}()
	for {
		switch asyncProducer.Status {
		case KafkaProducerDisconnected:
			time.Sleep(5 * time.Second)
			continue
		case KafkaProducerClosed:
			return
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		for {
			select {
			case msg := <-(asyncProducer.AsyncProducer).Successes():
				logger.Info("Success produce message ", zap.Any(msg.Topic, msg.Value))
			case err := <-(asyncProducer.AsyncProducer).Errors():
				KafkaStdLogger.Println("message send error", zap.Error(err))
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					// 连接中断触发重连，捕捉不到 EOF
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == KafkaProducerConnectd {
						asyncProducer.Status = KafkaProducerDisconnected
						asyncProducer.ReConnect <- true
					}
					asyncProducer.StatusLock.Unlock()
				}
			case s := <-signals:
				KafkaStdLogger.Println("kafka async producer receive system signal:" + s.String() + "; name: " + asyncProducer.Name)
				asyncProducer.Status = KafkaProducerClosed
				return
			}
		}
	}
}

type stdLogger interface {
	Print(v ...any)
	Printf(format string, v ...any)
	Println(v ...any)
}

const (
	// KafkaProducerConnectd 生产者已连接
	KafkaProducerConnectd string = "connected"

	// KafkaProducerDisconnected 生产者已断开
	KafkaProducerDisconnected string = "disconnected"

	// KafkaProducerClosed 生产者已关闭
	KafkaProducerClosed string = "closed"
)

var (
	ErrProduceTimeout   = errors.New("push message timeout")
	KafkaSyncProducers  = make(map[string]*SyncProducer)
	KafkaAsyncProducers = make(map[string]*AsyncProducer)
	KafkaStdLogger      stdLogger
)

func init() {
	// log.LstdFlags: 日志中包括 包括日期和时间信息. log.Lshortfile: 日志中包括文件名和行号信息
	KafkaStdLogger = log.New(os.Stdout, "[Kafka]", log.LstdFlags|log.Lshortfile)
}

func KafkaMsgValueEncoder(value []byte) sarama.Encoder {
	return sarama.ByteEncoder(value)
}

func KafkaMsgValueStrEncoder(value string) sarama.Encoder {
	return sarama.StringEncoder(value)
}

// getDefaultProducerConfig kafka 生产者默认配置
func getDefaultProducerConfig(clientID string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_8_2_0

	config.Net.DialTimeout = time.Second * 30
	config.Net.WriteTimeout = time.Second * 30
	config.Net.ReadTimeout = time.Second * 30

	config.Producer.Retry.Backoff = time.Millisecond * 500
	config.Producer.Retry.Max = 3

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// 需要小于 broker 的 `message.max.bytes` 配置，默认是 1000000
	config.Producer.MaxMessageBytes = 1000000

	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewHashPartitioner

	// zstd 算法有着最高的压缩比，而在吞吐量上的表现只能说中规中矩，压缩比zstd > LZ4 > GZIP > Snappy
	//LZ4 具有最高的吞吐性能，LZ4 > Snappy > zstd 和 GZIP
	//综上LZ4性价比最高
	config.Producer.Compression = sarama.CompressionLZ4
	return
}

// InitSyncKafkaProducer 初始化同步生产者
func InitSyncKafkaProducer(name string, hosts []string, config *sarama.Config) error {
	syncProducer := &SyncProducer{}
	syncProducer.Name = name
	syncProducer.Hosts = hosts
	syncProducer.Status = KafkaProducerDisconnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	syncProducer.Config = config

	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return errors.Wrap(err, "init kafka sync producer error. name is "+name)
	} else {
		syncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = producer
		syncProducer.Status = KafkaProducerConnectd
		logger.Info("init kafka sync producer success. name is " + name)
	}
	go syncProducer.keepConnect()
	go syncProducer.check()
	KafkaSyncProducers[name] = syncProducer
	return nil
}

func (syncProducer *SyncProducer) Send(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if syncProducer.Status != KafkaProducerConnectd {
		return -1, -1, errors.New("kafka syncProducer " + syncProducer.Status)
	}
	partition, offset, err = syncProducer.SyncProducer.SendMessage(msg)
	if err == nil {
		return
	}
	if errors.Is(err, sarama.ErrBrokerNotAvailable) {
		syncProducer.StatusLock.Lock()
		if syncProducer.Status == KafkaProducerConnectd {
			syncProducer.Status = KafkaProducerDisconnected
			syncProducer.ReConnect <- true
		}
		syncProducer.StatusLock.Unlock()
	}
	return
}

func (syncProducer *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) sarama.ProducerErrors {
	if syncProducer.Status != KafkaProducerConnectd {
		return sarama.ProducerErrors{&sarama.ProducerError{Err: errors.New("kafka syncProducer " + syncProducer.Status)}}
	}
	errs := syncProducer.SyncProducer.SendMessages(msgs)
	if errs != nil {
		var retErrs sarama.ProducerErrors
		ok := errors.As(errs, &retErrs)
		if !ok {
			KafkaStdLogger.Println("retErrs convert error:", zap.Error(errs))
			return retErrs
		}
		for _, err := range retErrs {
			if errors.Is(err, sarama.ErrBrokerNotAvailable) {
				syncProducer.StatusLock.Lock()
				if syncProducer.Status == KafkaProducerConnectd {
					syncProducer.Status = KafkaProducerDisconnected
					syncProducer.ReConnect <- true
				}
				syncProducer.StatusLock.Unlock()
			}
		}
		return retErrs
	}
	return nil
}

// InitAsyncKafkaProducer 初始化异步生产者
func InitAsyncKafkaProducer(name string, hosts []string, config *sarama.Config) error {
	asyncProducer := &AsyncProducer{}
	asyncProducer.Name = name
	asyncProducer.Hosts = hosts
	asyncProducer.Status = KafkaProducerDisconnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	asyncProducer.Config = config

	if producer, err := sarama.NewAsyncProducer(hosts, config); err != nil {
		return errors.Wrap(err, "init kafka async producer error. name: "+name)
	} else {
		asyncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		asyncProducer.ReConnect = make(chan bool)
		asyncProducer.AsyncProducer = producer
		asyncProducer.Status = KafkaProducerConnectd
		KafkaStdLogger.Println("AsyncKafkaProducer connected name ", name)
	}
	go asyncProducer.keepConnect()
	go asyncProducer.check()
	KafkaAsyncProducers[name] = asyncProducer
	return nil
}

func (asyncProducer *AsyncProducer) Send(msg *sarama.ProducerMessage) error {
	if asyncProducer.Status != KafkaProducerConnectd {
		return errors.New("Kafka disconnected")
	}
	(asyncProducer.AsyncProducer).Input() <- msg
	return nil
}

func (asyncProducer *AsyncProducer) Close() error {
	asyncProducer.StatusLock.Lock()
	defer asyncProducer.StatusLock.Unlock()
	err := asyncProducer.AsyncProducer.Close()
	asyncProducer.Status = KafkaProducerClosed
	return err
}
