package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/garyburd/redigo/redis"
	"github.com/joeshaw/envdecode"
	"github.com/pborman/uuid"
)

func main() {
	var cfg Config
	envdecode.MustStrictDecode(&cfg)

	pool := newRedisPool()

	addrs, config, err := AddrsConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	switch os.Args[1] {
	case "consume":
		consume(addrs, config, pool)
	case "produce":
		produce(addrs, config)
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s <consume|produce>", os.Args[0])
		os.Exit(1)
	}
}

func produce(addrs []string, config *cluster.Config) {
	p, err := sarama.NewSyncProducer(addrs, &config.Config)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	N := 100 // how many keys to create.
	M := 100 // the total sum of each key.

	total := 0
	errors := 0

	tick := time.Tick(time.Second)

	for i := 0; i < N; i++ {
		id := uuid.New()

		select {
		case <-tick:
			log.Printf("count#producer.errors=%d count#producer.total=%d", errors, total)
			total = 0
			errors = 0

		default:
		}

		for j := 0; j < M; j++ {
			_, _, err := p.SendMessage(&sarama.ProducerMessage{
				Key:   sarama.StringEncoder(id),
				Value: sarama.StringEncoder(strconv.Itoa(j)),
				Topic: "sum",
			})

			if err != nil {
				errors++
			}
			total++
		}
	}

}

func consume(addrs []string, config *cluster.Config, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	// init consumer
	topics := []string{"sum"}
	group := os.Getenv("GROUP_ID")
	prefix := os.Getenv("KEY_PREFIX")

	consumer, err := cluster.NewConsumer(addrs, group, topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	errors := 0
	total := 0
	tick := time.Tick(time.Second)

	// consume messages, watch errors and notifications
	for {
		total++

		select {
		case <-tick:
			log.Printf("count#consumer-total=%d count#consumer-errors=%d", total, errors)
			total = 0
			errors = 0

		case msg, more := <-consumer.Messages():
			if more {
				key := prefix + ":" + string(msg.Key)
				curr, err := redis.Int64(conn.Do("GET", key))
				if err != nil {
					errors++
					continue
				}

				v, err := strconv.Atoi(string(msg.Value))
				if err != nil {
					log.Println(err)
					continue
				}

				if _, err = conn.Do("SET", key, curr+v); err != nil {
					errors++
					continue
				}

				consumer.MarkOffset(msg, "") // mark message as processed
			}

		case err, more := <-consumer.Errors():
			if more {
				log.Printf("Error: %s\n", err.Error())
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.Printf("Rebalanced: %+v\n", ntf)
			}
		case <-signals:
			return
		}
	}
}

func newRedisPool() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			conn, err := redisurl.ConnectToURL(os.Getenv("REDIS_URL"))
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
	}
}
