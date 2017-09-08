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
	"github.com/soveran/redisurl"
)

var (
	N, _ = strconv.Atoi(os.Getenv("N")) // how many keys to create.
	M, _ = strconv.Atoi(os.Getenv("M")) // the total sum of each key.
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
		consumeSetBit(addrs, config, pool)
	case "produce":
		produce(addrs, config)
	case "check":
		checkBits(pool)
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s <consume|produce>", os.Args[0])
		os.Exit(1)
	}
}

func checkSum(pool *redis.Pool) {
	for {
		conn := pool.Get()
		defer conn.Close()

		prefix := os.Getenv("KEY_PREFIX")

		keys, err := redis.Strings(conn.Do("KEYS", prefix+":*"))
		if err != nil {
			log.Fatal(err)
		}

		want := 0
		for i := 0; i < M; i++ {
			want += i
		}

		found := false
		for _, k := range keys {
			v, err := redis.Int64(conn.Do("GET", k))
			if err != nil {
				log.Fatal(err)
			}

			if int64(want) != v {
				found = true
				log.Printf("key=%s got=%d want=%d", k, v, want)
			}
		}

		if !found {
			log.Printf("keys=%d all good", len(keys))
		}

		time.Sleep(10 * time.Second)
	}
}

func checkBits(pool *redis.Pool) {
	for {
		conn := pool.Get()
		defer conn.Close()

		prefix := os.Getenv("KEY_PREFIX")

		keys, err := redis.Strings(conn.Do("KEYS", prefix+":*"))
		if err != nil {
			log.Fatal(err)
		}

		conn.Send("MULTI")
		for i := 0; i < M; i++ {
			conn.Send("SETBIT", prefix+":sentinel", i, 1)
		}
		if _, err := conn.Do("EXEC"); err != nil {
			log.Fatal(err)
		}

		want, err := redis.String(conn.Do("GET", prefix+":sentinel"))
		if err != nil {
			log.Fatal(err)
		}

		found := false
		for _, k := range keys {
			v, err := redis.String(conn.Do("GET", k))
			if err != nil {
				log.Fatal(err)
			}

			if want != v {
				found = true
				log.Printf("key=%s got=%s want=%s", k, v, want)
			}
		}

		if !found {
			log.Printf("keys=%d all good", len(keys))
		}

		time.Sleep(10 * time.Second)
	}
}

func produce(addrs []string, config *cluster.Config) {
	p, err := sarama.NewSyncProducer(addrs, &config.Config)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	total := 0
	errors := 0
	topic := os.Getenv("TOPIC")

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
				Topic: topic,
			})

			if err != nil {
				errors++
			}
			total++
		}
	}

}

func consumeSetBit(addrs []string, config *cluster.Config, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	// init consumer
	topics := []string{os.Getenv("TOPIC")}
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
	errtick := time.Tick(time.Second)

	l := func(err error) {
		select {
		case <-errtick:
			log.Println(err)
		default:
		}
	}

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
				v, err := strconv.Atoi(string(msg.Value))
				if err != nil {
					l(err)
					continue
				}

				key := prefix + ":" + string(msg.Key)
				_, err = conn.Do("SETBIT", key, v, 1)
				if err != nil {
					l(err)
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

func consumeSum(addrs []string, config *cluster.Config, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	// init consumer
	topics := []string{os.Getenv("TOPIC")}
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
	errtick := time.Tick(time.Second)

	l := func(err error) {
		select {
		case <-errtick:
			log.Println(err)
		default:
		}
	}

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
					if err != redis.ErrNil {
						l(err)
						errors++
						continue
					}
				}

				v, err := strconv.Atoi(string(msg.Value))
				if err != nil {
					l(err)
					continue
				}

				if _, err = conn.Do("SET", key, curr+int64(v)); err != nil {
					l(err)
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
