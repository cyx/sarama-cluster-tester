package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/joeshaw/envdecode"
	"github.com/pborman/uuid"
)

func main() {
	var cfg Config
	envdecode.MustStrictDecode(&cfg)

	addrs, config, err := AddrsConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	switch os.Args[1] {
	case "consume":
		consume(addrs, config)
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

	N := 1000 // how many keys to create.
	M := 1000 // the total sum of each key.

	total := 0
	errors := 0

	for i := 0; i < N; i++ {
		id := uuid.New()

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

	log.Printf("count#producer.errors=%d count#producer.total=%d", errors, total)
}

func consume(addrs []string, config *cluster.Config) {
	// init consumer
	topics := []string{"sum"}
	group := os.Getenv("GROUP_ID")
	consumer, err := cluster.NewConsumer(addrs, group, topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume messages, watch errors and notifications
	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
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
