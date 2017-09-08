package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/garyburd/redigo/redis"
)

func consumeConfluent(addrs []string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	// init consumer
	topics := []string{os.Getenv("TOPIC")}
	group := os.Getenv("GROUP_ID")
	prefix := os.Getenv("KEY_PREFIX")
	broker := strings.Join(addrs, ",")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"enable.auto.commit":              false,
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true
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

	for run == true {
		select {
		case <-tick:
			log.Printf("count#consumer-total=%d count#consumer-errors=%d", total, errors)
			total = 0
			errors = 0

		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:

				total++

				v, err := strconv.Atoi(string(e.Value))
				if err != nil {
					l(err)
					continue
				}

				key := prefix + ":" + string(e.Key)
				_, err = conn.Do("SETBIT", key, v, 1)
				if err != nil {
					l(err)
					errors++
					continue
				}

				c.CommitMessage(e) // mark message as processed

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
