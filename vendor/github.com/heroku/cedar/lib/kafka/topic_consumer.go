package kafka

import (
	"bytes"
	"io"

	"github.com/Shopify/sarama"
	"github.com/heroku/cedar/lib/canceler"
	"golang.org/x/net/context"
)

// TopicConsumer returns messages from multiple PartitionConsumers.
type TopicConsumer interface {
	GetMessage(ctx context.Context) (*sarama.ConsumerMessage, error)
	Close() error
}

type topicConsumer struct {
	canceler           *canceler.Canceler
	partitionConsumers []sarama.PartitionConsumer
	messages           chan *sarama.ConsumerMessage
	errors             chan *sarama.ConsumerError
	filterFunc         func(*sarama.ConsumerMessage) bool
}

// tombstoneFilter is a filterFunc which will make a TopicConsumer
// skip over null msg.Values.
func tombstoneFilter(msg *sarama.ConsumerMessage) bool {
	return !bytes.Equal(nil, msg.Value)
}

// noopFilter is the default filterFunc for a TopicConsumer. It
// allows all messages to go through.
func noopFilter(msg *sarama.ConsumerMessage) bool {
	return true
}

// TCOptionFunc is anything you can pass in to NewTopicConsumer
// to customize the behavior of the consumer.
type TCOptionFunc func(*topicConsumer)

// SkipTombstones is an OptionFunc that you can initialize as part
// of calling NewTopicConsumer which makes the consumer skip over all
// tombstones(e.g. msg.Value is nil).
func SkipTombstones(tc *topicConsumer) {
	tc.filterFunc = tombstoneFilter
}

// NewTopicConsumer creates a new consumer with provided partitions.
func NewTopicConsumer(partitionConsumers []sarama.PartitionConsumer, opts ...TCOptionFunc) TopicConsumer {
	tc := &topicConsumer{
		canceler:           canceler.New(),
		partitionConsumers: partitionConsumers,
		messages:           make(chan *sarama.ConsumerMessage),
		errors:             make(chan *sarama.ConsumerError),
		filterFunc:         noopFilter,
	}

	for _, o := range opts {
		o(tc)
	}

	tc.consumePartitions()

	return tc
}

func (tc *topicConsumer) consumePartitions() {
	for _, pc := range tc.partitionConsumers {
		tc.consumePartition(pc)
	}
}

func (tc *topicConsumer) consumePartition(pc sarama.PartitionConsumer) {
	go func() {
		for msg := range pc.Messages() {
			if !tc.filterFunc(msg) {
				continue
			}

			select {
			case tc.messages <- msg:
			case <-tc.canceler.Done():
			}
		}
	}()

	go func() {
		for err := range pc.Errors() {
			select {
			case tc.errors <- err:
			case <-tc.canceler.Done():
			}
		}
	}()
}

// Close stops reading new messages from Kafka
// closes each partition consumer.
func (tc *topicConsumer) Close() error {
	tc.canceler.Cancel()

	for _, pc := range tc.partitionConsumers {
		pc.Close()
	}

	return nil
}

// GetMessage waits on partition consumer channels for messages errors or signals.
func (tc *topicConsumer) GetMessage(ctx context.Context) (*sarama.ConsumerMessage, error) {
	select {
	case msg := <-tc.messages:
		return msg, nil
	case err := <-tc.errors:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-tc.canceler.Done():
		return nil, canceler.ErrCanceled
	}
}

type updateOffsets struct {
	TopicConsumer
	OffsetManager
}

func (u *updateOffsets) GetMessage(ctx context.Context) (*sarama.ConsumerMessage, error) {
	msg, err := u.TopicConsumer.GetMessage(ctx)
	if err != nil {
		return nil, err
	}

	if err = u.OffsetManager.MarkOffset(msg.Partition, msg.Offset); err != nil {
		return nil, err
	}

	return msg, nil
}

type checkCaughtUp struct {
	TopicConsumer
	OffsetManager
	targetOffsets TopicOffsets
}

func (c *checkCaughtUp) GetMessage(ctx context.Context) (*sarama.ConsumerMessage, error) {
	if c.isCaughtUp() {
		return nil, io.EOF
	}

	return c.TopicConsumer.GetMessage(ctx)
}

// targetOffsetEmpty is the target when GetOffset for
// an empty partition returns 0 as the next offset to
// be produced.
const targetOffsetEmpty = -1

func (c *checkCaughtUp) isCaughtUp() bool {
	if c.targetOffsets == nil {
		return false
	}

	for p, o := range c.targetOffsets {
		if o != targetOffsetEmpty && c.OffsetManager.Offsets()[p] < o {
			return false
		}
	}

	return true
}

type skipTombstones struct {
	TopicConsumer
}

func (s *skipTombstones) GetMessage(ctx context.Context) (*sarama.ConsumerMessage, error) {
	for {
		msg, err := s.TopicConsumer.GetMessage(ctx)
		if err != nil {
			return nil, err
		}

		// use len to cover nil or empty msg.Value
		if len(msg.Value) == 0 {
			continue
		}

		return msg, nil
	}
}

type skipFirstMessage struct {
	TopicConsumer
	initialOffsets TopicOffsets
}

func (s skipFirstMessage) GetMessage(ctx context.Context) (*sarama.ConsumerMessage, error) {
	for {
		msg, err := s.TopicConsumer.GetMessage(ctx)
		if err != nil {
			return nil, err
		}

		if s.initialOffsets[msg.Partition] == msg.Offset {
			continue
		}

		return msg, nil
	}
}
