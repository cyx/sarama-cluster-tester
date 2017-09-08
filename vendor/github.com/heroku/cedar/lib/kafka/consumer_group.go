package kafka

import (
	"fmt"
	"io"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// ConsumerGroup is a superset of TopicConsumer and just adds MarkOffset on top
// of it.
type ConsumerGroup interface {
	TopicConsumer
	OffsetManager
}

// CGOptionFunc is any option you can pass in to NewConsumerGroup.
type CGOptionFunc func(*consumerGroup) error

// WithNativeOffsetManager uses the built in offset management in kafka for
// marking offsets.
func WithNativeOffsetManager(group string) CGOptionFunc {
	return func(cg *consumerGroup) error {
		om, err := newOffsetManager(cg.client, group, cg.topic)
		if err != nil {
			return err
		}
		cg.OffsetManager = om
		return nil
	}
}

// WithDefaultOffsetManager uses local offset management with preferred
// defaults (e.g. start from oldest offset and no initial offsets)
func WithDefaultOffsetManager() CGOptionFunc {
	return WithOffsets(nil, sarama.OffsetOldest)
}

// WithOffsets uses a given set of offsets when starting the individual
// partition consumers.
func WithOffsets(offsets TopicOffsets, defaultOffset int64) CGOptionFunc {
	return func(cg *consumerGroup) error {
		if len(offsets) > 0 {
			cg.OffsetManager = offsets
			return nil
		}

		partitions, err := cg.client.Partitions(cg.topic)
		if err != nil {
			return errors.Wrap(err, "getting partitions failed")
		}
		cg.OffsetManager = NewTopicOffsets(partitions, defaultOffset)
		return nil
	}
}

// WithFixExpiredOffsets checks all the given offsets and compares using
// GetOffset to determine its validity. Expired offsets are set to the
// smallest known value.
//
// Example:
//   Given: 0:300,1:301,2:302 (and 0:300 expired, smallest known=400)
//   Want:  0:400,1:301,2:302
func WithFixExpiredOffsets() CGOptionFunc {
	return func(cg *consumerGroup) error {
		given := cg.OffsetManager.Offsets().Copy()

		fixed, err := fixExpiredOffsets(cg.client, cg.topic, given)
		if err != nil {
			return errors.Wrap(err, "fix expired offsets failed")
		}
		cg.OffsetManager = fixed
		return nil
	}
}

// WithSerializedOffsets uses a local topic offset manager and initializes
// it from the given serialized offsets.
func WithSerializedOffsets(serializedOffsets string, defaultOffset int64) CGOptionFunc {
	return func(cg *consumerGroup) error {
		if serializedOffsets != "" {
			offsets := make(TopicOffsets)
			err := UnmarshalOffsets(serializedOffsets, offsets)
			cg.OffsetManager = offsets
			return errors.Wrap(err, "unmarshal offsets failed")
		}

		return WithOffsets(nil, defaultOffset)(cg)
	}
}

// errTopicConsumerMissing is return by a CGOptionFunc which relies on a
// TopicConsumer to already be initialized.
var errTopicConsumerMissing = errors.New("topic consumer missing")

// WithUpdatingOffsets makes the TopicConsumer of the ConsumerGroup mark
// offsets automatically.
func WithUpdatingOffsets() CGOptionFunc {
	return func(cg *consumerGroup) error {
		if cg.TopicConsumer == nil {
			return errTopicConsumerMissing
		}

		cg.TopicConsumer = &updateOffsets{cg.TopicConsumer, cg.OffsetManager}
		return nil
	}
}

// WithSkipFirstMessage makes the TopicConsumer of the ConsumerGroup
// skip the first message of each offset.
func WithSkipFirstMessage() CGOptionFunc {
	return func(cg *consumerGroup) error {
		if cg.TopicConsumer == nil {
			return errTopicConsumerMissing
		}

		initialOffsets := cg.OffsetManager.Offsets().Copy()
		cg.TopicConsumer = &skipFirstMessage{cg.TopicConsumer, initialOffsets}
		return nil
	}
}

// WithStreamToCurrent makes the TopicConsumer stream all the way to the
// latest offset -- which is determined at the time of calling NewConsumerGroup.
// Once the targetted offsets are all reached then the stream returns an io.EOF.
func WithStreamToCurrent() CGOptionFunc {
	return func(cg *consumerGroup) error {
		if cg.TopicConsumer == nil {
			return errTopicConsumerMissing
		}

		partitions, err := cg.client.Partitions(cg.topic)
		if err != nil {
			return errors.Wrap(err, "getting partitions")
		}

		targetOffsets, err := getTargetOffsets(cg.client, cg.topic, partitions)
		if err != nil {
			return errors.Wrap(err, "getting target offsets")
		}

		cg.TopicConsumer = &checkCaughtUp{cg.TopicConsumer, cg.OffsetManager, targetOffsets}
		return nil
	}
}

// WithSkipTombstones makes the TopicConsumer skip over tombstone values.
// Composing this with WithStreamToCurrent requires that WithStreamToCurrent
// appears first, and WithSkipTombstones appears next, otherwise
// WithStreamToCurrent might not terminate.
func WithSkipTombstones() CGOptionFunc {
	return func(cg *consumerGroup) error {
		if cg.TopicConsumer == nil {
			return errTopicConsumerMissing
		}

		cg.TopicConsumer = &skipTombstones{cg.TopicConsumer}
		return nil
	}
}

// NewConsumerGroup initializes a TopicConsumer with the configured offset
// management strategy.
func NewConsumerGroup(client sarama.Client, topic string, opts ...CGOptionFunc) (ConsumerGroup, error) {
	c := &consumerGroup{client: client, topic: topic}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, errors.Wrap(err, "new consumer from client failed")
	}
	c.consumer = consumer

	var topicConsumerOpts []CGOptionFunc

	for _, o := range opts {
		if err := o(c); err != nil {
			// If the option is more about manipulating TopicConsumer behavior
			// we'll need to apply it _after_ the basic TopicConsumer is setup.
			if err == errTopicConsumerMissing {
				topicConsumerOpts = append(topicConsumerOpts, o)
				continue
			}

			c.Close()
			return nil, errors.Wrapf(err, "applying option %T failed", o)
		}
	}

	if c.OffsetManager == nil {
		if err := WithDefaultOffsetManager()(c); err != nil {
			c.Close()
			return nil, errors.Wrap(err, "applying default offset manager failed")
		}
	}

	pcs, err := ConsumeTopicPartitions(consumer, topic, c.OffsetManager.Offsets())
	if err != nil {
		c.Close()
		return nil, errors.Wrap(err, "consume topic partitions failed")
	}

	c.TopicConsumer = NewTopicConsumer(pcs)

	for _, o := range topicConsumerOpts {
		if err := o(c); err != nil {
			c.Close()
			return nil, errors.Wrapf(err, "applying %T CGOptionFunc", o)
		}
	}

	return c, nil
}

// ConsumerGroup subscribes to a topic and automatically manages offsets based
// on the configured group. It implements the TopicConsumer interface, and also
// adds a MarkOffset method.
type consumerGroup struct {
	TopicConsumer
	OffsetManager

	// Stored mostly for CGOptionFunc convenience
	client sarama.Client
	topic  string

	consumer sarama.Consumer
}

// Compile time check for TopicConsumer adherance.
var _ ConsumerGroup = &consumerGroup{}

// Close tears down all the underlying kafka machinery in this consumer group.
// It is necessary to call this to avoid leaking resources.
func (c *consumerGroup) Close() error {
	var err error

	// These are done in cascading order as prescribed by sarama docs, e.g.
	// the inner most struct is closed, down to the most general, which is
	// the consumer.
	if c.OffsetManager != nil {
		if closer, ok := c.OffsetManager.(io.Closer); ok {
			err = errors.Wrap(closer.Close(), "offset manager close failed")
		}
	}

	if c.TopicConsumer != nil {
		err = errors.Wrap(c.TopicConsumer.Close(), "tc close failed")
	}

	if c.consumer != nil {
		err = errors.Wrap(c.consumer.Close(), "consumer close failed")
	}

	return err
}

// OffsetManager is any sort of persistence which can mark offsets and also
// return the latest Offsets.
type OffsetManager interface {
	MarkOffset(partition int32, offset int64) error
	Offsets() TopicOffsets
}

// offsetManager stores all the initial topic offsets, as well as all the
// PartitionOffsetManagers for a given topic.
type offsetManager struct {
	poms         map[int32]sarama.PartitionOffsetManager
	om           sarama.OffsetManager
	topicOffsets TopicOffsets
}

// newOffsetManager initializes all the offsets for a given topic. If no
// offsets have been previously marked, then all the partitions will default to
// the given defaultOffset.
func newOffsetManager(client sarama.Client, group, topic string) (*offsetManager, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client partitions")
	}

	om, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		return nil, errors.Wrap(err, "unable to make offset manager from client")
	}

	o := &offsetManager{
		om:           om,
		poms:         make(map[int32]sarama.PartitionOffsetManager),
		topicOffsets: make(TopicOffsets),
	}

	err = o.loadTopicOffsets(client, topic, partitions)
	return o, err
}

// loadTopicOffsets goes over all the given partitions, and assigns the proper
// offset for each of the partition. If any of the pre-existing marked offsets
// are invalid (e.g. maybe a consumer hasn't run in a while), then it will fail
// with an errRange.
func (o *offsetManager) loadTopicOffsets(client sarama.Client, topic string, partitions []int32) error {
	for _, p := range partitions {
		// Figure out the valid offset range for a topic/partition
		// combination.
		min, max, err := minMaxFor(client, topic, p)
		if err != nil {
			return errors.Wrapf(err, "getting min max failed for topic=%s partition=%d", topic, p)
		}

		// Create the PartitionOffsetManager for topic/partition.
		o.poms[p], err = o.om.ManagePartition(topic, p)
		if err != nil {
			return errors.Wrapf(err, "unable to ManagePartition for topic=%s partition=%d", topic, p)
		}

		// Check what the next, prescribed offset is. Can be what we
		// last marked via MarkOffset or Consumer.Offset.Initial.
		offset, _ := o.poms[p].NextOffset()

		// An offset is only valid if any of the following conditions
		// are true:
		// 1. It's a sentinel value (e.g. OffsetOldest / OffsetNewest)
		// 2. It's a real offset, that falls within range of current,
		// valid offsets for the topic/partition combination.
		if offset != sarama.OffsetOldest && offset != sarama.OffsetNewest && (offset < min || offset > max) {
			return errRange{min: min, max: max, offset: offset, partition: p}
		}

		o.topicOffsets[p] = offset
	}
	return nil
}

// minMaxFor gets the lower and upper bounds for a given topic/partition
// combination. This is useful, because kafka WILL delete old offsets
// eventually and trying to consume those will result in an error.
func minMaxFor(client sarama.Client, topic string, p int32) (min, max int64, err error) {
	min, err = client.GetOffset(topic, p, sarama.OffsetOldest)
	if err != nil {
		return 0, 0, err
	}
	max, err = client.GetOffset(topic, p, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, err
	}
	return min, max, err
}

func (o *offsetManager) Offsets() TopicOffsets {
	return o.topicOffsets
}

// Close gracefully tears down all the PartitionOffsetManagers. It is necessary
// to call this to avoid leaking resources.
func (o *offsetManager) Close() error {
	var err error

	// According to sarama docs, partition offset managers should be the
	// firt ones we close.
	for _, pm := range o.poms {
		err = errors.Wrap(pm.Close(), "partition offset manager close failed")
	}

	// The next phase is to close the offset manager.
	err = errors.Wrap(o.om.Close(), "offset manager close failed")

	return err
}

// errRange is returned by newOffsetManager when previously stored offsets in
// are invalid.
type errRange struct {
	partition int32
	offset    int64
	min       int64
	max       int64
}

// Error returns a description of the partition, offset, and min/max conditions
// for the invalid range.
func (e errRange) Error() string {
	return fmt.Sprintf("Offset out of range: (partition=%d) have %d, want %d..%d", e.partition, e.offset, e.min, e.max)
}

// MarkOffset delegates the appropriate call to an underlying
// PartitionOffsetManager. Trying to pass in an unknown partition will result
// in an error.
func (o *offsetManager) MarkOffset(partition int32, offset int64) error {
	pom, ok := o.poms[partition]
	if !ok {
		return errors.Errorf("MarkOffset called with invalid partition %d", partition)
	}
	pom.MarkOffset(offset, "")
	o.topicOffsets[partition] = offset
	return nil
}

// getTargetOffsets gets the current offsets for all partitions in topic and
// subtracts 1 from them so they reflect what exists now.
//
// Called "target" instead of current since "current" in sarama terms seems to
// mean the next offset that will be produced.
func getTargetOffsets(client sarama.Client, topic string, partitions []int32) (TopicOffsets, error) {
	os := NewTopicOffsets(partitions, sarama.OffsetOldest)
	for _, p := range partitions {
		po, err := client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			return nil, errors.Wrapf(err, "getting current offset for partition %d", p)
		}
		os[p] = po - 1 // - 1 to turn next offset to be produced into what exists now
	}
	return os, nil
}

func getOldestOffsets(client sarama.Client, topic string, partitions []int32) (TopicOffsets, error) {
	os := NewTopicOffsets(partitions, sarama.OffsetOldest)
	for _, p := range partitions {
		po, err := client.GetOffset(topic, p, sarama.OffsetOldest)
		if err != nil {
			return nil, errors.Wrapf(err, "getting current offset for partition %d", p)
		}
		os[p] = po
	}
	return os, nil
}

func fixExpiredOffsets(client sarama.Client, topic string, offsets TopicOffsets) (TopicOffsets, error) {
	partitions := make([]int32, len(offsets))
	for p := range offsets {
		partitions = append(partitions, p)
	}

	oldestOffsets, err := getOldestOffsets(client, topic, partitions)
	if err != nil {
		return nil, errors.Wrap(err, "getting oldest offsets")
	}

	result := make(TopicOffsets)
	for p, po := range oldestOffsets {
		result[p] = po

		if offsets[p] == sarama.OffsetOldest || offsets[p] == sarama.OffsetNewest {
			continue
		}

		if offsets[p] < po {
			result[p] = po
		}
	}

	return result, nil
}
