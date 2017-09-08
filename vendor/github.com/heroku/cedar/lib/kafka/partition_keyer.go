package kafka

import (
	"github.com/Shopify/sarama"
)

// PartitionKeyEncoder adds an additional partition key to a sarama.Encoder
type PartitionKeyEncoder interface {
	PartitionKey() sarama.Encoder
}

// PartitionKeyer contains an encoded key and encoded partition key for
// allowing message keying and partitioning on separate fields
type PartitionKeyer struct {
	sarama.Encoder
	PartKey sarama.Encoder
}

// PartitionKey returns an encoder wrapping PartitionKeyer's extra key
func (k PartitionKeyer) PartitionKey() sarama.Encoder {
	return k.PartKey
}

type partitionKeyPartitioner struct {
	sarama.Partitioner
}

// NewPartitionKeyPartitioner builds a hash partitioner that will employ
// a PartitionKeyEncoder's PartitionKey() field instead of message.Key
// Assign to sarama.Config.Producer.Partitioner
func NewPartitionKeyPartitioner(topic string) sarama.Partitioner {
	return &partitionKeyPartitioner{sarama.NewHashPartitioner(topic)}
}

func (p *partitionKeyPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	key := message.Key
	if pk, ok := key.(PartitionKeyEncoder); ok {
		key = pk.PartitionKey()
	}

	partitionerMessage := cloneMessage(message)
	partitionerMessage.Key = key
	return p.Partitioner.Partition(partitionerMessage, numPartitions)
}
