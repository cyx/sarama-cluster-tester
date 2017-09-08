package kafka

import "github.com/Shopify/sarama"

// ConsumeTopicPartitions takes a consumer, topic and set of offsets and creates
// the sarama.PartitionConsumer's for all of the given offsetter.Partitions().
func ConsumeTopicPartitions(cons sarama.Consumer, topic string, offsets TopicOffsets) ([]sarama.PartitionConsumer, error) {
	var pcs []sarama.PartitionConsumer

	for partition, offset := range offsets {
		pc, err := cons.ConsumePartition(topic, partition, offset)
		if err != nil {
			closePartitionConsumers(pcs)
			return nil, err
		}
		pcs = append(pcs, pc)
	}

	return pcs, nil
}

func closePartitionConsumers(pcs []sarama.PartitionConsumer) {
	for _, pc := range pcs {
		pc.Close()
	}
}
