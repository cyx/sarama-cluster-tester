package kafka

import "github.com/Shopify/sarama"

// NewSyncProducer takes a set of broker addrs and a sarama.Config and prepares
// all the necessary config flags to ensure a sync producer will work correctly.
func NewSyncProducer(brokers []string, baseCfg *sarama.Config) (sarama.SyncProducer, error) {
	cfg := cloneConfig(baseCfg)

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true

	return sarama.NewSyncProducer(brokers, cfg)
}
