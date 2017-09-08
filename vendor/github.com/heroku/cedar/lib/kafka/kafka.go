package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// Dialer builds clients given a set of options.
type Dialer func(opts ...OptionFunc) (sarama.Client, error)

// Addrs parses the comma-separated URLs
// and returns a slice of broker addresses.
func Addrs(urls string) ([]string, error) {
	if urls == "" {
		return nil, errors.New("no broker addresses")
	}

	addrs := []string{}
	for _, uri := range strings.Split(urls, ",") {
		u, err := url.Parse(uri)
		if err != nil {
			return nil, err
		}

		addrs = append(addrs, u.Host)
	}

	return addrs, nil
}

// NewConfig creates a sarama.Config with the appropriate version.
func NewConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0
	return cfg
}

// ConfigureTLS sets up required TLS configuration
// based on the input certs and keys.
func ConfigureTLS(cfg *sarama.Config, cert, key, caCert string) error {
	if cert == "" || key == "" || caCert == "" {
		return nil
	}

	tlsConfig, err := newTLSConfig(cert, key, caCert)
	if err != nil {
		return err
	}

	cfg.Net.TLS.Config = tlsConfig
	cfg.Net.TLS.Enable = true

	return nil
}

// OptionFunc is any option you can pass to NewClient.
type OptionFunc func(*sarama.Config)

// WithConsumerOffsetsInitial sets the Consumer.Offsets.Initial of the cloned
// config to the given offset value.
func WithConsumerOffsetsInitial(offset int64) OptionFunc {
	return func(cfg *sarama.Config) {
		cfg.Consumer.Offsets.Initial = offset
	}
}

// WithSyncProducerSettings configures the sarama.Config to work with a sync
// Producer.
func WithSyncProducerSettings() OptionFunc {
	return func(cfg *sarama.Config) {
		cfg.Producer.RequiredAcks = sarama.WaitForAll
		cfg.Producer.Return.Errors = true
		cfg.Producer.Return.Successes = true
	}
}

// NewClient is a wrapper for sarama.NewClient but clones the config. It also
// takes optional parameters to customize the config after cloning.
func NewClient(brokers []string, baseCfg *sarama.Config, opts ...OptionFunc) (sarama.Client, error) {
	cfg := cloneConfig(baseCfg)

	for _, o := range opts {
		o(cfg)
	}

	return sarama.NewClient(brokers, cfg)
}

func newTLSConfig(clientCert, clientKey, caCert string) (*tls.Config, error) {
	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCert))
	config := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            caCertPool,
	}
	config.BuildNameToCertificate()
	return config, nil
}

func cloneConfig(cfg *sarama.Config) *sarama.Config {
	copy := *cfg
	return &copy
}

func cloneMessage(src *sarama.ProducerMessage) *sarama.ProducerMessage {
	copy := *src
	return &copy
}
