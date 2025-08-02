package kafka_client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/segmentio/kafka-go"
)

const maxEarliest int64 = 100
const network = "tcp"
const debugLogLevel = "debug"
const errorLogLevel = "error"
const dialerTimeout = 10 * time.Second
const defaultHealthcheckTimeout = 2000 // 2 seconds in milliseconds

type Options struct {
	BootstrapServers   string `json:"bootstrapServers"`
	ClientId           string `json:"clientId"`
	SecurityProtocol   string `json:"securityProtocol"`
	SaslMechanisms     string `json:"saslMechanisms"`
	SaslUsername       string `json:"saslUsername"`
	SaslPassword       string `json:"saslPassword"`
	HealthcheckTimeout int32  `json:"healthcheckTimeout"`
	LogLevel           string `json:"logLevel"`
	// TLS Configuration
	TLSAuthWithCACert bool   `json:"tlsAuthWithCACert"`
	TLSAuth           bool   `json:"tlsAuth"`
	TLSSkipVerify     bool   `json:"tlsSkipVerify"`
	ServerName        string `json:"serverName"`
	TLSCACert         string `json:"tlsCACert"`
	TLSClientCert     string `json:"tlsClientCert"`
	TLSClientKey      string `json:"tlsClientKey"`
	// Advanced HTTP settings
	Timeout int32 `json:"timeout"`
}

type KafkaClient struct {
	Dialer             *kafka.Dialer
	Reader             *kafka.Reader
	Conn               *kafka.Client
	BootstrapServers   string
	ClientId           string
	TimestampMode      string
	SecurityProtocol   string
	SaslMechanisms     string
	SaslUsername       string
	SaslPassword       string
	LogLevel           string
	HealthcheckTimeout int32
	// TLS Configuration
	TLSAuthWithCACert bool
	TLSAuth           bool
	TLSSkipVerify     bool
	ServerName        string
	TLSCACert         string
	TLSClientCert     string
	TLSClientKey      string
	// Advanced settings
	Timeout int32
}

type KafkaMessage struct {
	Value     map[string]interface{}
	Timestamp time.Time
	Offset    int64
}

func NewKafkaClient(options Options) KafkaClient {
	healthcheckTimeout := options.HealthcheckTimeout
	if healthcheckTimeout <= 0 {
		healthcheckTimeout = defaultHealthcheckTimeout
	}

	client := KafkaClient{
		BootstrapServers:   options.BootstrapServers,
		ClientId:           options.ClientId,
		SecurityProtocol:   options.SecurityProtocol,
		SaslMechanisms:     options.SaslMechanisms,
		SaslUsername:       options.SaslUsername,
		SaslPassword:       options.SaslPassword,
		LogLevel:           options.LogLevel,
		HealthcheckTimeout: healthcheckTimeout,
		// TLS Configuration
		TLSAuthWithCACert: options.TLSAuthWithCACert,
		TLSAuth:           options.TLSAuth,
		TLSSkipVerify:     options.TLSSkipVerify,
		ServerName:        options.ServerName,
		TLSCACert:         options.TLSCACert,
		TLSClientCert:     options.TLSClientCert,
		TLSClientKey:      options.TLSClientKey,
		// Advanced settings
		Timeout: options.Timeout,
	}
	return client
}

func (client *KafkaClient) NewConnection() error {
	var mechanism sasl.Mechanism
	var err error

	// Set up SASL mechanism if provided
	if client.SaslMechanisms != "" {
		mechanism, err = getSASLMechanism(client)
		if err != nil {
			return fmt.Errorf("unable to get SASL mechanism: %w", err)
		}
	}

	// Configure Dialer
	dialer := &kafka.Dialer{
		Timeout:       dialerTimeout,
		SASLMechanism: mechanism,
		ClientID:      client.ClientId, // Add Client ID support
	}

	// Configure Transport
	transport := &kafka.Transport{
		SASL:     mechanism,
		ClientID: client.ClientId, // Add Client ID support
	}

	// Configure TLS if SSL or SASL_SSL is used
	if client.SecurityProtocol == "SASL_SSL" || client.SecurityProtocol == "SSL" {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: client.TLSSkipVerify,
		}

		// Set server name for TLS verification if provided
		if client.ServerName != "" {
			tlsConfig.ServerName = client.ServerName
		}

		// Add CA certificate if self-signed certificate option is enabled
		if client.TLSAuthWithCACert && client.TLSCACert != "" {
			roots := x509.NewCertPool()
			if ok := roots.AppendCertsFromPEM([]byte(client.TLSCACert)); !ok {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = roots
		}

		// Add client certificate if TLS client authentication is enabled
		if client.TLSAuth && client.TLSClientCert != "" && client.TLSClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(client.TLSClientCert), []byte(client.TLSClientKey))
			if err != nil {
				return fmt.Errorf("failed to parse client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		dialer.TLS = tlsConfig
		transport.TLS = tlsConfig
	}

	client.Dialer = dialer
	client.Conn = &kafka.Client{
		Addr:      kafka.TCP(strings.Split(client.BootstrapServers, ",")...),
		Timeout:   dialerTimeout,
		Transport: transport,
	}

	return nil
}

func (client *KafkaClient) newReader(topic string, partition int) *kafka.Reader {
	logger, errorLogger := getKafkaLogger(client.LogLevel)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(client.BootstrapServers, ","),
		Topic:          topic,
		Partition:      partition,
		Dialer:         client.Dialer,
		CommitInterval: 0,
		Logger:         logger,
		ErrorLogger:    errorLogger,
	})

	return reader
}

func (client *KafkaClient) NewStreamReader(
	ctx context.Context,
	topic string,
	partition int32,
	autoOffsetReset string,
) (*kafka.Reader, error) {
	var offset int64
	var high, low int64

	// Create connection if not exists
	if client.Dialer == nil {
		if err := client.NewConnection(); err != nil {
			return nil, fmt.Errorf("failed to create connection: %w", err)
		}
	}

	// Set up offset
	switch autoOffsetReset {
	case "latest":
		offset = kafka.LastOffset
	case "earliest":
		// Use first bootstrap server as seed broker for leader discovery
		firstBroker := strings.Split(client.BootstrapServers, ",")[0]
		conn, err := client.Dialer.DialLeader(ctx, network, firstBroker, topic, int(partition))
		if err != nil {
			return nil, fmt.Errorf("unable to dial leader: %w", err)
		}
		defer conn.Close()

		low, high, err = conn.ReadOffsets()
		if err != nil {
			return nil, fmt.Errorf("unable to read offsets: %w", err)
		}

		if high-low > maxEarliest {
			offset = high - maxEarliest
		} else {
			offset = low
		}
	default:
		offset = kafka.LastOffset
	}

	// Create new reader
	reader := client.newReader(topic, int(partition))
	if err := reader.SetOffset(offset); err != nil {
		reader.Close()
		return nil, fmt.Errorf("unable to set offset: %w", err)
	}

	return reader, nil
}

type offsetCache struct {
	sync.RWMutex
	entries map[string]int64 // key: "topic/partition/timestampUnix"
}

var cache = &offsetCache{
	entries: make(map[string]int64),
}

func (c *offsetCache) Get(key string) (int64, bool) {
	c.RLock()
	defer c.RUnlock()
	val, ok := c.entries[key]
	return val, ok
}

func (c *offsetCache) Set(key string, value int64) {
	c.Lock()
	defer c.Unlock()
	c.entries[key] = value
}

func (client *KafkaClient) calculateTimeOffset(conn *kafka.Conn, topic string, partition int32, t time.Time, low int64, high int64) (int64, error) {
	cacheKey := fmt.Sprintf("%s/%d/%d", topic, partition, t.Unix())

	if offset, ok := cache.Get(cacheKey); ok {
		return offset, nil
	}

	offset, err := conn.ReadOffset(t)
	if err != nil {
		return -1, err
	}

	if offset == -1 {
		cache.Set(cacheKey, high)
		return high, nil
	}
	if offset < low {
		cache.Set(cacheKey, low)
		return low, nil
	}
	if offset > high {
		cache.Set(cacheKey, high)
		return high, nil
	}

	cache.Set(cacheKey, offset)
	return offset, nil
}

func (client *KafkaClient) NewTimeRangeReader(ctx context.Context, topic string, partition int32, startTime, endTime time.Time) (*kafka.Reader, int64, error) {
	const timeout = 15 * time.Second
	logCtx := []interface{}{
		"topic", topic,
		"partition", partition,
		"startTime", startTime.Format(time.RFC3339),
		"endTime", endTime.Format(time.RFC3339),
	}
	log.DefaultLogger.Debug("Creating time range reader", logCtx...)

	if client.Dialer == nil {
		if err := client.NewConnection(); err != nil {
			log.DefaultLogger.Error("Failed to create connection", append(logCtx, "error", err)...)
			return nil, -1, fmt.Errorf("connection failed: %w", err)
		}
	}

	reader := client.newReader(topic, int(partition))
	log.DefaultLogger.Debug("Created reader instance", logCtx...)

	metaCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	broker := strings.Split(client.BootstrapServers, ",")[0]
	conn, err := client.Dialer.DialLeader(metaCtx, "tcp", broker, topic, int(partition))
	if err != nil {
		reader.Close()
		log.DefaultLogger.Error("Failed to dial leader", append(logCtx, "broker", broker, "error", err)...)
		return nil, -1, fmt.Errorf("dial leader failed: %w", err)
	}
	defer conn.Close()

	low, high, err := conn.ReadOffsets()
	if err != nil {
		reader.Close()
		log.DefaultLogger.Error("Failed to read offsets", append(logCtx, "error", err)...)
		return nil, -1, fmt.Errorf("read offsets failed: %w", err)
	}
	log.DefaultLogger.Debug("Partition offsets", append(logCtx, "low", low, "high", high)...)
	if high <= low {
		log.DefaultLogger.Debug("Empty partition, setting to low offset", append(logCtx, "offset", low)...)
		if err := reader.SetOffset(low); err != nil {
			reader.Close()
			return nil, -1, fmt.Errorf("set offset failed: %w", err)
		}
		return reader, high, nil
	}

	startOffset, err := client.calculateTimeOffset(conn, topic, partition, startTime, low, high)
	if err != nil {
		log.DefaultLogger.Warn("Start offset calculation failed, using low", append(logCtx, "error", err)...)
		startOffset = low
	}

	endOffset, err := client.calculateTimeOffset(conn, topic, partition, endTime, low, high)
	if err != nil {
		log.DefaultLogger.Warn("End offset calculation failed, using high", append(logCtx, "error", err)...)
		endOffset = high
	}

	if startOffset > endOffset {
		log.DefaultLogger.Debug("Start offset after end offset, using low",
			append(logCtx, "startOffset", startOffset, "endOffset", endOffset)...)
		startOffset = low
	}

	if err := reader.SetOffset(startOffset); err != nil {
		log.DefaultLogger.Warn("SetOffset failed, using low offset", append(logCtx, "error", err)...)
		if err := reader.SetOffset(low); err != nil {
			reader.Close()
			return nil, -1, fmt.Errorf("fallback offset failed: %w", err)
		}
		startOffset = low
	}

	log.DefaultLogger.Debug("Reader initialized",
		append(logCtx,
			"startOffset", startOffset,
			"endOffset", endOffset,
			"messageCount", endOffset-startOffset,
		)...)

	return reader, endOffset, nil
}

func (client *KafkaClient) BatchPull(ctx context.Context, reader *kafka.Reader, size int, endOffset int64) ([]KafkaMessage, error) {
	processed := make([]KafkaMessage, 0, size)
	for i := 0; i < size; i++ {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return nil, err
		}
		if endOffset != -1 && msg.Offset >= endOffset {
			break
		}
		processed = append(processed, decodeMessages(msg))
	}
	return processed, nil
}

func (client *KafkaClient) AsyncBatchPull(ctx context.Context, reader *kafka.Reader, size int, ch chan<- []KafkaMessage) {
	defer close(ch)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msgs, err := client.BatchPull(ctx, reader, size, -1)
			if err != nil {
				return
			}
			ch <- msgs
		}
	}
}

func decodeMessages(msg kafka.Message) KafkaMessage {
	return KafkaMessage{
		Timestamp: msg.Time,
		Value:     decodePayload(msg.Value),
		Offset:    msg.Offset,
	}
}

func decodePayload(value []byte) map[string]interface{} {
	var message map[string]interface{}
	if err := json.Unmarshal(value, &message); err != nil {
		return nil
	}
	return message
}

func (client *KafkaClient) ConsumerPull(ctx context.Context, reader *kafka.Reader) (KafkaMessage, error) {
	var message KafkaMessage

	log.DefaultLogger.Debug("Attempting to read message from Kafka",
		"topic", reader.Config().Topic,
		"partition", reader.Config().Partition)

	// Start timing the read operation
	readStart := time.Now()
	msg, err := reader.ReadMessage(ctx)
	readDuration := time.Since(readStart)

	if err != nil {
		if err == context.DeadlineExceeded {
			log.DefaultLogger.Debug("Timeout reading message from Kafka",
				"topic", reader.Config().Topic,
				"partition", reader.Config().Partition,
				"duration_ms", readDuration.Milliseconds())
		} else {
			log.DefaultLogger.Error("Error reading message from Kafka",
				"error", err,
				"topic", reader.Config().Topic,
				"partition", reader.Config().Partition,
				"duration_ms", readDuration.Milliseconds())
		}
		return message, fmt.Errorf("error reading message from Kafka: %w", err)
	}

	log.DefaultLogger.Debug("Successfully read message from Kafka",
		"topic", reader.Config().Topic,
		"partition", reader.Config().Partition,
		"offset", msg.Offset,
		"timestamp", msg.Time.Format(time.RFC3339),
		"duration_ms", readDuration.Milliseconds(),
		"message_size_bytes", len(msg.Value))

	// Start timing the unmarshal operation
	unmarshalStart := time.Now()
	if err := json.Unmarshal(msg.Value, &message.Value); err != nil {
		log.DefaultLogger.Error("Error unmarshalling message",
			"error", err,
			"topic", reader.Config().Topic,
			"partition", reader.Config().Partition,
			"offset", msg.Offset,
			"message_size_bytes", len(msg.Value),
			"message_preview", string(msg.Value[:min(len(msg.Value), 100)]))
		return message, fmt.Errorf("error unmarshalling message: %w", err)
	}
	unmarshalDuration := time.Since(unmarshalStart)

	message.Offset = msg.Offset
	message.Timestamp = msg.Time

	fieldCount := len(message.Value)
	log.DefaultLogger.Debug("Successfully unmarshalled message",
		"topic", reader.Config().Topic,
		"partition", reader.Config().Partition,
		"offset", msg.Offset,
		"timestamp", msg.Time.Format(time.RFC3339),
		"field_count", fieldCount,
		"unmarshal_duration_ms", unmarshalDuration.Milliseconds())

	return message, nil
}

// Helper function to get the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (client *KafkaClient) HealthCheck() error {
	if err := client.NewConnection(); err != nil {
		return fmt.Errorf("unable to initialize Kafka client: %w", err)
	}
	var conn *kafka.Conn
	var err error

	// Log connection attempt with non-sensitive info
	brokers := strings.Split(client.BootstrapServers, ",")
	brokerCount := len(brokers)
	log.DefaultLogger.Debug("Attempting health check connection", "brokerCount", brokerCount)

	// It is better to try several times due to possible network issues
	timeout := time.After(time.Duration(client.HealthcheckTimeout) * time.Millisecond)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("health check timed out after %d ms: %w", client.HealthcheckTimeout, err)
		case <-ticker.C:
			conn, err = client.Dialer.Dial(network, client.BootstrapServers)
			if err == nil {
				defer conn.Close()
				if _, err = conn.ReadPartitions(); err != nil {
					return fmt.Errorf("error reading partitions: %w", err)
				}
				return nil
			}
		}
	}
}

func (client *KafkaClient) Dispose() {
	if client.Reader != nil {
		client.Reader.Close()
	}
}

func getSASLMechanism(client *KafkaClient) (sasl.Mechanism, error) {
	switch client.SaslMechanisms {
	case "PLAIN":
		return plain.Mechanism{
			Username: client.SaslUsername,
			Password: client.SaslPassword,
		}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, client.SaslUsername, client.SaslPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, client.SaslUsername, client.SaslPassword)
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported mechanism SASL: %s", client.SaslMechanisms)
	}
}

func (client *KafkaClient) IsTopicExists(ctx context.Context, topicName string) (bool, error) {
	meta, err := client.Conn.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{topicName},
	})
	if err != nil {
		return false, fmt.Errorf("unable to get metadata: %w", err)
	}

	if len(meta.Topics) > 0 && meta.Topics[0].Error == nil {
		return true, nil
	}

	return false, nil
}

func getKafkaLogger(level string) (kafka.LoggerFunc, kafka.LoggerFunc) {
	noop := kafka.LoggerFunc(func(msg string, args ...interface{}) {})

	var logger = noop
	var errorLogger = noop

	switch strings.ToLower(level) {
	case debugLogLevel:
		logger = func(msg string, args ...interface{}) {
			// Convert variadic args to key-value pairs for Grafana logger
			kvs := make([]interface{}, 0, len(args))
			for i, arg := range args {
				// For odd indices, use as value with auto-generated key
				if i%2 == 0 {
					kvs = append(kvs, fmt.Sprintf("arg%d", i), arg)
				} else {
					// For even indices, use previous value as key if it's a string
					if key, ok := args[i-1].(string); ok {
						kvs[len(kvs)-2] = key // Replace auto-generated key
					}
					kvs[len(kvs)-1] = arg // Keep the value
				}
			}
			log.DefaultLogger.Debug("KAFKA: "+msg, kvs...)
		}
		errorLogger = func(msg string, args ...interface{}) {
			// Convert variadic args to key-value pairs for Grafana logger
			kvs := make([]interface{}, 0, len(args))
			for i, arg := range args {
				// For odd indices, use as value with auto-generated key
				if i%2 == 0 {
					kvs = append(kvs, fmt.Sprintf("arg%d", i), arg)
				} else {
					// For even indices, use previous value as key if it's a string
					if key, ok := args[i-1].(string); ok {
						kvs[len(kvs)-2] = key // Replace auto-generated key
					}
					kvs[len(kvs)-1] = arg // Keep the value
				}
			}
			log.DefaultLogger.Error("KAFKA: "+msg, kvs...)
		}
	case errorLogLevel:
		errorLogger = func(msg string, args ...interface{}) {
			// Convert variadic args to key-value pairs for Grafana logger
			kvs := make([]interface{}, 0, len(args))
			for i, arg := range args {
				// For odd indices, use as value with auto-generated key
				if i%2 == 0 {
					kvs = append(kvs, fmt.Sprintf("arg%d", i), arg)
				} else {
					// For even indices, use previous value as key if it's a string
					if key, ok := args[i-1].(string); ok {
						kvs[len(kvs)-2] = key // Replace auto-generated key
					}
					kvs[len(kvs)-1] = arg // Keep the value
				}
			}
			log.DefaultLogger.Error("KAFKA: "+msg, kvs...)
		}
	}

	return logger, errorLogger
}
