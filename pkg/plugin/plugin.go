package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

var (
	_ backend.QueryDataHandler      = (*KafkaDatasource)(nil)
	_ backend.CheckHealthHandler    = (*KafkaDatasource)(nil)
	_ backend.StreamHandler         = (*KafkaDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*KafkaDatasource)(nil)
)

func NewKafkaInstance(_ context.Context, s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	settings, err := getDatasourceSettings(s)

	if err != nil {
		return nil, err
	}

	kafka_client := kafka_client.NewKafkaClient(*settings)

	return &KafkaDatasource{kafka_client}, nil
}

func getDatasourceSettings(s backend.DataSourceInstanceSettings) (*kafka_client.Options, error) {
	settings := &kafka_client.Options{}

	if err := json.Unmarshal(s.JSONData, settings); err != nil {
		return nil, err
	}

	// Handle secure JSON data
	if saslPassword, exists := s.DecryptedSecureJSONData["saslPassword"]; exists {
		settings.SaslPassword = saslPassword
	}

	// TLS certificate fields from secure JSON data
	if caCert, exists := s.DecryptedSecureJSONData["tlsCACert"]; exists {
		settings.TLSCACert = caCert
	}
	if clientCert, exists := s.DecryptedSecureJSONData["tlsClientCert"]; exists {
		settings.TLSClientCert = clientCert
	}
	if clientKey, exists := s.DecryptedSecureJSONData["tlsClientKey"]; exists {
		settings.TLSClientKey = clientKey
	}

	// Parse the JSONData to handle specific field types
	var jsonData map[string]interface{}
	if err := json.Unmarshal(s.JSONData, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON data: %w", err)
	}

	// Handle boolean fields that might come as different types from JSON
	if val, exists := jsonData["tlsSkipVerify"]; exists {
		if b, ok := val.(bool); ok {
			settings.TLSSkipVerify = b
		} else if str, ok := val.(string); ok && str == "true" {
			settings.TLSSkipVerify = true
		}
	}

	if val, exists := jsonData["tlsAuthWithCACert"]; exists {
		if b, ok := val.(bool); ok {
			settings.TLSAuthWithCACert = b
		} else if str, ok := val.(string); ok && str == "true" {
			settings.TLSAuthWithCACert = true
		}
	}

	if val, exists := jsonData["tlsAuth"]; exists {
		if b, ok := val.(bool); ok {
			settings.TLSAuth = b
		} else if str, ok := val.(string); ok && str == "true" {
			settings.TLSAuth = true
		}
	}

	return settings, nil
}

type KafkaDatasource struct {
	client kafka_client.KafkaClient
}

func (d *KafkaDatasource) Dispose() {
	// Clean up datasource instance resources.
}

func (d *KafkaDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Debug("QueryData called", "request", req)

	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	Topic           string `json:"topicName"`
	Partition       int32  `json:"partition"`
	AutoOffsetReset string `json:"autoOffsetReset"`
	TimestampMode   string `json:"timestampMode"`
	Streaming       bool   `json:"streaming"`
	MaxMessages     *int   `json:"maxMessages,omitempty"`
}

func (d *KafkaDatasource) query(ctx context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	startTime := time.Now()

	response := backend.DataResponse{}
	var qm queryModel
	response.Error = json.Unmarshal(query.JSON, &qm)

	if response.Error != nil {
		return response
	}

	// If streaming is enabled, return a minimal response as streaming will be handled separately
	if qm.Streaming {
		frame := data.NewFrame("response")
		frame.Fields = append(frame.Fields,
			data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
			data.NewField("values", nil, []int64{0, 0}),
		)
		response.Frames = append(response.Frames, frame)
		return response
	}

	// For non-streaming mode, fetch data from Kafka
	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Creating new Kafka connection error", "error", err)
		response.Error = err
		return response
	}

	exists, err := d.client.IsTopicExists(ctx, qm.Topic)
	if err != nil {
		log.DefaultLogger.Error("Checking kafka topic error", "error", err)
		response.Error = err
		return response
	}

	if !exists {
		log.DefaultLogger.Debug("Topic not found", "topic", qm.Topic)
		response.Error = fmt.Errorf("topic not found: %s", qm.Topic)
		return response
	}

	reader, err := d.client.NewStreamReader(ctx, qm.Topic, qm.Partition, qm.AutoOffsetReset)
	if err != nil {
		log.DefaultLogger.Error("Failed to create stream reader", "error", err)
		response.Error = err
		return response
	}
	defer reader.Close()

	frame := data.NewFrame("response")

	// Get the max messages setting from the data source or query
	maxMessages := 50 // Default value

	// Check if the query has a max messages setting
	if qm.MaxMessages != nil && *qm.MaxMessages > 0 {
		maxMessages = *qm.MaxMessages
	} else {
		// Try to get the data source setting
		var jsonData map[string]interface{}
		if err := json.Unmarshal(pCtx.DataSourceInstanceSettings.JSONData, &jsonData); err == nil {
			if dsMaxMessages, ok := jsonData["maxMessages"].(float64); ok && dsMaxMessages > 0 {
				maxMessages = int(dsMaxMessages)
			}
		}
	}
	// Minimum number of messages to collect before returning
	const minMessages = 5
	// Number of messages that's enough for visualization
	const targetMessages = 20

	timeValues := make([]time.Time, 0, maxMessages)

	// Maps to track all field keys and their types
	allFields := make(map[string]string) // key -> type
	messages := make([]map[string]interface{}, 0, maxMessages)

	queryTimeout := time.After(2 * time.Second)
	messageCount := 0

	// First pass: collect all messages and identify all fields and their types
	for messageCount < maxMessages {
		select {
		case <-queryTimeout:
			log.DefaultLogger.Debug("Query timeout reached", "messageCount", messageCount)
			break
		default:
			// Reduce per-message timeout to 100ms
			msgCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			msg, err := d.client.ConsumerPull(msgCtx, reader)
			cancel()

			if err != nil {
				log.DefaultLogger.Debug("Error or timeout fetching message", "error", err, "messageCount", messageCount)
				break
			}

			var frameTime time.Time
			if qm.TimestampMode == "now" {
				frameTime = time.Now()
			} else {
				frameTime = msg.Timestamp
			}
			timeValues = append(timeValues, frameTime)

			// Store the message for second pass
			messages = append(messages, msg.Value)

			// Record all field keys and their types
			for key, value := range msg.Value {
				// Only record the type if we haven't seen this field before
				// or if we've seen it but it was null
				if existingType, exists := allFields[key]; !exists || existingType == "" {
					switch value.(type) {
					case float64:
						allFields[key] = "float64"
					case string:
						allFields[key] = "string"
					case bool:
						allFields[key] = "bool"
					case nil:
						// For null values, we'll use string type but mark it as empty
						// so it can be overridden if we find a non-null value later
						if !exists {
							allFields[key] = ""
						}
					default:
						// For complex types, store as JSON string
						allFields[key] = "string"
					}
				}
			}

			messageCount++

			// Early return if we have enough messages for visualization
			// but ensure we have at least minMessages
			if messageCount >= targetMessages && messageCount >= minMessages {
				log.DefaultLogger.Debug("Collected enough messages for visualization", "messageCount", messageCount)
				break
			}
		}
	}

	// If no messages were collected, return early
	if len(messages) == 0 {
		frame.Fields = append(frame.Fields,
			data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
			data.NewField("values", nil, []int64{0, 0}),
		)
		response.Frames = append(response.Frames, frame)

		// Log query execution time
		queryDuration := time.Since(startTime)
		log.DefaultLogger.Debug("Query execution completed with no messages", "duration_ms", queryDuration.Milliseconds())

		return response
	}

	// Log the number of messages and fields for debugging
	log.DefaultLogger.Debug("Collected messages",
		"count", len(messages),
		"fieldCount", len(allFields),
		"timeValues", len(timeValues))

	// Initialize field arrays with the correct length
	fieldValues := make(map[string]interface{})
	for key, fieldType := range allFields {
		// Default to string type for null fields
		if fieldType == "" {
			fieldType = "string"
			allFields[key] = "string"
		}

		switch fieldType {
		case "float64":
			fieldValues[key] = make([]float64, len(messages))
		case "string":
			fieldValues[key] = make([]string, len(messages))
		case "bool":
			fieldValues[key] = make([]bool, len(messages))
		}
	}

	// Second pass: populate field arrays with values from messages
	for i, msg := range messages {
		for key, fieldType := range allFields {
			value, exists := msg[key]

			if !exists {
				// Field doesn't exist in this message, use default value
				continue // The arrays are already initialized with zero values
			}

			switch fieldType {
			case "float64":
				if v, ok := value.(float64); ok {
					fieldValues[key].([]float64)[i] = v
				}
			case "string":
				if v, ok := value.(string); ok {
					fieldValues[key].([]string)[i] = v
				} else if value == nil {
					fieldValues[key].([]string)[i] = "null"
				} else {
					// Convert complex types to JSON string
					jsonBytes, err := json.Marshal(value)
					if err != nil {
						log.DefaultLogger.Error("Error marshalling complex value", "error", err)
						continue
					}
					fieldValues[key].([]string)[i] = string(jsonBytes)
				}
			case "bool":
				if v, ok := value.(bool); ok {
					fieldValues[key].([]bool)[i] = v
				}
			}
		}
	}

	frame.Fields = append(frame.Fields, data.NewField("time", nil, timeValues))
	for key, values := range fieldValues {
		switch allFields[key] {
		case "float64":
			frame.Fields = append(frame.Fields, data.NewField(key, nil, values.([]float64)))
		case "string":
			frame.Fields = append(frame.Fields, data.NewField(key, nil, values.([]string)))
		case "bool":
			frame.Fields = append(frame.Fields, data.NewField(key, nil, values.([]bool)))
		}
	}

	// Log query execution time and message processing rate
	queryDuration := time.Since(startTime)
	messagesPerSecond := float64(messageCount) / queryDuration.Seconds()

	log.DefaultLogger.Debug("Query execution completed",
		"duration_ms", queryDuration.Milliseconds(),
		"messageCount", messageCount,
		"messagesPerSecond", messagesPerSecond,
		"fieldCount", len(allFields))

	response.Frames = append(response.Frames, frame)
	return response
}

func (d *KafkaDatasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Debug("CheckHealth called",
		"datasourceID", req.PluginContext.DataSourceInstanceSettings.ID)

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	err := d.client.HealthCheck()
	if err != nil {
		status = backend.HealthStatusError
		message = err.Error()
		log.DefaultLogger.Error("Plugin health check failed.", "error", err)
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (d *KafkaDatasource) SubscribeStream(ctx context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	log.DefaultLogger.Debug("SubscribeStream called", "path", req.Path)

	var qm queryModel
	err := json.Unmarshal(req.Data, &qm)
	if err != nil {
		log.DefaultLogger.Error("SubscribeStream unmarshal error", "error", err)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, err
	}

	if qm.Topic == "" {
		err := fmt.Errorf("empty topic in stream path: %q", req.Path)
		log.DefaultLogger.Error("SubscribeStream topic error", "error", err)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, err
	}

	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Creating new Kafka connection error", "error", err)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, err
	}

	exists, err := d.client.IsTopicExists(ctx, qm.Topic)
	if err != nil {
		log.DefaultLogger.Error("Checking kafka topic error", "error", err)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, err
	}

	if !exists {
		log.DefaultLogger.Debug("Topic not found", "topic", qm.Topic)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusNotFound,
		}, nil
	}

	log.DefaultLogger.Debug("SubscribeStream success", "topic", qm.Topic, "partition", qm.Partition)
	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

func (d *KafkaDatasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	log.DefaultLogger.Debug("RunStream called", "path", req.Path)

	var qm queryModel
	err := json.Unmarshal(req.Data, &qm)
	if err != nil {
		log.DefaultLogger.Error("RunStream unmarshal error", "error", err)
	}

	// Create stream-specific reader
	reader, err := d.client.NewStreamReader(ctx, qm.Topic, qm.Partition, qm.AutoOffsetReset)
	if err != nil {
		return fmt.Errorf("failed to create stream reader: %w", err)
	}
	defer reader.Close()

	// Performance tracking variables
	startTime := time.Now()
	messageCount := 0
	lastLogTime := startTime
	const logInterval = 30 * time.Second // Log stats every 30 seconds
	const logMessageFrequency = 100      // Log only every 100 messages

	// Get the max messages setting from the data source or query
	maxMessages := 50 // Default value

	// Check if the query has a max messages setting
	if qm.MaxMessages != nil && *qm.MaxMessages > 0 {
		maxMessages = *qm.MaxMessages
	} else {
		// Try to get the data source setting
		var jsonData map[string]interface{}
		if err := json.Unmarshal(req.PluginContext.DataSourceInstanceSettings.JSONData, &jsonData); err == nil {
			if dsMaxMessages, ok := jsonData["maxMessages"].(float64); ok && dsMaxMessages > 0 {
				maxMessages = int(dsMaxMessages)
			}
		}
	}

	log.DefaultLogger.Debug("Stream configured",
		"topic", qm.Topic,
		"maxMessages", maxMessages)

	// Error handling variables
	const maxConsecutiveErrors = 5
	const initialBackoff = 100 * time.Millisecond
	const maxBackoff = 2 * time.Second
	consecutiveErrors := 0
	currentBackoff := initialBackoff

	for {
		select {
		case <-ctx.Done():
			// Calculate final stats
			duration := time.Since(startTime)
			messagesPerSecond := float64(messageCount) / duration.Seconds()

			log.DefaultLogger.Debug("Stream finished",
				"path", req.Path,
				"topic", qm.Topic,
				"duration_s", duration.Seconds(),
				"messageCount", messageCount,
				"messagesPerSecond", messagesPerSecond)
			return nil

		default:
			// Use timeout context for message pulls to avoid blocking
			msgCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			msg, err := d.client.ConsumerPull(msgCtx, reader)
			cancel()

			if err != nil {
				// Handle errors with backoff/retry
				consecutiveErrors++

				// Only log every few errors to avoid flooding logs
				if consecutiveErrors == 1 || consecutiveErrors%5 == 0 {
					log.DefaultLogger.Debug("Error pulling message",
						"error", err,
						"consecutiveErrors", consecutiveErrors,
						"backoff_ms", currentBackoff.Milliseconds())
				}

				// If too many consecutive errors, return with error
				if consecutiveErrors > maxConsecutiveErrors {
					return fmt.Errorf("too many consecutive errors (%d): %w", consecutiveErrors, err)
				}

				// Apply backoff with exponential increase (capped)
				time.Sleep(currentBackoff)
				if currentBackoff < maxBackoff {
					currentBackoff *= 2
				}

				continue
			}

			// Reset error counters on success
			consecutiveErrors = 0
			currentBackoff = initialBackoff

			// Increment message counter
			messageCount++

			// Check if we've reached the max messages limit
			if messageCount >= maxMessages {
				duration := time.Since(startTime)
				messagesPerSecond := float64(messageCount) / duration.Seconds()

				log.DefaultLogger.Debug("Stream reached max messages limit",
					"path", req.Path,
					"topic", qm.Topic,
					"maxMessages", maxMessages,
					"duration_s", duration.Seconds(),
					"messagesPerSecond", messagesPerSecond)
				return nil
			}

			// Create frame for this message
			frame := data.NewFrame("response")
			frame.Fields = append(frame.Fields,
				data.NewField("time", nil, make([]time.Time, 1)),
			)

			// Determine timestamp based on mode
			var frameTime time.Time
			if qm.TimestampMode == "now" {
				frameTime = time.Now()
			} else {
				frameTime = msg.Timestamp
			}

			// Only log every Nth message to reduce log volume
			if messageCount == 1 || messageCount%logMessageFrequency == 0 {
				log.DefaultLogger.Debug("Message received",
					"topic", qm.Topic,
					"partition", qm.Partition,
					"offset", msg.Offset,
					"messageCount", messageCount,
					"fieldCount", len(msg.Value))
			}

			// Log periodic performance stats
			now := time.Now()
			if now.Sub(lastLogTime) >= logInterval {
				duration := now.Sub(startTime)
				messagesPerSecond := float64(messageCount) / duration.Seconds()

				log.DefaultLogger.Debug("Stream performance stats",
					"topic", qm.Topic,
					"duration_s", duration.Seconds(),
					"messageCount", messageCount,
					"messagesPerSecond", messagesPerSecond)

				lastLogTime = now
			}

			frame.Fields[0].Set(0, frameTime)

			cnt := 1
			for key, value := range msg.Value {
				switch v := value.(type) {
				case float64:
					frame.Fields = append(frame.Fields,
						data.NewField(key, nil, make([]float64, 1)))
					frame.Fields[cnt].Set(0, v)
				case string:
					frame.Fields = append(frame.Fields,
						data.NewField(key, nil, make([]string, 1)))
					frame.Fields[cnt].Set(0, v)
				case bool:
					frame.Fields = append(frame.Fields,
						data.NewField(key, nil, make([]bool, 1)))
					frame.Fields[cnt].Set(0, v)
				case nil:
					frame.Fields = append(frame.Fields,
						data.NewField(key, nil, make([]string, 1)))
					frame.Fields[cnt].Set(0, "null")
				default:
					jsonBytes, err := json.Marshal(v)
					if err != nil {
						log.DefaultLogger.Error("Error marshalling complex value", "error", err)
						continue
					}
					frame.Fields = append(frame.Fields,
						data.NewField(key, nil, make([]string, 1)))
					frame.Fields[cnt].Set(0, string(jsonBytes))
				}
				cnt++
			}

			err = sender.SendFrame(frame, data.IncludeAll)
			if err != nil {
				log.DefaultLogger.Error("Error sending frame", "error", err)
				// Continue processing even if sending fails
				continue
			}
		}
	}
}

func (d *KafkaDatasource) PublishStream(_ context.Context, req *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	log.DefaultLogger.Debug("PublishStream called",
		"path", req.Path)

	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}
