package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"github.com/segmentio/kafka-go"
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
	UseTimeRange    bool   `json:"useTimeRange"`
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

	var reader *kafka.Reader

	// Use time range or auto offset reset based on the query settings
	if qm.UseTimeRange {
		timeRangeFrom := query.TimeRange.From
		timeRangeTo := query.TimeRange.To
		timeRangeDuration := timeRangeTo.Sub(timeRangeFrom)

		log.DefaultLogger.Debug("Using time range for query",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"from", timeRangeFrom.Format(time.RFC3339),
			"to", timeRangeTo.Format(time.RFC3339),
			"duration", timeRangeDuration.String(),
			"duration_seconds", timeRangeDuration.Seconds())

		// Log the time range in a more human-readable format
		now := time.Now()
		log.DefaultLogger.Debug("Time range relative to now",
			"from_relative", now.Sub(timeRangeFrom).String()+" ago",
			"to_relative", now.Sub(timeRangeTo).String()+" ago",
			"now", now.Format(time.RFC3339))

		log.DefaultLogger.Debug("Creating time range reader",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"startTime", timeRangeFrom.Format(time.RFC3339))

		readerCreationStart := time.Now()
		reader, err = d.client.NewTimeRangeReader(ctx, qm.Topic, qm.Partition, timeRangeFrom)
		readerCreationDuration := time.Since(readerCreationStart)

		if err != nil {
			log.DefaultLogger.Error("Failed to create time range reader",
				"error", err,
				"topic", qm.Topic,
				"partition", qm.Partition,
				"startTime", timeRangeFrom.Format(time.RFC3339),
				"duration_ms", readerCreationDuration.Milliseconds())
			response.Error = err
			return response
		}

		log.DefaultLogger.Debug("Successfully created time range reader",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"startTime", timeRangeFrom.Format(time.RFC3339),
			"duration_ms", readerCreationDuration.Milliseconds())
	} else {
		log.DefaultLogger.Debug("Using auto offset reset for query",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"autoOffsetReset", qm.AutoOffsetReset)

		readerCreationStart := time.Now()
		reader, err = d.client.NewStreamReader(ctx, qm.Topic, qm.Partition, qm.AutoOffsetReset)
		readerCreationDuration := time.Since(readerCreationStart)

		if err != nil {
			log.DefaultLogger.Error("Failed to create stream reader",
				"error", err,
				"topic", qm.Topic,
				"partition", qm.Partition,
				"autoOffsetReset", qm.AutoOffsetReset,
				"duration_ms", readerCreationDuration.Milliseconds())
			response.Error = err
			return response
		}

		log.DefaultLogger.Debug("Successfully created stream reader",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"autoOffsetReset", qm.AutoOffsetReset,
			"duration_ms", readerCreationDuration.Milliseconds())
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

	// Use a longer timeout for time-based queries as they might take longer
	queryTimeoutDuration := 2 * time.Second
	if qm.UseTimeRange {
		queryTimeoutDuration = 10 * time.Second
		log.DefaultLogger.Debug("Using extended timeout for time-based query",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"from", query.TimeRange.From,
			"to", query.TimeRange.To,
			"timeout_seconds", queryTimeoutDuration.Seconds())
	}
	queryTimeout := time.After(queryTimeoutDuration)
	messageCount := 0

	// Log the query parameters for debugging
	log.DefaultLogger.Debug("Starting query execution",
		"topic", qm.Topic,
		"partition", qm.Partition,
		"maxMessages", maxMessages,
		"useTimeRange", qm.UseTimeRange,
		"timeRangeFrom", query.TimeRange.From,
		"timeRangeTo", query.TimeRange.To)

	// First pass: collect all messages and identify all fields and their types
	log.DefaultLogger.Debug("Starting message collection loop",
		"maxMessages", maxMessages,
		"targetMessages", targetMessages,
		"minMessages", minMessages,
		"useTimeRange", qm.UseTimeRange)

	loopStartTime := time.Now()
	messageProcessingTimes := make([]time.Duration, 0, maxMessages)

	for messageCount < maxMessages {
		select {
		case <-queryTimeout:
			log.DefaultLogger.Debug("Query timeout reached",
				"messageCount", messageCount,
				"elapsedTime", time.Since(loopStartTime).String())
			break
		default:
			// Set per-message timeout - longer for time-based queries
			msgTimeoutDuration := 100 * time.Millisecond
			if qm.UseTimeRange {
				msgTimeoutDuration = 500 * time.Millisecond
			}

			msgStartTime := time.Now()
			log.DefaultLogger.Debug("Attempting to pull message",
				"messageCount", messageCount,
				"timeout", msgTimeoutDuration.String())

			msgCtx, cancel := context.WithTimeout(ctx, msgTimeoutDuration)
			msg, err := d.client.ConsumerPull(msgCtx, reader)
			msgDuration := time.Since(msgStartTime)
			messageProcessingTimes = append(messageProcessingTimes, msgDuration)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					log.DefaultLogger.Debug("Timeout fetching message",
						"messageCount", messageCount,
						"duration_ms", msgDuration.Milliseconds(),
						"timeout_ms", msgTimeoutDuration.Milliseconds())
				} else {
					log.DefaultLogger.Error("Error fetching message",
						"error", err,
						"messageCount", messageCount,
						"duration_ms", msgDuration.Milliseconds())
				}
				break
			}

			log.DefaultLogger.Debug("Successfully pulled message",
				"messageCount", messageCount,
				"offset", msg.Offset,
				"timestamp", msg.Timestamp.Format(time.RFC3339),
				"duration_ms", msgDuration.Milliseconds(),
				"fieldCount", len(msg.Value))

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
			newFieldsFound := 0
			for key, value := range msg.Value {
				// Only record the type if we haven't seen this field before
				// or if we've seen it but it was null
				if existingType, exists := allFields[key]; !exists || existingType == "" {
					newFieldsFound++
					valueType := "unknown"

					switch value.(type) {
					case float64:
						allFields[key] = "float64"
						valueType = "float64"
					case string:
						allFields[key] = "string"
						valueType = "string"
					case bool:
						allFields[key] = "bool"
						valueType = "bool"
					case nil:
						// For null values, we'll use string type but mark it as empty
						// so it can be overridden if we find a non-null value later
						if !exists {
							allFields[key] = ""
							valueType = "null"
						}
					default:
						// For complex types, store as JSON string
						allFields[key] = "string"
						valueType = fmt.Sprintf("complex (%T)", value)
					}

					if newFieldsFound <= 5 { // Limit logging to first 5 new fields to avoid log spam
						log.DefaultLogger.Debug("New field detected",
							"messageCount", messageCount,
							"field", key,
							"type", valueType)
					}
				}
			}

			if newFieldsFound > 0 {
				log.DefaultLogger.Debug("Field detection summary",
					"messageCount", messageCount,
					"newFieldsFound", newFieldsFound,
					"totalFieldsCount", len(allFields))
			}

			messageCount++

			// Early return if we have enough messages for visualization
			// but ensure we have at least minMessages
			if messageCount >= targetMessages && messageCount >= minMessages {
				log.DefaultLogger.Debug("Collected enough messages for visualization",
					"messageCount", messageCount,
					"targetMessages", targetMessages,
					"minMessages", minMessages,
					"elapsedTime", time.Since(loopStartTime).String())
				break
			}
		}
	}

	// Calculate message processing statistics
	loopDuration := time.Since(loopStartTime)
	var avgProcessingTime time.Duration
	if len(messageProcessingTimes) > 0 {
		var totalTime time.Duration
		for _, t := range messageProcessingTimes {
			totalTime += t
		}
		avgProcessingTime = totalTime / time.Duration(len(messageProcessingTimes))
	}

	log.DefaultLogger.Debug("Message collection loop completed",
		"messageCount", messageCount,
		"totalDuration", loopDuration.String(),
		"avgMessageProcessingTime", avgProcessingTime.String(),
		"messagesPerSecond", float64(messageCount)/loopDuration.Seconds())

	// If no messages were collected, return early with a more informative response
	if len(messages) == 0 {
		log.DefaultLogger.Info("No messages found in query result",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"useTimeRange", qm.UseTimeRange)

		// Try to get reader stats for debugging
		stats := reader.Stats()
		log.DefaultLogger.Debug("Kafka reader stats for empty result",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"offset", stats.Offset,
			"lag", stats.Lag,
			"minBytes", stats.MinBytes,
			"maxBytes", stats.MaxBytes,
			"maxWait", stats.MaxWait.String(),
			"dials", stats.Dials,
			"fetches", stats.Fetches,
			"messages", stats.Messages,
			"errors", stats.Errors)

		// Try to get partition information directly
		// Create a context with timeout for getting partition info
		partInfoCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		// Try to get the first broker to check partition info
		firstBroker := strings.Split(d.client.BootstrapServers, ",")[0]

		// Check if Dialer is initialized
		if d.client.Dialer == nil {
			log.DefaultLogger.Error("Cannot get partition info, dialer is nil",
				"topic", qm.Topic,
				"partition", qm.Partition)
		} else {
			conn, dialErr := d.client.Dialer.DialLeader(partInfoCtx, "tcp", firstBroker, qm.Topic, int(qm.Partition))

			if dialErr != nil {
				log.DefaultLogger.Error("Failed to dial leader for partition info",
					"error", dialErr,
					"broker", firstBroker,
					"topic", qm.Topic,
					"partition", qm.Partition)
			} else {
				defer conn.Close()

				// Try to get partition offsets
				low, high, readErr := conn.ReadOffsets()
				if readErr != nil {
					log.DefaultLogger.Error("Failed to read offsets for partition info",
						"error", readErr,
						"topic", qm.Topic,
						"partition", qm.Partition)
				} else {
					messageCount := high - low
					log.DefaultLogger.Info("Partition information for empty result",
						"topic", qm.Topic,
						"partition", qm.Partition,
						"lowWatermark", low,
						"highWatermark", high,
						"messageCount", messageCount)

					if qm.UseTimeRange {
						log.DefaultLogger.Info("Time range query returned no results",
							"topic", qm.Topic,
							"partition", qm.Partition,
							"from", query.TimeRange.From.Format(time.RFC3339),
							"to", query.TimeRange.To.Format(time.RFC3339),
							"lowWatermark", low,
							"highWatermark", high,
							"messageCount", messageCount)

						if messageCount == 0 {
							log.DefaultLogger.Info("Partition is empty, no messages to return")
						} else {
							log.DefaultLogger.Info("Partition has messages, but none match the time range",
								"timeRangeFrom", query.TimeRange.From.Format(time.RFC3339),
								"timeRangeTo", query.TimeRange.To.Format(time.RFC3339))
						}
					}
				}
			}
		}

		frame.Fields = append(frame.Fields,
			data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
			data.NewField("values", nil, []int64{0, 0}),
		)

		frame.Meta = &data.FrameMeta{
			Notices: []data.Notice{
				{
					Severity: data.NoticeSeverityInfo,
					Text:     fmt.Sprintf("No messages found in topic '%s' partition %d", qm.Topic, qm.Partition),
				},
			},
		}

		if qm.UseTimeRange {
			frame.Meta.Notices = append(frame.Meta.Notices, data.Notice{
				Severity: data.NoticeSeverityInfo,
				Text: fmt.Sprintf("No messages found in the specified time range: %s to %s",
					query.TimeRange.From.Format(time.RFC3339),
					query.TimeRange.To.Format(time.RFC3339)),
			})
		}

		response.Frames = append(response.Frames, frame)

		queryDuration := time.Since(startTime)
		log.DefaultLogger.Debug("Query execution completed with no messages",
			"topic", qm.Topic,
			"partition", qm.Partition,
			"useTimeRange", qm.UseTimeRange,
			"timeRangeFrom", query.TimeRange.From.Format(time.RFC3339),
			"timeRangeTo", query.TimeRange.To.Format(time.RFC3339),
			"duration_ms", queryDuration.Milliseconds())

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

	perfMetrics := map[string]interface{}{
		"duration_ms":       queryDuration.Milliseconds(),
		"messageCount":      messageCount,
		"messagesPerSecond": messagesPerSecond,
		"fieldCount":        len(allFields),
		"topic":             qm.Topic,
		"partition":         qm.Partition,
		"useTimeRange":      qm.UseTimeRange,
	}

	if qm.UseTimeRange {
		perfMetrics["timeRangeFrom"] = query.TimeRange.From
		perfMetrics["timeRangeTo"] = query.TimeRange.To
		perfMetrics["timeRangeDuration"] = query.TimeRange.To.Sub(query.TimeRange.From).String()
	}

	// Add metadata to the frame with performance information
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}

	frame.Meta.Notices = append(frame.Meta.Notices, data.Notice{
		Severity: data.NoticeSeverityInfo,
		Text: fmt.Sprintf("Query completed in %d ms, processed %d messages (%0.2f msgs/sec)",
			queryDuration.Milliseconds(), messageCount, messagesPerSecond),
	})

	// Log detailed performance metrics
	log.DefaultLogger.Debug("Query execution completed", perfMetrics)

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
