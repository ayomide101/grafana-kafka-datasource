package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	if err := json.Unmarshal(query.JSON, &qm); err != nil {
		response.Error = err
		return response
	}

	if qm.Streaming {
		return createStreamingResponse(query)
	}

	// Establish Kafka connection
	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Kafka connection error", "error", err)
		response.Error = err
		return response
	}

	exists, err := d.client.IsTopicExists(ctx, qm.Topic)
	if err != nil || !exists {
		return handleTopicError(response, qm.Topic, exists, err)
	}

	queryTimeout := 2 * time.Second
	if qm.UseTimeRange {
		queryTimeout = 15 * time.Second
	}
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	reader, endOffset, err := d.createKafkaReader(queryCtx, qm, query)
	if err != nil {
		response.Error = err
		return response
	}
	defer reader.Close()

	maxMessages := calculateMaxMessages(qm, pCtx)

	messages, timeValues, allFields := d.processMessages(
		queryCtx, reader, qm, maxMessages, startTime, endOffset)
	if len(messages) == 0 {
		return handleEmptyResults(qm, query, reader, ctx)
	}

	frame := buildDataFrame(messages, timeValues, allFields, qm, query, startTime)
	response.Frames = append(response.Frames, frame)
	return response
}

func createStreamingResponse(query backend.DataQuery) backend.DataResponse {
	frame := data.NewFrame("response",
		data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
		data.NewField("values", nil, []int64{0, 0}),
	)
	return backend.DataResponse{Frames: []*data.Frame{frame}}
}

func handleTopicError(response backend.DataResponse, topic string, exists bool, err error) backend.DataResponse {
	if err != nil {
		log.DefaultLogger.Error("Topic check error", "error", err)
		response.Error = err
	} else if !exists {
		errMsg := fmt.Sprintf("Topic not found: %s", topic)
		log.DefaultLogger.Debug("Topic missing", "topic", topic)
		response.Error = errors.New(errMsg)
	}
	return response
}

func (d *KafkaDatasource) createKafkaReader(ctx context.Context, qm queryModel, query backend.DataQuery) (*kafka.Reader, int64, error) {
	if qm.UseTimeRange {
		log.DefaultLogger.Debug("Creating time-range reader",
			"topic", qm.Topic, "partition", qm.Partition,
			"start", query.TimeRange.From)

		return d.client.NewTimeRangeReader(ctx, qm.Topic, qm.Partition, query.TimeRange.From, query.TimeRange.To)
	}

	log.DefaultLogger.Debug("Creating stream reader",
		"topic", qm.Topic, "partition", qm.Partition,
		"offsetReset", qm.AutoOffsetReset)

	reader, err := d.client.NewStreamReader(ctx, qm.Topic, qm.Partition, qm.AutoOffsetReset)
	return reader, -1, err
}

func calculateMaxMessages(qm queryModel, pCtx backend.PluginContext) int {
	maxMessages := 50
	if qm.MaxMessages != nil && *qm.MaxMessages > 0 {
		maxMessages = *qm.MaxMessages
	} else if dsSettings := pCtx.DataSourceInstanceSettings; dsSettings != nil {
		var jsonData map[string]interface{}
		if err := json.Unmarshal(pCtx.DataSourceInstanceSettings.JSONData, &jsonData); err == nil {
			if dsMaxMessages, ok := jsonData["maxMessages"].(float64); ok && dsMaxMessages > 0 {
				maxMessages = int(dsMaxMessages)
			}
		}
	}
	return maxMessages
}

func (d *KafkaDatasource) processMessages(
	ctx context.Context,
	reader *kafka.Reader,
	qm queryModel,
	maxMessages int,
	startTime time.Time,
	endOffset int64,
) ([]map[string]interface{}, []time.Time, map[string]string) {
	messages := make([]map[string]interface{}, 0, maxMessages)
	timeValues := make([]time.Time, 0, maxMessages)
	allFields := make(map[string]string)
	messageCount := 0
	batchSize := 10
	if qm.UseTimeRange {
		batchSize = 50
	}

	for messageCount < maxMessages {
		msgs, err := d.client.BatchPull(ctx, reader, batchSize, endOffset)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				log.DefaultLogger.Debug("Batch pull timeout", "count", messageCount)
			} else {
				log.DefaultLogger.Error("Batch pull error", "error", err)
			}
			break
		}

		for _, msg := range msgs {
			frameTime := msg.Timestamp
			if qm.TimestampMode == "now" {
				frameTime = time.Now()
			}
			timeValues = append(timeValues, frameTime)

			messages = append(messages, msg.Value)
			for key, value := range msg.Value {
				if _, exists := allFields[key]; !exists {
					allFields[key] = inferFieldType(value)
				}
			}

			messageCount++
			if messageCount >= maxMessages {
				break
			}
		}

		if len(msgs) < batchSize {
			break
		}
	}

	log.DefaultLogger.Debug("Message processing complete",
		"count", messageCount,
		"duration", time.Since(startTime),
		"fields", len(allFields))

	return messages, timeValues, allFields
}

func inferFieldType(value interface{}) string {
	switch value.(type) {
	case float64:
		return "float64"
	case string:
		return "string"
	case bool:
		return "bool"
	case nil:
		return "null"
	default:
		return "string"
	}
}

func handleEmptyResults(qm queryModel, query backend.DataQuery, reader *kafka.Reader, ctx context.Context) backend.DataResponse {
	frame := data.NewFrame("response",
		data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
		data.NewField("values", nil, []int64{0, 0}),
	)

	frame.Meta = &data.FrameMeta{
		Notices: []data.Notice{{
			Severity: data.NoticeSeverityInfo,
			Text:     fmt.Sprintf("No messages found in topic '%s' partition %d", qm.Topic, qm.Partition),
		}},
	}

	if qm.UseTimeRange {
		frame.Meta.Notices = append(frame.Meta.Notices, data.Notice{
			Severity: data.NoticeSeverityInfo,
			Text: fmt.Sprintf("No messages in time range: %s to %s",
				query.TimeRange.From.Format(time.RFC3339),
				query.TimeRange.To.Format(time.RFC3339)),
		})
	}

	logDebugInfo(qm, query, reader, ctx)
	return backend.DataResponse{Frames: []*data.Frame{frame}}
}

func logDebugInfo(qm queryModel, query backend.DataQuery, reader *kafka.Reader, ctx context.Context) {
	stats := reader.Stats()
	log.DefaultLogger.Debug("Kafka reader stats",
		"topic", qm.Topic, "partition", qm.Partition,
		"offset", stats.Offset, "lag", stats.Lag,
		"minBytes", stats.MinBytes, "maxBytes", stats.MaxBytes)

	if qm.UseTimeRange {
		log.DefaultLogger.Info("Time-range query empty",
			"topic", qm.Topic, "partition", qm.Partition,
			"from", query.TimeRange.From, "to", query.TimeRange.To)
	}
}

func buildDataFrame(
	messages []map[string]interface{},
	timeValues []time.Time,
	allFields map[string]string,
	qm queryModel,
	query backend.DataQuery,
	startTime time.Time,
) *data.Frame {
	frame := data.NewFrame("response")
	frame.Fields = append(frame.Fields, data.NewField("time", nil, timeValues))

	// Initialize field containers
	fieldContainers := make(map[string]interface{})
	for field, ftype := range allFields {
		switch ftype {
		case "float64":
			fieldContainers[field] = make([]float64, len(messages))
		case "string":
			fieldContainers[field] = make([]string, len(messages))
		case "bool":
			fieldContainers[field] = make([]bool, len(messages))
		}
	}

	// Populate field data
	for i, msg := range messages {
		for field, container := range fieldContainers {
			value := msg[field]
			switch v := container.(type) {
			case []float64:
				if num, ok := value.(float64); ok {
					v[i] = num
				}
			case []string:
				if str, ok := value.(string); ok {
					v[i] = str
				} else if value == nil {
					v[i] = "null"
				} else {
					jsonBytes, _ := json.Marshal(value)
					v[i] = string(jsonBytes)
				}
			case []bool:
				if b, ok := value.(bool); ok {
					v[i] = b
				}
			}
		}
	}

	for field, container := range fieldContainers {
		switch v := container.(type) {
		case []float64:
			frame.Fields = append(frame.Fields, data.NewField(field, nil, v))
		case []string:
			frame.Fields = append(frame.Fields, data.NewField(field, nil, v))
		case []bool:
			frame.Fields = append(frame.Fields, data.NewField(field, nil, v))
		}
	}

	duration := time.Since(startTime)
	frame.Meta = &data.FrameMeta{
		Notices: []data.Notice{{
			Severity: data.NoticeSeverityInfo,
			Text: fmt.Sprintf("Processed %d messages in %d ms",
				len(messages), duration.Milliseconds()),
		}},
	}

	return frame
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
