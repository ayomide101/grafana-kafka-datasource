package kafka_client

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestNewKafkaClient_Defaults(t *testing.T) {
	options := Options{
		BootstrapServers:  "localhost:9092",
		ClientId:          "test-client",
		TLSAuthWithCACert: true,
		TLSAuth:           true,
		TLSSkipVerify:     true,
		ServerName:        "test-server",
		TLSCACert:         "test-ca-cert",
		TLSClientCert:     "test-client-cert",
		TLSClientKey:      "test-client-key",
		Timeout:           1234,
	}
	client := NewKafkaClient(options)
	if client.BootstrapServers != "localhost:9092" {
		t.Errorf("Expected BootstrapServers to be 'localhost:9092', got %s", client.BootstrapServers)
	}
	if client.ClientId != "test-client" {
		t.Errorf("Expected ClientId to be 'test-client', got %s", client.ClientId)
	}
	if !client.TLSAuthWithCACert {
		t.Error("Expected TLSAuthWithCACert to be true")
	}
	if !client.TLSAuth {
		t.Error("Expected TLSAuth to be true")
	}
	if !client.TLSSkipVerify {
		t.Error("Expected TLSSkipVerify to be true")
	}
	if client.ServerName != "test-server" {
		t.Errorf("Expected ServerName to be 'test-server', got %s", client.ServerName)
	}
	if client.TLSCACert != "test-ca-cert" {
		t.Errorf("Expected TLSCACert to be 'test-ca-cert', got %s", client.TLSCACert)
	}
	if client.TLSClientCert != "test-client-cert" {
		t.Errorf("Expected TLSClientCert to be 'test-client-cert', got %s", client.TLSClientCert)
	}
	if client.TLSClientKey != "test-client-key" {
		t.Errorf("Expected TLSClientKey to be 'test-client-key', got %s", client.TLSClientKey)
	}
	if client.Timeout != 1234 {
		t.Errorf("Expected Timeout to be 1234, got %d", client.Timeout)
	}
	if client.HealthcheckTimeout <= 0 {
		t.Error("Expected HealthcheckTimeout to be set to default if not provided")
	}
}

func TestKafkaClient_NewConnection_NoSASL(t *testing.T) {
	client := NewKafkaClient(Options{BootstrapServers: "localhost:9092"})
	err := client.NewConnection()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if client.Dialer == nil {
		t.Error("Expected Dialer to be initialized")
	}
	if client.Conn == nil {
		t.Error("Expected Conn to be initialized")
	}
}

func TestKafkaClient_Dispose(t *testing.T) {
	client := NewKafkaClient(Options{BootstrapServers: "localhost:9092"})
	client.Dispose() // Should not panic
}

func TestGetSASLMechanism_Unsupported(t *testing.T) {
	client := NewKafkaClient(Options{SaslMechanisms: "UNSUPPORTED"})
	_, err := getSASLMechanism(&client)
	if err == nil {
		t.Error("Expected error for unsupported SASL mechanism")
	}
}

func TestGetKafkaLogger(t *testing.T) {
	logger, errorLogger := getKafkaLogger("debug")
	logger("test debug")
	errorLogger("test error")
	logger, errorLogger = getKafkaLogger("error")
	logger("should not print")
	errorLogger("should print error")
}

// MockKafkaReader is a simplified mock for testing
type MockKafkaReader struct {
	Messages []kafka.Message
	Index    int
}

func (m *MockKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.Index >= len(m.Messages) {
		return kafka.Message{}, context.DeadlineExceeded
	}
	msg := m.Messages[m.Index]
	m.Index++
	return msg, nil
}

func (m *MockKafkaReader) Close() error {
	return nil
}

// testConsumerPull is a test-specific version of ConsumerPull that accepts our simplified mock
func testConsumerPull(ctx context.Context, reader *MockKafkaReader) (KafkaMessage, error) {
	var message KafkaMessage

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return message, err
	}

	if err := json.Unmarshal(msg.Value, &message.Value); err != nil {
		return message, err
	}

	message.Offset = msg.Offset
	message.Timestamp = msg.Time

	return message, nil
}

func TestConsumerPull_Float64Values(t *testing.T) {
	// Create a mock reader with a message containing float64 values
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{
				Value: []byte(`{"temp": 23.5, "humidity": 45.2}`),
				Time:  time.Now(),
			},
		},
	}

	msg, err := testConsumerPull(context.Background(), mockReader)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that the values were correctly parsed as float64
	if temp, ok := msg.Value["temp"].(float64); !ok || temp != 23.5 {
		t.Errorf("Expected temp to be float64 23.5, got %v (%T)", msg.Value["temp"], msg.Value["temp"])
	}
	if humidity, ok := msg.Value["humidity"].(float64); !ok || humidity != 45.2 {
		t.Errorf("Expected humidity to be float64 45.2, got %v (%T)", msg.Value["humidity"], msg.Value["humidity"])
	}
}

func TestConsumerPull_StringValues(t *testing.T) {
	// Create a mock reader with a message containing string values
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{
				Value: []byte(`{"name": "John", "city": "New York"}`),
				Time:  time.Now(),
			},
		},
	}

	msg, err := testConsumerPull(context.Background(), mockReader)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that the values were correctly parsed as strings
	if name, ok := msg.Value["name"].(string); !ok || name != "John" {
		t.Errorf("Expected name to be string 'John', got %v (%T)", msg.Value["name"], msg.Value["name"])
	}
	if city, ok := msg.Value["city"].(string); !ok || city != "New York" {
		t.Errorf("Expected city to be string 'New York', got %v (%T)", msg.Value["city"], msg.Value["city"])
	}
}

func TestConsumerPull_BooleanValues(t *testing.T) {
	// Create a mock reader with a message containing boolean values
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{
				Value: []byte(`{"active": true, "verified": false}`),
				Time:  time.Now(),
			},
		},
	}

	msg, err := testConsumerPull(context.Background(), mockReader)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that the values were correctly parsed as booleans
	if active, ok := msg.Value["active"].(bool); !ok || !active {
		t.Errorf("Expected active to be bool true, got %v (%T)", msg.Value["active"], msg.Value["active"])
	}
	if verified, ok := msg.Value["verified"].(bool); !ok || verified {
		t.Errorf("Expected verified to be bool false, got %v (%T)", msg.Value["verified"], msg.Value["verified"])
	}
}

func TestConsumerPull_NullValues(t *testing.T) {
	// Create a mock reader with a message containing null values
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{
				Value: []byte(`{"nullable": null}`),
				Time:  time.Now(),
			},
		},
	}

	msg, err := testConsumerPull(context.Background(), mockReader)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that the null value was correctly parsed
	if msg.Value["nullable"] != nil {
		t.Errorf("Expected nullable to be nil, got %v (%T)", msg.Value["nullable"], msg.Value["nullable"])
	}
}

func TestConsumerPull_ComplexValues(t *testing.T) {
	// Create a mock reader with a message containing complex values (objects and arrays)
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{
				Value: []byte(`{
					"object": {"key1": "value1", "key2": 42},
					"array": [1, 2, 3, 4]
				}`),
				Time: time.Now(),
			},
		},
	}

	msg, err := testConsumerPull(context.Background(), mockReader)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that the complex values were correctly parsed
	object, ok := msg.Value["object"].(map[string]interface{})
	if !ok {
		t.Errorf("Expected object to be map[string]interface{}, got %T", msg.Value["object"])
	} else {
		if value, ok := object["key1"].(string); !ok || value != "value1" {
			t.Errorf("Expected object.key1 to be string 'value1', got %v (%T)", object["key1"], object["key1"])
		}
		if value, ok := object["key2"].(float64); !ok || value != 42 {
			t.Errorf("Expected object.key2 to be float64 42, got %v (%T)", object["key2"], object["key2"])
		}
	}

	array, ok := msg.Value["array"].([]interface{})
	if !ok {
		t.Errorf("Expected array to be []interface{}, got %T", msg.Value["array"])
	} else {
		if len(array) != 4 {
			t.Errorf("Expected array to have 4 elements, got %d", len(array))
		}
		for i, expected := range []float64{1, 2, 3, 4} {
			if i < len(array) {
				if value, ok := array[i].(float64); !ok || value != expected {
					t.Errorf("Expected array[%d] to be float64 %f, got %v (%T)", i, expected, array[i], array[i])
				}
			}
		}
	}
}

func TestConsumerPull_MixedValues(t *testing.T) {
	// Create a mock reader with a message containing mixed value types
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{
				Value: []byte(`{
					"number": 42.5,
					"string": "hello",
					"boolean": true,
					"null": null,
					"object": {"nested": "value"},
					"array": [1, "two", false]
				}`),
				Time: time.Now(),
			},
		},
	}

	msg, err := testConsumerPull(context.Background(), mockReader)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Check that all value types were correctly parsed
	if number, ok := msg.Value["number"].(float64); !ok || number != 42.5 {
		t.Errorf("Expected number to be float64 42.5, got %v (%T)", msg.Value["number"], msg.Value["number"])
	}
	if str, ok := msg.Value["string"].(string); !ok || str != "hello" {
		t.Errorf("Expected string to be string 'hello', got %v (%T)", msg.Value["string"], msg.Value["string"])
	}
	if boolean, ok := msg.Value["boolean"].(bool); !ok || !boolean {
		t.Errorf("Expected boolean to be bool true, got %v (%T)", msg.Value["boolean"], msg.Value["boolean"])
	}
	if msg.Value["null"] != nil {
		t.Errorf("Expected null to be nil, got %v (%T)", msg.Value["null"], msg.Value["null"])
	}

	// Check complex types
	if _, ok := msg.Value["object"].(map[string]interface{}); !ok {
		t.Errorf("Expected object to be map[string]interface{}, got %T", msg.Value["object"])
	}
	if _, ok := msg.Value["array"].([]interface{}); !ok {
		t.Errorf("Expected array to be []interface{}, got %T", msg.Value["array"])
	}
}