# Kafka Datasource Plugin

Visualize real-time streaming data from Apache Kafka directly in your Grafana dashboards. This plugin enables you to monitor live Kafka topics with automatic updates and time-series visualizations.

## Features

- Real-time monitoring of Kafka topics
- Historical data querying using dashboard time range
- Ability to toggle between streaming and non-streaming modes
- Configurable message limits for both streaming and queries
- Ability to query specific partitions
- Support for Grafana template variables in topic names and partition numbers
- Choose between "latest" or "last 100" messages
- Timestamp modes: "Now" for real-time or "Message Timestamp" for event time
- Simple JSON data format support
- Kafka authentication support (SASL)
- Encryption support (SSL/TLS)

## Version Compatibility

- Apache Kafka v0.9 or later
- Grafana v10.2 or later
- Network access from Grafana server to Kafka brokers

## Installation

For the installation process, please refer to the [plugin installation docs](https://grafana.com/docs/grafana/latest/administration/plugin-management/).

### Configuration

After installation, configure the plugin by adding a new Kafka data source in Grafana and filling out the following fields:

- **Bootstrap Servers**: Comma-separated list of Kafka bootstrap servers (e.g. `broker1:9092, broker2:9092`)
- **Security Protocol**: Choose the protocol (e.g. `PLAINTEXT`, `SASL_PLAINTEXT`)
- **SASL Mechanisms**: Specify SASL mechanism if required (e.g. `PLAIN`, `SCRAM-SHA-512`)
- **SASL Username/Password**: Provide credentials if SASL authentication is enabled
- **Log Level**: Set log verbosity (`debug`, `error`)
- **Healthcheck Timeout**: Timeout for health checks in milliseconds (default: 2000ms)
- **API Key**: (Deprecated) This field is deprecated and will be removed in future versions. Avoid using it for new configurations.

### Provisioning

You can automatically configure the Kafka datasource using Grafana's provisioning feature. Create a YAML file in your Grafana provisioning directory just like example in `provisioning/datasources/datasource.yaml`. This allows you to set up the Kafka data source without manual configuration through the Grafana UI.

## Build The Query

1. Create a new dashboard panel
2. Select your Kafka data source
3. Configure the query:
   - **Topic**: Your Kafka topic name (supports Grafana template variables like `$topic` or `${topic}`)
   - **Partition**: Partition number (usually 0, also supports template variables like `$partition`)
   - **Enable Streaming**: Toggle to enable/disable continuous streaming of data
   - **Max Messages**: Maximum number of messages to fetch (overrides data source setting)
   
   When streaming is enabled:
   - **Auto offset reset**: Choose "latest" for new data or "last 100" for recent history
   
   When streaming is disabled:
   - **Use Dashboard Time Range**: When enabled, fetches messages based on the dashboard time range
   - **Auto offset reset**: (Only shown when "Use Dashboard Time Range" is disabled) Choose "latest" for new data or "last 100" for recent history
   
   Always visible:
   - **Timestamp Mode**: Use "Now" for real-time or "Message Timestamp" for event time

   > **Using Template Variables**: You can use Grafana dashboard variables in both the Topic and Partition fields. This allows you to create dynamic dashboards where users can select different topics or partitions using dropdown menus. For example, if you have a dashboard variable called `topic`, you can use `$topic` in the Topic field.

## Streaming vs. Time Range Queries

The plugin supports two main modes of operation:

### Streaming Mode

When "Enable Streaming" is toggled on, the plugin continuously streams data from Kafka in real-time. This is ideal for:
- Live monitoring dashboards
- Observing real-time events as they occur
- Situations where you need continuous updates without refreshing

Streaming mode maintains an active connection to Kafka and updates the visualization as new messages arrive.

### Time Range Queries

When "Enable Streaming" is toggled off, you have two options:

1. **Use Dashboard Time Range**: When enabled, the plugin fetches messages based on the time range selected in the Grafana dashboard. This is ideal for:
   - Historical analysis of past events
   - Investigating specific time periods
   - Creating reports for specific time windows

2. **Auto Offset Reset**: When "Use Dashboard Time Range" is disabled, the plugin fetches a fixed set of messages based on the offset reset option. This is useful for quick checks without continuous streaming.

Time range queries respect the dashboard's time picker, allowing you to zoom in on specific time periods or analyze historical data.

## Performance Considerations

### Time Range Queries

When using time range queries, keep the following performance considerations in mind:

1. **Time Range Size**: Querying very large time ranges may take longer to process, especially for high-volume topics. Start with smaller time ranges and expand as needed.

2. **Message Limits**: The plugin respects the "Max Messages" setting even for time range queries. If your time range contains more messages than this limit, only the first N messages will be returned.

3. **Timeouts**: Time range queries have longer timeouts (10 seconds) than regular queries (2 seconds) to accommodate the additional processing required. If queries consistently time out, consider:
   - Reducing the time range
   - Increasing the log level to "debug" to get more diagnostic information
   - Using non-streaming mode with auto offset reset instead

4. **Empty Results**: If no messages are found in the specified time range, the plugin will automatically fall back to the earliest available messages. You'll see a notice in the panel explaining this behavior.

### Streaming Mode

For streaming mode, consider:

1. **Resource Usage**: Continuous streaming consumes more resources on both the Grafana server and Kafka broker. For high-volume topics, consider using non-streaming mode with periodic refresh.

2. **Message Limits**: The "Max Messages" setting controls how many messages are processed before the stream automatically stops. Set this appropriately based on your visualization needs.

## Usage Examples

### Example 1: Real-time Monitoring Dashboard

For a real-time monitoring dashboard that shows the latest events as they occur:

1. Enable streaming
2. Set auto offset reset to "Latest"
3. Set timestamp mode to "Now"
4. Set max messages to a reasonable limit (e.g., 1000)

This configuration will continuously stream the latest messages from Kafka, updating the visualization in real-time.

### Example 2: Historical Analysis

For analyzing events that occurred during a specific time period:

1. Disable streaming
2. Enable "Use Dashboard Time Range"
3. Set timestamp mode to "Message Timestamp"
4. Set the dashboard time picker to the desired time range

This configuration will fetch messages from the specified time range, allowing you to analyze historical data.

### Example 3: Quick Check of Recent Messages

For a quick check of the most recent messages without continuous streaming:

1. Disable streaming
2. Disable "Use Dashboard Time Range"
3. Set auto offset reset to "From the last 100"
4. Set timestamp mode to "Message Timestamp"

This configuration will fetch the 100 most recent messages without maintaining a continuous stream.

## Supported Data Format

All data formats are now supported which allows the data-source plugin to be used in a variety of panels:

```json
{
    "temperature": 23.5,
    "humidity": 65.2,
    "pressure": 1013.25,
    "metra": {
       "foo": "bar"
    }
}
```

Each numeric field becomes a separate series in your graph, allowing you to monitor multiple metrics from a single topic.

## Getting Help

- Check the [GitHub repository](https://github.com/hoptical/grafana-kafka-datasource) for documentation and examples
- Review sample producer code in different programming languages
- Report issues or request features via GitHub Issues
