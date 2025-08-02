import { defaults } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { Checkbox, Combobox, InlineField, InlineFieldRow, Input, Switch } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { AutoOffsetReset, defaultQuery, KafkaDataSourceOptions, KafkaQuery, TimestampMode } from './types';

const autoResetOffsets: Array<{ label: string; value: AutoOffsetReset }> = [
  {
    label: 'From the last 100',
    value: AutoOffsetReset.EARLIEST,
  },
  {
    label: 'Latest',
    value: AutoOffsetReset.LATEST,
  },
];

const timestampModes: Array<{ label: string; value: TimestampMode }> = [
  {
    label: 'Now',
    value: TimestampMode.Now,
  },
  {
    label: 'Message Timestamp',
    value: TimestampMode.Message,
  },
];

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

export class QueryEditor extends PureComponent<Props> {
  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: event.target.value });
    onRunQuery();
  };

  onPartitionChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = parseInt(event.target.value, 10);
    // Ensure partition is a valid non-negative integer
    const partition = isNaN(value) || value < 0 ? 0 : value;
    onChange({ ...query, partition });
    onRunQuery();
  };

  onAutoResetOffsetChanged = (value: AutoOffsetReset) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, autoOffsetReset: value });
    onRunQuery();
  };

  onTimestampModeChanged = (value: TimestampMode) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, timestampMode: value });
    onRunQuery();
  };

  onStreamingChanged = (event: React.FormEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, streaming: event.currentTarget.checked });
    onRunQuery();
  };

  onMaxMessagesChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = parseInt(event.target.value, 10);
    const maxMessages = isNaN(value) || value < 1 ? undefined : value;
    onChange({ ...query, maxMessages });
    onRunQuery();
  };

  onUseTimeRangeChanged = (event: React.FormEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, useTimeRange: event.currentTarget.checked });
    onRunQuery();
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { topicName, partition, autoOffsetReset, timestampMode, streaming, maxMessages, useTimeRange } = query;

    return (
      <>
        <InlineFieldRow>
          <InlineField
            label="Topic"
            labelWidth={10}
            tooltip="Kafka topic name. Supports Grafana template variables (e.g., $variable or ${variable})"
          >
            <Input
              id="query-editor-topic"
              value={topicName || ''}
              onChange={this.onTopicNameChange}
              type="text"
              width={20}
              placeholder="Enter topic name or use $variable"
            />
          </InlineField>
          <InlineField
            label="Partition"
            labelWidth={10}
            tooltip="Kafka partition number. Supports Grafana template variables (e.g., $variable or ${variable})"
          >
            <Input
              id="query-editor-partition"
              value={partition}
              onChange={this.onPartitionChange}
              type="number"
              width={10}
              min={0}
              step={1}
              placeholder="0"
            />
          </InlineField>
        </InlineFieldRow>
        <InlineFieldRow>
          <InlineField
            label="Enable Streaming"
            labelWidth={20}
            tooltip="Enable continuous streaming of data from Kafka. When disabled, a fixed set of messages will be fetched once."
          >
            <Switch value={streaming} onChange={this.onStreamingChanged} />
          </InlineField>
          <InlineField
            label="Max Messages"
            labelWidth={20}
            tooltip="Maximum number of messages to fetch (overrides data source setting). Leave empty to use data source default."
          >
            <Input
              id="query-editor-max-messages"
              value={maxMessages === undefined ? '' : maxMessages}
              onChange={this.onMaxMessagesChange}
              type="number"
              width={15}
              min={1}
              step={10}
              placeholder="Default"
            />
          </InlineField>
        </InlineFieldRow>

        {streaming ? (
          // Show auto offset reset options when streaming is enabled
          <InlineFieldRow>
            <InlineField
              label="Auto offset reset"
              labelWidth={20}
              tooltip="Starting offset to consume that can be from latest or last 100."
            >
              <Combobox
                width={22}
                value={autoOffsetReset}
                options={autoResetOffsets}
                onChange={(value) => this.onAutoResetOffsetChanged(value.value!)}
              />
            </InlineField>
          </InlineFieldRow>
        ) : (
          // Show time range options when streaming is disabled
          <InlineFieldRow>
            <InlineField
              label="Use Dashboard Time Range"
              labelWidth="auto"
              tooltip="When enabled, messages will be fetched based on the dashboard time range. When disabled, auto offset reset will be used."
            >
              <Checkbox value={useTimeRange} onChange={this.onUseTimeRangeChanged} label="" />
            </InlineField>

            {!useTimeRange && (
              <InlineField
                label="Auto offset reset"
                labelWidth="auto"
                tooltip="Starting offset to consume that can be from latest or last 100."
              >
                <Combobox
                  width={'auto'}
                  minWidth={22}
                  value={autoOffsetReset}
                  options={autoResetOffsets}
                  onChange={(value) => this.onAutoResetOffsetChanged(value.value!)}
                />
              </InlineField>
            )}
          </InlineFieldRow>
        )}

        <InlineFieldRow>
          <InlineField label="Timestamp Mode" labelWidth="auto" tooltip="Timestamp of the kafka value to visualize.">
            <Combobox
              width={'auto'}
              minWidth={22}
              value={timestampMode}
              options={timestampModes}
              onChange={(value) => this.onTimestampModeChanged(value.value!)}
            />
          </InlineField>
        </InlineFieldRow>
      </>
    );
  }
}
