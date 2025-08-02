import { CoreApp, DataQueryRequest, DataQueryResponse, DataSourceInstanceSettings, LiveChannelScope, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getGrafanaLiveSrv, getTemplateSrv } from '@grafana/runtime';
import { Observable, merge, throwError } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode } from './types';
export class DataSource extends DataSourceWithBackend<KafkaQuery, KafkaDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<KafkaDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<KafkaQuery> {
    return {
      topicName: '',
      partition: 0,
      autoOffsetReset: AutoOffsetReset.LATEST,
      timestampMode: TimestampMode.Now
    };
  }

  filterQuery(query: KafkaQuery): boolean {
    return !!query.topicName && query.partition >= 0;
  }

  applyTemplateVariables(query: KafkaQuery, scopedVars: ScopedVars) {
    const templateSrv = getTemplateSrv();
    return {
      ...query,
      topicName: templateSrv.replace(query.topicName, scopedVars),
      partition: Number.parseInt(
        templateSrv.replace(query.partition.toString(), scopedVars),
        10,
      ) || 0,
    };
  }

  query(request: DataQueryRequest<KafkaQuery>): Observable<DataQueryResponse> {
    const observables = request.targets
      .filter(this.filterQuery)
      .map(query => {
        const interpolatedQuery = this.applyTemplateVariables(query, request.scopedVars);
        if (interpolatedQuery.streaming) {
          // Use streaming mode (Grafana Live)
          return getGrafanaLiveSrv().getDataStream({
            addr: {
              scope: LiveChannelScope.DataSource,
              namespace: this.uid,
              path: `${interpolatedQuery.topicName}-${interpolatedQuery.partition}-${interpolatedQuery.autoOffsetReset}`,
              data: interpolatedQuery,
            },
          }).pipe(
            catchError(err => {
              console.error('Stream error:', err);
              return throwError(() => ({
                message: `Error connecting to Kafka topic ${interpolatedQuery.topicName}: ${err.message}`,
                status: 'error'
              }));
            })
          );
        } else {
          // Use non-streaming mode (regular query)
          return super.query({
            ...request,
            targets: [interpolatedQuery],
          });
        }
      });

    return merge(...observables);
  }
}
