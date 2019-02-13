// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_UTIL_EVENT_METRICS_H
#define IMPALA_UTIL_EVENT_METRICS_H

#include "util/event-metrics.h"

namespace impala {
    string MetastoreEventMetrics::NUMBER_EVENTS_RECEIVED_METRIC_NAME = "events-processor.events-received";
    string MetastoreEventMetrics::NUMBER_EVENTS_SKIPPED_METRIC_NAME = "events-processor.events-skipped";
    string MetastoreEventMetrics::EVENT_PROCESSOR_STATUS_METRIC_NAME = "events-processor.status";
    string MetastoreEventMetrics::EVENTS_FETCH_DURATION_MEAN_METRIC_NAME = "events-processor.avg-events-fetch-duration";
    string MetastoreEventMetrics::EVENTS_PROCESS_DURATION_MEAN_METRIC_NAME = "events-processor.avg-events-process-duration";

    string MetastoreEventMetrics::EVENTS_RECEIVED_1MIN_METRIC_NAME = "events-processor.events-received-1min-rate";
    string MetastoreEventMetrics::EVENTS_RECEIVED_5MIN_METRIC_NAME = "events-processor.events-received-5min-rate";
    string MetastoreEventMetrics::EVENTS_RECEIVED_15MIN_METRIC_NAME = "events-processor.events-received-15min-rate";

    IntCounter* MetastoreEventMetrics::NUM_EVENTS_RECEIVED_COUNTER = nullptr;
    IntCounter* MetastoreEventMetrics::NUM_EVENTS_SKIPPED_COUNTER = nullptr;

    DoubleGauge* MetastoreEventMetrics::EVENTS_FETCH_DURATION_MEAN = nullptr;
    DoubleGauge* MetastoreEventMetrics::EVENTS_PROCESS_DURATION_MEAN = nullptr;

    StringProperty* MetastoreEventMetrics::EVENT_PROCESSOR_STATUS = nullptr;

    DoubleGauge* MetastoreEventMetrics::EVENTS_RECEIVED_1MIN_RATE = nullptr;
    DoubleGauge* MetastoreEventMetrics::EVENTS_RECEIVED_5MIN_RATE = nullptr;
    DoubleGauge* MetastoreEventMetrics::EVENTS_RECEIVED_15MIN_RATE = nullptr;

    void MetastoreEventMetrics::InitMetastoreEventMetrics(MetricGroup *metric_group) {
        MetricGroup *event_metrics = metric_group->GetOrCreateChildGroup("events");
        NUM_EVENTS_RECEIVED_COUNTER = event_metrics->AddCounter(NUMBER_EVENTS_RECEIVED_METRIC_NAME, 0);
        NUM_EVENTS_SKIPPED_COUNTER = event_metrics->AddCounter(NUMBER_EVENTS_SKIPPED_METRIC_NAME, 0);
        EVENTS_FETCH_DURATION_MEAN = event_metrics->AddDoubleGauge(EVENTS_FETCH_DURATION_MEAN_METRIC_NAME, 0.0);
        EVENTS_PROCESS_DURATION_MEAN = event_metrics->AddDoubleGauge(EVENTS_PROCESS_DURATION_MEAN_METRIC_NAME, 0.0);
        EVENT_PROCESSOR_STATUS = event_metrics->AddProperty<string>(EVENT_PROCESSOR_STATUS_METRIC_NAME, "Unavailable");

        EVENTS_RECEIVED_1MIN_RATE = event_metrics->AddDoubleGauge(EVENTS_RECEIVED_1MIN_METRIC_NAME, 0.0);
        EVENTS_RECEIVED_5MIN_RATE = event_metrics->AddDoubleGauge(EVENTS_RECEIVED_5MIN_METRIC_NAME, 0.0);
        EVENTS_RECEIVED_15MIN_RATE = event_metrics->AddDoubleGauge(EVENTS_RECEIVED_15MIN_METRIC_NAME, 0.0);
    }

    void MetastoreEventMetrics::refresh(TGetEventProcessorMetricsResponse *response) {
        if (response != nullptr) {
            EVENT_PROCESSOR_STATUS->SetValue(response->status.c_str());
            DCHECK(response->__isset.events_process_duration_mean);
            DCHECK(response->__isset.events_fetch_duration_mean);
            DCHECK(response->__isset.events_skipped);
            DCHECK(response->__isset.events_received);
            DCHECK(response->__isset.events_received_1min_rate);
            DCHECK(response->__isset.events_received_5min_rate);
            DCHECK(response->__isset.events_received_15min_rate);

            if (response->__isset.events_received) {
                NUM_EVENTS_RECEIVED_COUNTER->SetValue(response->events_received);
            }
            if (response->__isset.events_skipped) {
                NUM_EVENTS_SKIPPED_COUNTER->SetValue(response->events_skipped);
            }
            if (response->__isset.events_fetch_duration_mean) {
                EVENTS_FETCH_DURATION_MEAN->SetValue(response->events_fetch_duration_mean);
            }
            if (response->__isset.events_process_duration_mean) {
                EVENTS_PROCESS_DURATION_MEAN->SetValue(response->events_process_duration_mean);
            }
            if (response->__isset.events_received_1min_rate) {
                EVENTS_RECEIVED_1MIN_RATE->SetValue(response->events_received_1min_rate);
            }
            if (response->__isset.events_received_5min_rate) {
                EVENTS_RECEIVED_5MIN_RATE->SetValue(response->events_received_1min_rate);
            }
            if (response->__isset.events_received_15min_rate) {
                EVENTS_RECEIVED_15MIN_RATE->SetValue(response->events_received_15min_rate);
            }
            return;
        }
        LOG(ERROR) << "Received a null response when trying to refresh metastore event metrics";
    }
}

#endif