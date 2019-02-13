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

#include "util/metrics.h"

namespace impala {

// class which is used to fetch the metastore event related metrics from catalog
class MetastoreEventMetrics {

public:
    /// Registers and initializes the Metastore event metrics
    static void InitMetastoreEventMetrics(MetricGroup* metric_group);
    // refreshes the metrics based on the given response
    static void refresh(TGetEventProcessorMetricsResponse* response);

    static IntCounter* NUM_EVENTS_RECEIVED_COUNTER;
    static IntCounter* NUM_EVENTS_SKIPPED_COUNTER;
    static DoubleGauge* EVENTS_FETCH_DURATION_MEAN;
    static DoubleGauge* EVENTS_PROCESS_DURATION_MEAN;
    static StringProperty* EVENT_PROCESSOR_STATUS;
    static DoubleGauge* EVENTS_RECEIVED_1MIN_RATE;
    static DoubleGauge* EVENTS_RECEIVED_5MIN_RATE;
    static DoubleGauge* EVENTS_RECEIVED_15MIN_RATE;

private:
    static string NUMBER_EVENTS_RECEIVED_METRIC_NAME;
    static string NUMBER_EVENTS_SKIPPED_METRIC_NAME;
    static string EVENT_PROCESSOR_STATUS_METRIC_NAME;
    static string EVENTS_FETCH_DURATION_MEAN_METRIC_NAME;
    static string EVENTS_PROCESS_DURATION_MEAN_METRIC_NAME;
    static string EVENTS_RECEIVED_1MIN_METRIC_NAME;
    static string EVENTS_RECEIVED_5MIN_METRIC_NAME;
    static string EVENTS_RECEIVED_15MIN_METRIC_NAME;
};

}
