# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
import json
import time
import requests

from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, \
    SkipIfLocal, SkipIfHive2
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon, SkipIfLocal
from tests.util.hive_utils import HiveDbWrapper
from tests.util.event_processor_utils import EventProcessorUtils


@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestEventProcessing(CustomClusterTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog."""

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  @SkipIfHive2.acid
  def test_insert_events_transactional(self,unique_database):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    self.run_test_insert_events(unique_database, is_transactional=True)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for non-transactional tables.
    """
    self.run_test_insert_events(unique_database)

  def run_test_insert_events(self, unique_database, is_transactional=False):
    """Test for insert event processing. Events are created in Hive and processed in
    Impala. The following cases are tested :
    Insert into table --> for partitioned and non-partitioned table
    Insert overwrite table --> for partitioned and non-partitioned table
    Insert into partition --> for partitioned table
    """
    # Test table with no partitions.
    TBL_INSERT_NOPART = 'tbl_insert_nopart'
    self.run_stmt_in_hive("drop table if exists %s.%s" % (unique_database, TBL_INSERT_NOPART))
    TBLPROPERTIES = ""
    if is_transactional:
      TBLPROPERTIES = "TBLPROPERTIES ('transactional'='true'," \
          "'transactional_properties'='insert_only')"
    self.run_stmt_in_hive("create table %s.%s (id int, val int) %s"
        % (unique_database, TBL_INSERT_NOPART, TBLPROPERTIES))
    # Test insert into table, this will fire an insert event.
    self.run_stmt_in_hive("insert into %s.%s values(101, 200)"
        % (unique_database, TBL_INSERT_NOPART))
    # With MetastoreEventProcessor running, the insert event will be processed. Query the
    # table from Impala.
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s" % (unique_database, TBL_INSERT_NOPART))
    assert data.split('\t') == ['101', '200']
 
    # Test insert overwrite. Overwrite the existing value.
    self.run_stmt_in_hive("insert overwrite table %s.%s values(101, 201)"
        % (unique_database, TBL_INSERT_NOPART))
    # Make sure the event has been processed.
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s" % (unique_database, TBL_INSERT_NOPART))
    assert data.split('\t') == ['101', '201']
 
    # Test partitioned table.
    TBL_INSERT_PART = 'tbl_insert_part'
    self.run_stmt_in_hive("drop table if exists %s.%s" % (unique_database, TBL_INSERT_PART))
    self.run_stmt_in_hive("create table %s.%s (id int, name string) "
        "partitioned by(day int, month int, year int) %s"
        % (unique_database, TBL_INSERT_PART, TBLPROPERTIES))
    # Insert data into partitions.
    self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
        "values(101, 'x')" % (unique_database, TBL_INSERT_PART))
    # Make sure the event has been processed.
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s" % (unique_database, TBL_INSERT_PART))
    assert data.split('\t') == ['101', 'x', '28', '3', '2019']
 
    # Test inserting into existing partitions.
    self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
        "values(102, 'y')" % (unique_database, TBL_INSERT_PART))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select count(*) from %s.%s where day=28 and month=3 "
        "and year=2019" % (unique_database, TBL_INSERT_PART))
    assert data.split('\t') == ['2']
 
    # Test insert overwrite into existing partitions
    self.run_stmt_in_hive("insert overwrite table %s.%s partition(day=28, month=03, "
        "year=2019)" "values(101, 'z')" % (unique_database, TBL_INSERT_PART))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s where day=28 and month=3 and"
        " year=2019 and id=101" % (unique_database, TBL_INSERT_PART))
    assert data.split('\t') == ['101', 'z', '28', '3', '2019']
 
  @CustomClusterTestSuite.with_args(
     catalogd_args="--hms_event_polling_interval_s=1"
  )
  @SkipIfHive2.acid
  def test_empty_partition_events_transactional(self, unique_database):
    self._run_test_empty_partition_events(unique_database, True)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1"
  )
  def test_empty_partition_events(self, unique_database):
    self._run_test_empty_partition_events(unique_database, False)

  def _run_test_empty_partition_events(self, unique_database, is_transactional):
    TBLPROPERTIES = ""
    if is_transactional:
       TBLPROPERTIES = "TBLPROPERTIES ('transactional'='true'," \
           "'transactional_properties'='insert_only')"
    test_tbl = unique_database + ".test_events"
    self.run_stmt_in_hive("create table {0} (key string, value string) \
      partitioned by (year int) stored as parquet {1}".format(test_tbl, TBLPROPERTIES))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    self.client.execute("describe {0}".format(test_tbl))

    self.run_stmt_in_hive(
      "alter table {0} add partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')

    self.run_stmt_in_hive(
      "alter table {0} add if not exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop if exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self.hive_client)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

