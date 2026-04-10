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

suite("test_max_compute_mcqa", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk")

        String mcCatalog = "test_max_compute_mcqa"
        String schemaCatalog = "test_max_compute_mcqa_schema"

        sql """drop catalog if exists ${mcCatalog}"""
        sql """
            create catalog if not exists ${mcCatalog} properties (
                "type" = "max_compute",
                "mc.default.project" = "mc_datalake",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.enable_mcqa_query" = "true",
                "mc.mcqa_query_limit_threshold" = "4096"
            );
        """

        sql """switch ${mcCatalog}"""
        sql """use mc_datalake"""

        qt_mcqa_all_types """select * from mc_all_types limit 1"""
        qt_mcqa_datetime_tb1 """select * from datetime_tb1 limit 1"""
        qt_mcqa_timestamp_tb1 """select * from timestamp_tb1 limit 1"""
        qt_mcqa_timestamp_tb2 """select * from timestamp_tb2 limit 1"""
        qt_mcqa_timestamp_tb3 """select * from timestamp_tb3 limit 1"""
        qt_mcqa_timestamp_tb4 """select * from timestamp_tb4 limit 1"""
        qt_mcqa_array_table """select * from array_table limit 1"""
        qt_mcqa_map_table """select * from map_table limit 1"""
        qt_mcqa_struct_table """select * from struct_table limit 1"""
        qt_mcqa_nested_complex """select * from nested_complex_table limit 1"""
        qt_mcqa_one_partition """select * from one_partition_tb where part1 = 2024 limit 1"""
        qt_mcqa_two_partition """select * from two_partition_tb where part1 = 'US' and part2 = 1 limit 1"""

        sql """drop catalog if exists ${schemaCatalog}"""
        sql """
            create catalog if not exists ${schemaCatalog} properties (
                "type" = "max_compute",
                "mc.default.project" = "mc_datalake_schema",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
                "mc.enable_mcqa_query" = "true",
                "mc.mcqa_query_limit_threshold" = "4096",
                "mc.enable.namespace.schema" = "true"
            );
        """

        sql """switch ${schemaCatalog}"""
        sql """use analytics"""

        qt_mcqa_namespace_non_partition """select * from product_sales limit 1"""
        qt_mcqa_namespace_partition """select * from web_log where ds2 = '20251015' and hour = '10' limit 1"""

        sql """drop catalog if exists ${schemaCatalog}"""
        sql """drop catalog if exists ${mcCatalog}"""
    }
}
