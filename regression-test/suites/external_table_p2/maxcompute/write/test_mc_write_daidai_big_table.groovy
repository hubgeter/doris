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

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

suite("test_mc_write_daidai_big_table", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_daidai_big_table"
    String defaultProject = "mc_doris_test_write"

    String partitionStartValue = "20250101"
    String partitionEndValue = "20250110"
    long totalRowCount = 10000000L

    DateTimeFormatter partitionFormatter = DateTimeFormatter.BASIC_ISO_DATE
    LocalDate partitionStartDate = LocalDate.parse(partitionStartValue, partitionFormatter)
    LocalDate partitionEndDate = LocalDate.parse(partitionEndValue, partitionFormatter)
    long partitionCount = ChronoUnit.DAYS.between(partitionStartDate, partitionEndDate) + 1
    assert partitionCount > 0
    assert totalRowCount % partitionCount == 0
    long rowsPerPartition = totalRowCount / partitionCount

    sql """drop catalog if exists ${mc_catalog_name}"""
    sql """
    CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
        "type" = "max_compute",
        "mc.default.project" = "${defaultProject}",
        "mc.access_key" = "${ak}",
        "mc.secret_key" = "${sk}",
        "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
        "mc.quota" = "pay-as-you-go",
        "mc.enable.namespace.schema" = "true"
    );
    """

    sql """switch ${mc_catalog_name}"""

    String db = "mc_write_daidai_big_table"
    String tb = "daidai_big_table"
    String dataColumnDefs = (1..50).collect { "col${it} STRING" }.join(",\n            ")

    sql """create database if not exists ${db}"""
    sql """use ${db}"""

    sql """DROP TABLE IF EXISTS ${tb}"""
    sql """
    CREATE TABLE ${tb} (
        id BIGINT,
        ${dataColumnDefs},
        dt STRING
    )
    PARTITION BY (dt)()
    """

    for (long partitionOffset = 0; partitionOffset < partitionCount; partitionOffset++) {
        String partitionValue = partitionStartDate.plusDays(partitionOffset).format(partitionFormatter)
        String insertColumnExprs = (1..50).collect {
            "CONCAT('${it}_', '${partitionValue}_', CAST(number AS STRING)) AS col${it}"
        }.join(",\n            ")
        long idOffset = partitionOffset * rowsPerPartition
        sql """
        INSERT INTO ${tb} PARTITION (dt='${partitionValue}')
        SELECT
            ${idOffset} + number AS id,
            ${insertColumnExprs}
        FROM numbers("number"="${rowsPerPartition}")
        """
    }
}
