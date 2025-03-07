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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_javaudtf_decimal") {
    def tableName = "test_javaudtf_decimal"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` INT NOT NULL COMMENT "",
            `cost_1` decimal(27,9) NOT NULL COMMENT "",
            `cost_2` decimal(27,9) COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        
        
        sql """ INSERT INTO ${tableName} (`user_id`,`cost_1`,`cost_2`) VALUES
                (111,11111.11111,222222.3333333),
                (112,1234556.11111,222222.3333333),
                (113,87654321.11111,null)
            """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }
        sql """DROP FUNCTION IF EXISTS udtf_decimal_outer(decimal(27,9));"""
        sql """ CREATE TABLES FUNCTION udtf_decimal(decimal(27,9)) RETURNS array<decimal(27,9)> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDTFDecimalTest",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); """

        qt_select1 """ SELECT user_id, cost_1, e1 FROM ${tableName} lateral view  udtf_decimal(cost_1) temp as e1 order by user_id; """
        qt_select2 """ SELECT user_id, cost_2, e1 FROM ${tableName} lateral view  udtf_decimal(cost_2) temp as e1 order by user_id; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS udtf_decimal(decimal(27,9));")
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
