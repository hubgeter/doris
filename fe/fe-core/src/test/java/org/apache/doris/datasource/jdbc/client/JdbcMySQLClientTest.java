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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Types;
import java.util.Optional;

public class JdbcMySQLClientTest {

    @Test
    public void testMysqlTypeToDorisNormalizesLowerCaseMetadataType() {
        JdbcFieldSchema fieldSchema = mockFieldSchema(Types.VARCHAR, "varchar", null, 10, null);

        Type type = JdbcMySQLClient.mysqlTypeToDoris(fieldSchema, false, false, false);

        Assert.assertEquals(ScalarType.createVarcharType(10), type);
    }

    @Test
    public void testMysqlTypeToDorisUsesOceanBaseRawUnsignedDefinition() {
        JdbcFieldSchema fieldSchema = mockFieldSchema(Types.INTEGER, "int", "int(10) unsigned", 10, 0);

        Type type = JdbcMySQLClient.mysqlTypeToDoris(fieldSchema, false, false, false);

        Assert.assertEquals(Type.BIGINT, type);
    }

    @Test
    public void testMysqlTypeToDorisUsesOceanBaseRawTemporalScale() {
        JdbcFieldSchema fieldSchema = mockFieldSchema(Types.TIMESTAMP, "timestamp", "timestamp(4)", 19, null);

        Type type = JdbcMySQLClient.mysqlTypeToDoris(fieldSchema, false, false, false);

        Assert.assertEquals(ScalarType.createDatetimeV2Type(4), type);
    }

    @Test
    public void testMysqlTypeToDorisPreservesOceanBaseBooleanMetadata() {
        JdbcFieldSchema fieldSchema = mockFieldSchema(Types.BIT, "BIT", "tinyint(1)", 5, 0);

        Type type = JdbcMySQLClient.mysqlTypeToDoris(fieldSchema, false, false, false);

        Assert.assertEquals(Type.BOOLEAN, type);
    }

    private JdbcFieldSchema mockFieldSchema(int jdbcType, String typeName, String fullTypeName,
            Integer columnSize, Integer decimalDigits) {
        JdbcFieldSchema fieldSchema = Mockito.mock(JdbcFieldSchema.class);
        Mockito.when(fieldSchema.getDataType()).thenReturn(jdbcType);
        Mockito.when(fieldSchema.getDataTypeName()).thenReturn(Optional.ofNullable(typeName));
        Mockito.when(fieldSchema.getFullDataTypeName()).thenReturn(Optional.ofNullable(fullTypeName));
        Mockito.when(fieldSchema.getColumnSize()).thenReturn(Optional.ofNullable(columnSize));
        Mockito.when(fieldSchema.getDecimalDigits()).thenReturn(Optional.ofNullable(decimalDigits));
        Mockito.when(fieldSchema.requiredColumnSize()).thenReturn(columnSize == null ? 0 : columnSize);
        Mockito.when(fieldSchema.requiredDecimalDigits()).thenReturn(decimalDigits == null ? 0 : decimalDigits);
        return fieldSchema;
    }
}
