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

package org.apache.doris.maxcompute;

import org.apache.doris.common.jni.vec.ColumnValue;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MaxComputeRecordColumnValueTest {
    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");

    @Test
    public void testPrimitiveAndTemporalExtraction() {
        Column[] columns = new Column[] {
                new Column("boolean_col", TypeInfoFactory.BOOLEAN),
                new Column("datetime_col", TypeInfoFactory.DATETIME),
                new Column("timestamp_col", TypeInfoFactory.TIMESTAMP),
                new Column("timestamp_ntz_col", TypeInfoFactory.TIMESTAMP_NTZ),
                new Column("date_col", TypeInfoFactory.DATE),
                new Column("decimal_col", TypeInfoFactory.getDecimalTypeInfo(10, 2))
        };

        ZonedDateTime zonedDateTime = ZonedDateTime.of(2024, 4, 20, 9, 8, 7, 0, SHANGHAI);
        Instant timestamp = Instant.parse("2024-04-20T01:02:03Z");
        LocalDateTime timestampNtz = LocalDateTime.of(2024, Month.APRIL, 20, 11, 12, 13, 456000000);
        LocalDate localDate = LocalDate.of(2024, Month.APRIL, 21);
        BigDecimal decimal = new BigDecimal("123.45");
        ArrayRecord record = new ArrayRecord(columns, new Object[] {
                true,
                zonedDateTime,
                timestamp,
                timestampNtz,
                localDate,
                decimal
        });

        MaxComputeRecordColumnValue value = new MaxComputeRecordColumnValue(record, 0, SHANGHAI);
        Assert.assertTrue(value.getBoolean());

        value.setColumnIdx(1);
        Assert.assertEquals(zonedDateTime.toLocalDateTime(), value.getDateTime());

        value.setColumnIdx(2);
        Assert.assertEquals(LocalDateTime.of(2024, Month.APRIL, 20, 9, 2, 3), value.getDateTime());
        Assert.assertEquals(LocalDateTime.of(2024, Month.APRIL, 20, 1, 2, 3), value.getTimeStampTz());

        value.setColumnIdx(3);
        Assert.assertEquals(timestampNtz, value.getDateTime());

        value.setColumnIdx(4);
        Assert.assertEquals(localDate, value.getDate());

        value.setColumnIdx(5);
        Assert.assertEquals(decimal, value.getDecimal());
    }

    @Test
    public void testUnpackArrayMapAndStruct() {
        TypeInfo intType = TypeInfoFactory.INT;
        TypeInfo stringType = TypeInfoFactory.STRING;
        TypeInfo arrayType = TypeInfoFactory.getArrayTypeInfo(intType);
        TypeInfo mapType = TypeInfoFactory.getMapTypeInfo(stringType, intType);
        TypeInfo structType = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("name", "score"),
                Arrays.asList(stringType, intType));
        Column[] columns = new Column[] {
                new Column("array_col", arrayType),
                new Column("map_col", mapType),
                new Column("struct_col", structType)
        };

        Map<String, Integer> mapValue = new LinkedHashMap<>();
        mapValue.put("alice", 10);
        mapValue.put("bob", 20);
        Struct structValue = new SimpleStruct((StructTypeInfo) structType, Arrays.<Object>asList("carol", 30));
        ArrayRecord record = new ArrayRecord(columns, new Object[] {
                Arrays.asList(1, 2, 3),
                mapValue,
                structValue
        });

        MaxComputeRecordColumnValue value = new MaxComputeRecordColumnValue(record, 0, SHANGHAI);

        List<ColumnValue> arrayValues = new ArrayList<>();
        value.unpackArray(arrayValues);
        Assert.assertEquals(3, arrayValues.size());
        Assert.assertEquals(1, arrayValues.get(0).getInt());
        Assert.assertEquals(3, arrayValues.get(2).getInt());

        value.setColumnIdx(1);
        List<ColumnValue> mapKeys = new ArrayList<>();
        List<ColumnValue> mapValues = new ArrayList<>();
        value.unpackMap(mapKeys, mapValues);
        Assert.assertEquals(2, mapKeys.size());
        Assert.assertEquals("alice", mapKeys.get(0).getString());
        Assert.assertEquals(10, mapValues.get(0).getInt());
        Assert.assertEquals("bob", mapKeys.get(1).getString());
        Assert.assertEquals(20, mapValues.get(1).getInt());

        value.setColumnIdx(2);
        List<ColumnValue> structValues = new ArrayList<>();
        value.unpackStruct(Arrays.asList(0, 1), structValues);
        Assert.assertEquals(2, structValues.size());
        Assert.assertEquals("carol", structValues.get(0).getString());
        Assert.assertEquals(30, structValues.get(1).getInt());
    }
}
