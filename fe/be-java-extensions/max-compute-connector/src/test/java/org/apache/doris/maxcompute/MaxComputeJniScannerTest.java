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

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MaxComputeJniScannerTest {
    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");

    @Test
    public void testResultSetColumnValueParsesStringDateTime() throws Exception {
        ColumnValue value = newResultSetColumnValue("2026-04-12 15:16:17.123", "datetimev2(3)");

        Assert.assertEquals(LocalDateTime.of(2026, 4, 12, 15, 16, 17, 123_000_000), value.getDateTime());
    }

    @Test
    public void testResultSetColumnValueParsesOffsetDateTimeString() throws Exception {
        ColumnValue value = newResultSetColumnValue("2026-04-12T15:16:17Z", "datetimev2(3)");

        Assert.assertEquals(LocalDateTime.of(2026, 4, 12, 23, 16, 17), value.getDateTime());
    }

    @Test
    public void testResultSetColumnValueParsesStringDate() throws Exception {
        ColumnValue value = newResultSetColumnValue("2026-04-12", "datev2");

        Assert.assertEquals(LocalDate.of(2026, 4, 12), value.getDate());
    }

    @Test
    public void testResultSetColumnValueUnpacksArrayString() throws Exception {
        ColumnValue value = newResultSetColumnValue("[value1, value2, NULL]", "array<string>");
        List<ColumnValue> elements = new ArrayList<>();

        value.unpackArray(elements);

        Assert.assertEquals(3, elements.size());
        Assert.assertEquals("value1", elements.get(0).getString());
        Assert.assertEquals("value2", elements.get(1).getString());
        Assert.assertTrue(elements.get(2).isNull());
    }

    @Test
    public void testResultSetColumnValueUnpacksMapString() throws Exception {
        ColumnValue value = newResultSetColumnValue("{key1:value1, key2:NULL}", "map<string,string>");
        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();

        value.unpackMap(keys, values);

        Assert.assertEquals(2, keys.size());
        Assert.assertEquals("key1", keys.get(0).getString());
        Assert.assertEquals("value1", values.get(0).getString());
        Assert.assertEquals("key2", keys.get(1).getString());
        Assert.assertTrue(values.get(1).isNull());
    }

    @Test
    public void testResultSetColumnValueUnpacksStructString() throws Exception {
        ColumnValue value = newResultSetColumnValue("{field1:abc, field2:123}", "struct<field1:string,field2:int>");
        List<ColumnValue> fields = new ArrayList<>();

        value.unpackStruct(Arrays.asList(0, 1), fields);

        Assert.assertEquals(2, fields.size());
        Assert.assertEquals("abc", fields.get(0).getString());
        Assert.assertEquals(123, fields.get(1).getInt());
    }

    @Test
    public void testResultSetColumnValueUnpacksNestedComplexString() throws Exception {
        ColumnValue value = newResultSetColumnValue("{key:[{s_int:-123}]}", "map<string,array<struct<s_int:int>>>");
        List<ColumnValue> keys = new ArrayList<>();
        List<ColumnValue> values = new ArrayList<>();

        value.unpackMap(keys, values);

        Assert.assertEquals(1, keys.size());
        Assert.assertEquals("key", keys.get(0).getString());

        List<ColumnValue> arrayElements = new ArrayList<>();
        values.get(0).unpackArray(arrayElements);
        Assert.assertEquals(1, arrayElements.size());

        List<ColumnValue> structFields = new ArrayList<>();
        arrayElements.get(0).unpackStruct(Arrays.asList(0), structFields);
        Assert.assertEquals(-123, structFields.get(0).getInt());
    }

    private ColumnValue newResultSetColumnValue(Object value, String type) throws Exception {
        Class<?> clazz = Class.forName("org.apache.doris.maxcompute.MaxComputeJniScanner$ResultSetColumnValue");
        Constructor<?> constructor = clazz.getDeclaredConstructor(Object.class, ColumnType.class, ZoneId.class);
        constructor.setAccessible(true);
        return (ColumnValue) constructor.newInstance(value, ColumnType.parseType("c1", type), SHANGHAI);
    }
}
