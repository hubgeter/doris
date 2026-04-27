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

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.JsonValue;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * MaxCompute column value wrapper backed by raw SDK objects from Record/ArrayRecord.
 */
public class MaxComputeRecordColumnValue implements ColumnValue {
    private int idx;
    private ArrayRecord record;
    private Object fieldData;
    private TypeInfo typeInfo;
    private ZoneId timeZone;

    public MaxComputeRecordColumnValue() {
        this(null, 0, ZoneId.systemDefault());
    }

    public MaxComputeRecordColumnValue(ArrayRecord record, int idx) {
        this(record, idx, ZoneId.systemDefault());
    }

    public MaxComputeRecordColumnValue(ArrayRecord record, int idx, ZoneId timeZone) {
        this.timeZone = timeZone;
        reset(record);
        setColumnIdx(idx);
    }

    public MaxComputeRecordColumnValue(Object fieldData, TypeInfo typeInfo, ZoneId timeZone) {
        this.fieldData = fieldData;
        this.typeInfo = typeInfo;
        this.timeZone = timeZone == null ? ZoneId.systemDefault() : timeZone;
    }

    public void reset(ArrayRecord record) {
        this.record = record;
        this.idx = 0;
        refreshField();
    }

    public void setColumnIdx(int idx) {
        this.idx = idx;
        refreshField();
    }

    public void setTimeZone(ZoneId timeZone) {
        this.timeZone = timeZone == null ? ZoneId.systemDefault() : timeZone;
    }

    @Override
    public boolean canGetStringAsBytes() {
        return true;
    }

    @Override
    public boolean isNull() {
        return fieldData == null;
    }

    @Override
    public boolean getBoolean() {
        return (Boolean) fieldData;
    }

    @Override
    public byte getByte() {
        return ((Number) fieldData).byteValue();
    }

    @Override
    public short getShort() {
        return ((Number) fieldData).shortValue();
    }

    @Override
    public int getInt() {
        return ((Number) fieldData).intValue();
    }

    @Override
    public float getFloat() {
        return ((Number) fieldData).floatValue();
    }

    @Override
    public long getLong() {
        return ((Number) fieldData).longValue();
    }

    @Override
    public double getDouble() {
        return ((Number) fieldData).doubleValue();
    }

    @Override
    public BigInteger getBigInteger() {
        if (fieldData instanceof BigInteger) {
            return (BigInteger) fieldData;
        }
        return BigInteger.valueOf(((Number) fieldData).longValue());
    }

    @Override
    public BigDecimal getDecimal() {
        return (BigDecimal) fieldData;
    }

    @Override
    public String getString() {
        if (fieldData instanceof Char || fieldData instanceof Varchar || fieldData instanceof JsonValue) {
            return fieldData.toString();
        }
        return (String) fieldData;
    }

    @Override
    public byte[] getStringAsBytes() {
        return getString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public LocalDate getDate() {
        return (LocalDate) fieldData;
    }

    @Override
    public LocalDateTime getDateTime() {
        if (typeInfo == null) {
            return coerceDateTime(fieldData);
        }

        OdpsType odpsType = typeInfo.getOdpsType();
        if (odpsType == OdpsType.DATETIME) {
            return ((ZonedDateTime) fieldData).toLocalDateTime();
        }
        if (odpsType == OdpsType.TIMESTAMP_NTZ) {
            return (LocalDateTime) fieldData;
        }
        if (odpsType == OdpsType.TIMESTAMP) {
            return Instant.ofEpochMilli(((Instant) fieldData).toEpochMilli()).atZone(timeZone).toLocalDateTime();
        }
        return coerceDateTime(fieldData);
    }

    @Override
    public LocalDateTime getTimeStampTz() {
        if (fieldData instanceof Instant) {
            return ((Instant) fieldData).atZone(ZoneId.of("UTC")).toLocalDateTime();
        }
        if (fieldData instanceof Timestamp) {
            return ((Timestamp) fieldData).toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime();
        }
        return coerceDateTime(fieldData);
    }

    @Override
    public byte[] getBytes() {
        if (fieldData instanceof Binary) {
            return ((Binary) fieldData).data();
        }
        return (byte[]) fieldData;
    }

    @Override
    public void unpackArray(List<ColumnValue> values) {
        List<?> arrayValues = (List<?>) fieldData;
        TypeInfo elementType = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
        for (Object value : arrayValues) {
            values.add(new MaxComputeRecordColumnValue(value, elementType, timeZone));
        }
    }

    @Override
    public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
        Map<?, ?> mapValues = (Map<?, ?>) fieldData;
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        TypeInfo keyType = mapTypeInfo.getKeyTypeInfo();
        TypeInfo valueType = mapTypeInfo.getValueTypeInfo();
        for (Map.Entry<?, ?> entry : mapValues.entrySet()) {
            keys.add(new MaxComputeRecordColumnValue(entry.getKey(), keyType, timeZone));
            values.add(new MaxComputeRecordColumnValue(entry.getValue(), valueType, timeZone));
        }
    }

    @Override
    public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
        Struct struct = (Struct) fieldData;
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<TypeInfo> fieldTypes = structTypeInfo.getFieldTypeInfos();
        for (Integer fieldIndex : structFieldIndex) {
            Object value = struct.getFieldValue(fieldIndex);
            values.add(new MaxComputeRecordColumnValue(value, fieldTypes.get(fieldIndex), timeZone));
        }
    }

    private void refreshField() {
        if (record == null) {
            fieldData = null;
            typeInfo = null;
            return;
        }
        typeInfo = record.getColumns()[idx].getTypeInfo();
        fieldData = record.isNull(idx) ? null : extractValue(record, idx, typeInfo);
    }

    private static Object extractValue(ArrayRecord record, int idx, TypeInfo typeInfo) {
        OdpsType odpsType = typeInfo.getOdpsType();
        switch (odpsType) {
            case BOOLEAN:
                return record.getBoolean(idx);
            case TINYINT:
                return record.getTinyint(idx);
            case SMALLINT:
                return record.getSmallint(idx);
            case INT:
                return record.getInt(idx);
            case BIGINT:
                return record.getBigint(idx);
            case FLOAT:
                return record.getFloat(idx);
            case DOUBLE:
                return record.getDouble(idx);
            case DECIMAL:
                return record.getDecimal(idx);
            case STRING:
                return record.getString(idx);
            case CHAR:
                return record.getChar(idx);
            case VARCHAR:
                return record.getVarchar(idx);
            case JSON:
                return record.getJsonValue(idx);
            case DATE:
                return record.getDateAsLocalDate(idx);
            case DATETIME:
                return record.getDatetimeAsZonedDateTime(idx);
            case TIMESTAMP:
                return record.getTimestampAsInstant(idx);
            case TIMESTAMP_NTZ:
                return record.getTimestampNtz(idx);
            case BINARY:
                return record.getBinary(idx);
            case ARRAY:
                return record.getArray(idx);
            case MAP:
                return record.getMap(idx);
            case STRUCT:
                return record.getStruct(idx);
            default:
                return record.get(idx);
        }
    }

    private static LocalDateTime coerceDateTime(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        }
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDateTime();
        }
        if (value instanceof Instant) {
            return ((Instant) value).atZone(ZoneId.of("UTC")).toLocalDateTime();
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toLocalDateTime();
        }
        throw new IllegalStateException("Unsupported MaxCompute datetime value type: "
                + (value == null ? "null" : value.getClass().getName()));
    }
}
