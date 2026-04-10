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

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.doris.common.maxcompute.MCProperties;
import org.apache.doris.common.maxcompute.MCUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.FallbackPolicy;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.google.common.base.Strings;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MaxComputeJ JniScanner. BE will read data from the scanner object.
 */
public class MaxComputeJniScanner extends JniScanner {
    static {
        //Set `NullCheckingForGet.NULL_CHECKING_ENABLED` false.
        //We will call isNull() before calling getXXX(), so we can set this parameter
        // to skip the repeated check of isNull().
        System.setProperty("arrow.enable_null_check_for_get", "false");
    }

    private static final Logger LOG = Logger.getLogger(MaxComputeJniScanner.class);

    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";
    private static final String ENDPOINT = "endpoint";
    private static final String QUOTA = "quota";
    private static final String PROJECT = "project";
    private static final String TABLE = "table";

    private static final String START_OFFSET = "start_offset";
    private static final String SPLIT_SIZE = "split_size";
    private static final String SESSION_ID = "session_id";
    private static final String SCAN_SERIALIZER = "scan_serializer";
    private static final String TIME_ZONE = "time_zone";

    private static final String CONNECT_TIMEOUT = "connect_timeout";
    private static final String READ_TIMEOUT = "read_timeout";
    private static final String RETRY_COUNT  = "retry_count";
    private static final String MCQA_PAYLOAD_PREFIX = "mcqa:v1:";
    private static final int LOGVIEW_V2 = 2;

    private enum SplitType {
        BYTE_SIZE,
        ROW_OFFSET
    }

    private SplitType splitType;
    private TableBatchReadSession scan;
    public  String sessionId;

    private String project;
    private String table;

    private SplitReader<VectorSchemaRoot> currentSplitReader;
    private MaxComputeColumnValue columnValue;
    private Odps odps;
    private SQLExecutor mcqaExecutor;
    private boolean mcqaMode;
    private String remoteQueryProject;
    private String remoteQuerySql;
    private String mcqaQuota;
    private boolean namespaceSchemaEnabled;
    private ResultSet remoteResultSet;
    private long remoteRowsRead;
    private long remoteSubmitTime;
    private long remoteResultSetInitTime;
    private long remoteFirstBatchTime;
    private boolean remoteFirstBatchLogged;

    private Map<String, Integer> readColumnsToId;

    private long startOffset = -1L;
    private long splitSize = -1L;
    public EnvironmentSettings settings;
    public ZoneId timeZone;

    public MaxComputeJniScanner(int batchSize, Map<String, String> params) {
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);

        if (!Strings.isNullOrEmpty(params.get(START_OFFSET))
                && !Strings.isNullOrEmpty(params.get(SPLIT_SIZE))) {
            startOffset = Long.parseLong(params.get(START_OFFSET));
            splitSize = Long.parseLong(params.get(SPLIT_SIZE));
            if (splitSize == -1) {
                splitType = SplitType.BYTE_SIZE;
            } else {
                splitType = SplitType.ROW_OFFSET;
            }
        }

        String endpoint = Objects.requireNonNull(params.get(ENDPOINT), "required property '" + ENDPOINT + "'.");
        String quota = Objects.requireNonNull(params.get(QUOTA), "required property '" + QUOTA + "'.");
        String scanSerializer = Objects.requireNonNull(params.get(SCAN_SERIALIZER),
                "required property '" + SCAN_SERIALIZER + "'.");
        project = Objects.requireNonNull(params.get(PROJECT), "required property '" + PROJECT + "'.");
        table = Objects.requireNonNull(params.get(TABLE), "required property '" + TABLE + "'.");
        sessionId = Objects.requireNonNull(params.get(SESSION_ID), "required property '" + SESSION_ID + "'.");
        mcqaQuota = params.get(MCProperties.MCQA_QUOTA);
        namespaceSchemaEnabled = Boolean.parseBoolean(params.getOrDefault(
                MCProperties.ENABLE_NAMESPACE_SCHEMA, MCProperties.DEFAULT_ENABLE_NAMESPACE_SCHEMA));
        String timeZoneName = Objects.requireNonNull(params.get(TIME_ZONE), "required property '" + TIME_ZONE + "'.");
        try {
            timeZone = ZoneId.of(timeZoneName);
        } catch (Exception e) {
            LOG.warn(e.getMessage() + " Set timeZoneName = " + timeZoneName + "fail, use systemDefault.");
            timeZone = ZoneId.systemDefault();
        }

        odps = MCUtils.createMcClient(params);
        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);

        Credentials credentials = Credentials.newBuilder().withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount()).build();


        int connectTimeout = 10; // 10s
        if (!Strings.isNullOrEmpty(params.get(CONNECT_TIMEOUT))) {
            connectTimeout = Integer.parseInt(params.get(CONNECT_TIMEOUT));
        }

        int readTimeout = 120; // 120s
        if (!Strings.isNullOrEmpty(params.get(READ_TIMEOUT))) {
            readTimeout =  Integer.parseInt(params.get(READ_TIMEOUT));
        }

        int retryTimes = 4; // 4 times
        if (!Strings.isNullOrEmpty(params.get(RETRY_COUNT))) {
            retryTimes = Integer.parseInt(params.get(RETRY_COUNT));
        }

        RestOptions restOptions = RestOptions.newBuilder()
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout)
                .withRetryTimes(retryTimes).build();

        settings = EnvironmentSettings.newBuilder()
                .withCredentials(credentials)
                .withServiceEndpoint(odps.getEndpoint())
                .withQuotaName(quota)
                .withRestOptions(restOptions)
                .build();

        if (isMcqaPayload(scanSerializer)) {
            QueryPayload payload = decodeQueryPayload(scanSerializer, MCQA_PAYLOAD_PREFIX);
            mcqaMode = true;
            remoteQueryProject = Strings.isNullOrEmpty(payload.project) ? project : payload.project;
            remoteQuerySql = payload.query;
            odps.setDefaultProject(remoteQueryProject);
        } else {
            try {
                scan = (TableBatchReadSession) deserialize(scanSerializer);
            } catch (Exception e) {
                String errorMsg = "Failed to deserialize table batch read session.";
                LOG.warn(errorMsg, e);
                throw new IllegalArgumentException(errorMsg, e);
            }
        }
    }


    @Override
    protected void initTableInfo(ColumnType[] requiredTypes, String[] requiredFields, int batchSize) {
        super.initTableInfo(requiredTypes, requiredFields, batchSize);
        readColumnsToId = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            if (!Strings.isNullOrEmpty(fields[i])) {
                readColumnsToId.put(fields[i], i);
            }
        }
    }

    @Override
    public void open() throws IOException {
        if (mcqaMode) {
            openMcqaScanner();
            return;
        }
        try {
            InputSplit split;
            if (splitType == SplitType.BYTE_SIZE) {
                split = new IndexedInputSplit(sessionId, (int) startOffset);
            } else {
                split = new RowRangeInputSplit(sessionId, startOffset, splitSize);
            }

            currentSplitReader = scan.createArrowReader(split, ReaderOptions.newBuilder().withSettings(settings)
                    .withCompressionCodec(CompressionCodec.ZSTD)
                    .withReuseBatch(true)
                    .build());

        } catch (Exception e) {
            String errorMsg = "MaxComputeJniScanner Failed to open table batch read session.";
            LOG.warn(errorMsg, e);
            close();
            throw new IOException(errorMsg, e);
        }
    }

    @Override
    public void close() throws IOException {
        startOffset = -1;
        splitSize = -1;
        currentSplitReader = null;
        settings = null;
        scan = null;
        remoteResultSet = null;
        if (mcqaExecutor != null) {
            mcqaExecutor.close();
            mcqaExecutor = null;
        }
        remoteQuerySql = null;
        remoteQueryProject = null;
        remoteRowsRead = 0;
        remoteSubmitTime = 0;
        remoteResultSetInitTime = 0;
        remoteFirstBatchTime = 0;
        remoteFirstBatchLogged = false;
        readColumnsToId.clear();
    }

    @Override
    protected int getNext() throws IOException {
        if (mcqaMode) {
            return readMcqaRecords(batchSize);
        }
        if (currentSplitReader == null) {
            return 0;
        }
        columnValue = new MaxComputeColumnValue();
        columnValue.setTimeZone(timeZone);
        int expectedRows = batchSize;
        return readVectors(expectedRows);
    }

    private int readVectors(int expectedRows) throws IOException {
        int curReadRows = 0;
        while (curReadRows < expectedRows) {
            try {
                if (!currentSplitReader.hasNext()) {
                    currentSplitReader.close();
                    currentSplitReader = null;
                    break;
                }
            } catch (Exception e) {
                String errorMsg = "MaxComputeJniScanner readVectors hasNext fail";
                LOG.warn(errorMsg, e);
                throw new IOException(e.getMessage(), e);
            }

            try {
                VectorSchemaRoot data = currentSplitReader.get();
                if (data.getRowCount() == 0) {
                    break;
                }

                List<FieldVector> fieldVectors = data.getFieldVectors();
                int batchRows = 0;
                long startTime = System.nanoTime();
                for (FieldVector column : fieldVectors) {
                    Integer readColumnId = readColumnsToId.get(column.getName());
                    batchRows = column.getValueCount();
                    if (readColumnId == null) {
                        continue;
                    }
                    columnValue.reset(column);
                    for (int j = 0; j < batchRows; j++) {
                        columnValue.setColumnIdx(j);
                        appendData(readColumnId, columnValue);
                    }
                }
                appendDataTime += System.nanoTime() - startTime;

                curReadRows += batchRows;
            } catch (Exception e) {
                String errorMsg = String.format("MaxComputeJniScanner Fail to read arrow data. "
                        + "curReadRows = {}, expectedRows = {}", curReadRows, expectedRows);
                LOG.warn(errorMsg, e);
                throw new RuntimeException(errorMsg, e);
            }
        }
        return curReadRows;
    }

    @Override
    public Map<String, String> getStatistics() {
        Map<String, String> stats = new HashMap<>();
        if (mcqaMode) {
            stats.put("counter:McqaRowsRead", String.valueOf(remoteRowsRead));
            stats.put("timer:McqaRunTime", String.valueOf(remoteSubmitTime));
            stats.put("timer:McqaResultSetInitTime", String.valueOf(remoteResultSetInitTime));
        }
        return stats;
    }

    private void openMcqaScanner() throws IOException {
        try {
            SQLExecutorBuilder builder = SQLExecutorBuilder.builder()
                    .odps(odps)
                    .executeMode(ExecuteMode.INTERACTIVE)
                    .fallbackPolicy(FallbackPolicy.alwaysFallbackPolicy())
                    .useInstanceTunnel(false)
                    .enableOdpsNamespaceSchema(namespaceSchemaEnabled)
                    .logviewVersion(LOGVIEW_V2);
            if (!Strings.isNullOrEmpty(mcqaQuota)) {
                builder.quotaName(mcqaQuota);
            }
            mcqaExecutor = builder.build();

            Map<String, String> hints = new HashMap<>();
            if (namespaceSchemaEnabled) {
                hints.put("odps.namespace.schema", "true");
                hints.put("odps.sql.allow.namespace.schema", "true");
            }

            long runStart = System.nanoTime();
            mcqaExecutor.run(remoteQuerySql, hints);
            remoteSubmitTime += System.nanoTime() - runStart;
            long submitMs = nanosToMillis(remoteSubmitTime);
            LOG.info("MaxComputeJniScanner MCQA submitted: project=" + remoteQueryProject
                    + ", table=" + table + ", quota=" + mcqaQuota
                    + ", submitMs=" + submitMs + ", sql=" + remoteQuerySql
                    + ", queryId=" + mcqaExecutor.getQueryId());

            long resultSetInitStart = System.nanoTime();
            remoteResultSet = mcqaExecutor.getResultSet();
            remoteResultSetInitTime += System.nanoTime() - resultSetInitStart;
            long resultSetInitMs = nanosToMillis(remoteResultSetInitTime);

            LOG.info("MaxComputeJniScanner opened MCQA mode: project=" + remoteQueryProject
                    + ", table=" + table + ", quota=" + mcqaQuota
                    + ", queryId=" + mcqaExecutor.getQueryId()
                    + ", submitMs=" + submitMs
                    + ", resultSetInitMs=" + resultSetInitMs
                    + ", logview=" + safeGenerateMcqaLogView()
                    + ", executionLog=" + mcqaExecutor.getExecutionLog()
                    + ", sql=" + remoteQuerySql);
        } catch (Exception e) {
            String errorMsg = "MaxComputeJniScanner failed to open MCQA mode.";
            LOG.warn(errorMsg, e);
            close();
            throw new IOException(errorMsg, e);
        }
    }

    private int readMcqaRecords(int expectedRows) {
        if (remoteResultSet == null) {
            return 0;
        }
        int curReadRows = 0;
        long startTime = System.nanoTime();
        while (curReadRows < expectedRows && remoteResultSet.hasNext()) {
            Record record = remoteResultSet.next();
            for (int i = 0; i < fields.length; i++) {
                appendData(i, new ResultSetColumnValue(record.get(i), types[i], timeZone));
            }
            curReadRows++;
        }
        long batchNanos = System.nanoTime() - startTime;
        appendDataTime += batchNanos;
        remoteRowsRead += curReadRows;
        if (!remoteFirstBatchLogged && curReadRows > 0) {
            remoteFirstBatchTime = batchNanos;
            remoteFirstBatchLogged = true;
            LOG.info("MaxComputeJniScanner MCQA first batch: project=" + remoteQueryProject
                    + ", table=" + table + ", rows=" + curReadRows
                    + ", batchMs=" + nanosToMillis(batchNanos));
        }
        if (curReadRows == 0) {
            LOG.info("MaxComputeJniScanner MCQA finished: project=" + remoteQueryProject
                    + ", table=" + table + ", totalRows=" + remoteRowsRead
                    + ", submitMs=" + nanosToMillis(remoteSubmitTime)
                    + ", resultSetInitMs=" + nanosToMillis(remoteResultSetInitTime)
                    + ", firstBatchMs=" + nanosToMillis(remoteFirstBatchTime));
        }
        return curReadRows;
    }

    private String safeGenerateMcqaLogView() {
        try {
            return mcqaExecutor != null ? mcqaExecutor.getLogView() : "";
        } catch (Exception e) {
            LOG.warn("Failed to generate MaxCompute MCQA LogView", e);
            return "";
        }
    }

    private static boolean isMcqaPayload(String scanSerializer) {
        return !Strings.isNullOrEmpty(scanSerializer) && scanSerializer.startsWith(MCQA_PAYLOAD_PREFIX);
    }

    private static QueryPayload decodeQueryPayload(String scanSerializer, String payloadPrefix) {
        String encodedPayload = scanSerializer.substring(payloadPrefix.length());
        int separatorIndex = encodedPayload.indexOf(':');
        if (separatorIndex <= 0 || separatorIndex >= encodedPayload.length() - 1) {
            throw new IllegalArgumentException("Invalid MaxCompute remote query payload");
        }
        String encodedProject = encodedPayload.substring(0, separatorIndex);
        String encodedSql = encodedPayload.substring(separatorIndex + 1);
        return new QueryPayload(
                new String(Base64.getDecoder().decode(encodedProject), StandardCharsets.UTF_8),
                new String(Base64.getDecoder().decode(encodedSql), StandardCharsets.UTF_8));
    }

    private static Object deserialize(String serializedString) throws IOException, ClassNotFoundException {
        byte[] serializedBytes = Base64.getDecoder().decode(serializedString);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return objectInputStream.readObject();
    }

    private static long nanosToMillis(long nanos) {
        return nanos / 1_000_000L;
    }

    private static final class QueryPayload {
        private final String project;
        private final String query;

        private QueryPayload(String project, String query) {
            this.project = project;
            this.query = query;
        }
    }

    private static final class ResultSetColumnValue implements ColumnValue {
        private final Object value;
        private final ColumnType columnType;
        private final ZoneId timeZone;

        private ResultSetColumnValue(Object value, ColumnType columnType, ZoneId timeZone) {
            this.value = value;
            this.columnType = columnType;
            this.timeZone = timeZone;
        }

        @Override
        public boolean canGetStringAsBytes() {
            return value instanceof String || value instanceof byte[];
        }

        @Override
        public boolean isNull() {
            return value == null;
        }

        @Override
        public boolean getBoolean() {
            if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            return (Boolean) value;
        }

        @Override
        public byte getByte() {
            return asNumber().byteValue();
        }

        @Override
        public short getShort() {
            return asNumber().shortValue();
        }

        @Override
        public int getInt() {
            return asNumber().intValue();
        }

        @Override
        public float getFloat() {
            return asNumber().floatValue();
        }

        @Override
        public long getLong() {
            return asNumber().longValue();
        }

        @Override
        public double getDouble() {
            return asNumber().doubleValue();
        }

        @Override
        public BigInteger getBigInteger() {
            if (value instanceof BigInteger) {
                return (BigInteger) value;
            }
            if (value instanceof BigDecimal) {
                return ((BigDecimal) value).toBigInteger();
            }
            return BigInteger.valueOf(asNumber().longValue());
        }

        @Override
        public BigDecimal getDecimal() {
            if (value instanceof BigDecimal) {
                return (BigDecimal) value;
            }
            if (value instanceof BigInteger) {
                return new BigDecimal((BigInteger) value);
            }
            Number number = asNumber();
            if (number instanceof Byte || number instanceof Short
                    || number instanceof Integer || number instanceof Long) {
                return BigDecimal.valueOf(number.longValue());
            }
            return BigDecimal.valueOf(number.doubleValue());
        }

        @Override
        public String getString() {
            if (value instanceof byte[]) {
                return new String((byte[]) value, StandardCharsets.UTF_8);
            }
            return String.valueOf(value);
        }

        @Override
        public byte[] getStringAsBytes() {
            if (value instanceof byte[]) {
                return (byte[]) value;
            }
            return getString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public LocalDate getDate() {
            if (value instanceof LocalDate) {
                return (LocalDate) value;
            }
            if (value instanceof java.sql.Date) {
                return ((java.sql.Date) value).toLocalDate();
            }
            if (value instanceof ZonedDateTime) {
                return ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalDate();
            }
            if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).toLocalDate();
            }
            return asInstant().atZone(timeZone).toLocalDate();
        }

        @Override
        public LocalDateTime getDateTime() {
            if (value instanceof LocalDateTime) {
                return (LocalDateTime) value;
            }
            if (value instanceof ZonedDateTime) {
                return ((ZonedDateTime) value).withZoneSameInstant(timeZone).toLocalDateTime();
            }
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime();
            }
            return LocalDateTime.ofInstant(asInstant(), timeZone);
        }

        @Override
        public LocalDateTime getTimeStampTz() {
            return getDateTime();
        }

        @Override
        public byte[] getBytes() {
            return (byte[]) value;
        }

        @Override
        public void unpackArray(List<ColumnValue> values) {
            ColumnType childType = columnType.getChildTypes().get(0);
            Object arrayObject = value;
            try {
                if (arrayObject instanceof String) {
                    arrayObject = new ComplexValueParser((String) arrayObject).parseArray(childType);
                }
                if (arrayObject instanceof java.sql.Array) {
                    arrayObject = ((java.sql.Array) arrayObject).getArray();
                }
                if (arrayObject instanceof List<?>) {
                    for (Object element : (List<?>) arrayObject) {
                        values.add(new ResultSetColumnValue(element, childType, timeZone));
                    }
                    return;
                }
                int length = java.lang.reflect.Array.getLength(arrayObject);
                for (int i = 0; i < length; i++) {
                    values.add(new ResultSetColumnValue(java.lang.reflect.Array.get(arrayObject, i),
                            childType, timeZone));
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to unpack array value", e);
            }
        }

        @Override
        public void unpackMap(List<ColumnValue> keys, List<ColumnValue> values) {
            Object mapObject = value;
            if (mapObject instanceof String) {
                mapObject = new ComplexValueParser((String) mapObject).parseMap(
                        columnType.getChildTypes().get(0), columnType.getChildTypes().get(1));
            }
            Map<?, ?> map = (Map<?, ?>) mapObject;
            ColumnType keyType = columnType.getChildTypes().get(0);
            ColumnType valueType = columnType.getChildTypes().get(1);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                keys.add(new ResultSetColumnValue(entry.getKey(), keyType, timeZone));
                values.add(new ResultSetColumnValue(entry.getValue(), valueType, timeZone));
            }
        }

        @Override
        public void unpackStruct(List<Integer> structFieldIndex, List<ColumnValue> values) {
            List<ColumnType> childTypes = columnType.getChildTypes();
            try {
                if (value instanceof String) {
                    List<Object> fieldValues = new ComplexValueParser((String) value).parseStruct(columnType);
                    for (Integer fieldIndex : structFieldIndex) {
                        values.add(new ResultSetColumnValue(fieldValues.get(fieldIndex),
                                childTypes.get(fieldIndex), timeZone));
                    }
                    return;
                }
                if (value instanceof List<?>) {
                    List<?> fieldValues = (List<?>) value;
                    for (Integer fieldIndex : structFieldIndex) {
                        values.add(new ResultSetColumnValue(fieldValues.get(fieldIndex),
                                childTypes.get(fieldIndex), timeZone));
                    }
                    return;
                }
                if (value instanceof Struct) {
                    Struct struct = (Struct) value;
                    for (Integer fieldIndex : structFieldIndex) {
                        values.add(new ResultSetColumnValue(struct.getFieldValue(fieldIndex),
                                childTypes.get(fieldIndex), timeZone));
                    }
                    return;
                }
                if (value instanceof java.sql.Struct) {
                    Object[] attributes = ((java.sql.Struct) value).getAttributes();
                    for (Integer fieldIndex : structFieldIndex) {
                        values.add(new ResultSetColumnValue(attributes[fieldIndex],
                                childTypes.get(fieldIndex), timeZone));
                    }
                    return;
                }
                throw new IllegalStateException("Unsupported struct value type: " + value.getClass().getName());
            } catch (Exception e) {
                throw new IllegalStateException("Failed to unpack struct value", e);
            }
        }

        private Number asNumber() {
            if (value instanceof String) {
                return new BigDecimal((String) value);
            }
            return (Number) value;
        }

        private Instant asInstant() {
            if (value instanceof Instant) {
                return (Instant) value;
            }
            if (value instanceof java.util.Date) {
                return ((java.util.Date) value).toInstant();
            }
            if (value instanceof CharSequence) {
                String text = normalizeTemporalText(value.toString());
                try {
                    return OffsetDateTime.parse(text).toInstant();
                } catch (DateTimeParseException e) {
                    // fall through
                }
                try {
                    return ZonedDateTime.parse(text).toInstant();
                } catch (DateTimeParseException e) {
                    // fall through
                }
                try {
                    return Instant.parse(text);
                } catch (DateTimeParseException e) {
                    // fall through
                }
                try {
                    return LocalDateTime.parse(text).atZone(timeZone).toInstant();
                } catch (DateTimeParseException e) {
                    // fall through
                }
                try {
                    return LocalDate.parse(text).atStartOfDay(timeZone).toInstant();
                } catch (DateTimeParseException e) {
                    throw new IllegalStateException("Unsupported temporal string value: " + text, e);
                }
            }
            throw new IllegalStateException("Unsupported temporal value type: " + value.getClass().getName());
        }

        private String normalizeTemporalText(String text) {
            String normalized = text.trim();
            if (normalized.length() > 10 && normalized.charAt(10) == ' ') {
                return normalized.substring(0, 10) + "T" + normalized.substring(11);
            }
            return normalized;
        }
    }

    private static final class ComplexValueParser {
        private final String text;
        private int position;

        private ComplexValueParser(String text) {
            this.text = text == null ? "" : text;
        }

        private List<Object> parseArray(ColumnType childType) {
            skipLeadingSpaces();
            expect('[');
            List<Object> values = new ArrayList<>();
            skipLeadingSpaces();
            if (consumeIf(']')) {
                return values;
            }
            while (true) {
                values.add(parseValue(childType, ',', ']'));
                skipLeadingSpaces();
                if (consumeIf(',')) {
                    skipLeadingSpaces();
                    continue;
                }
                expect(']');
                return values;
            }
        }

        private Map<Object, Object> parseMap(ColumnType keyType, ColumnType valueType) {
            skipLeadingSpaces();
            expect('{');
            Map<Object, Object> values = new LinkedHashMap<>();
            skipLeadingSpaces();
            if (consumeIf('}')) {
                return values;
            }
            while (true) {
                Object key = parseValue(keyType, ':');
                expect(':');
                Object value = parseValue(valueType, ',', '}');
                values.put(key, value);
                skipLeadingSpaces();
                if (consumeIf(',')) {
                    skipLeadingSpaces();
                    continue;
                }
                expect('}');
                return values;
            }
        }

        private List<Object> parseStruct(ColumnType structType) {
            skipLeadingSpaces();
            expect('{');
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < structType.getChildTypes().size(); i++) {
                values.add(null);
            }
            skipLeadingSpaces();
            if (consumeIf('}')) {
                return values;
            }
            while (true) {
                String fieldName = asString(parseScalar(':'));
                int fieldIndex = structType.getChildNames().indexOf(fieldName);
                if (fieldIndex < 0) {
                    throw new IllegalStateException("Unknown struct field '" + fieldName + "' in value: " + text);
                }
                expect(':');
                values.set(fieldIndex, parseValue(structType.getChildTypes().get(fieldIndex), ',', '}'));
                skipLeadingSpaces();
                if (consumeIf(',')) {
                    skipLeadingSpaces();
                    continue;
                }
                expect('}');
                return values;
            }
        }

        private Object parseValue(ColumnType type, char... terminators) {
            skipLeadingSpaces();
            if (isNullToken(terminators)) {
                position += 4;
                return null;
            }
            if (type.isArray()) {
                return parseArray(type.getChildTypes().get(0));
            }
            if (type.isMap()) {
                return parseMap(type.getChildTypes().get(0), type.getChildTypes().get(1));
            }
            if (type.isStruct()) {
                return parseStruct(type);
            }
            return parseScalar(terminators);
        }

        private Object parseScalar(char... terminators) {
            int start = position;
            while (position < text.length() && !isTerminator(text.charAt(position), terminators)) {
                position++;
            }
            return text.substring(start, position);
        }

        private boolean isNullToken(char... terminators) {
            if (!text.regionMatches(true, position, "NULL", 0, 4)) {
                return false;
            }
            int end = position + 4;
            return end >= text.length() || isTerminator(text.charAt(end), terminators);
        }

        private boolean isTerminator(char current, char... terminators) {
            for (char terminator : terminators) {
                if (current == terminator) {
                    return true;
                }
            }
            return false;
        }

        private void skipLeadingSpaces() {
            while (position < text.length() && text.charAt(position) == ' ') {
                position++;
            }
        }

        private boolean consumeIf(char expected) {
            if (position < text.length() && text.charAt(position) == expected) {
                position++;
                return true;
            }
            return false;
        }

        private void expect(char expected) {
            if (!consumeIf(expected)) {
                throw new IllegalStateException("Expected '" + expected + "' at position "
                        + position + " in value: " + text);
            }
        }

        private String asString(Object value) {
            return value == null ? null : String.valueOf(value);
        }
    }
}
