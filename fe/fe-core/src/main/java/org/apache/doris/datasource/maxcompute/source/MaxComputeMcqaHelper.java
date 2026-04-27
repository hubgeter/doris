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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;

import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class MaxComputeMcqaHelper {
    private final MaxComputeExternalTable table;
    private final MaxComputeExternalCatalog catalog;
    private final List<SlotDescriptor> slotDescriptors;
    private final List<Expr> conjuncts;
    private final long limit;
    private final boolean onlyPartitionEqualityPredicate;

    MaxComputeMcqaHelper(MaxComputeExternalTable table, MaxComputeExternalCatalog catalog,
            List<SlotDescriptor> slotDescriptors, List<Expr> conjuncts, long limit,
            boolean onlyPartitionEqualityPredicate) {
        this.table = table;
        this.catalog = catalog;
        this.slotDescriptors = slotDescriptors;
        this.conjuncts = conjuncts;
        this.limit = limit;
        this.onlyPartitionEqualityPredicate = onlyPartitionEqualityPredicate;
    }

    boolean shouldUseMcqaQueryMode() {
        if (!isMcqaQueryEnabled() || !hasLimit() || !isLimitWithinMcqaThreshold()) {
            return false;
        }
        if (!hasSupportedProjection()) {
            return false;
        }
        if (table.getPartitionColumns().isEmpty()) {
            return conjuncts.isEmpty();
        }
        if (conjuncts.isEmpty() || !onlyPartitionEqualityPredicate) {
            return false;
        }
        try {
            buildMcqaWhereClause();
            return true;
        } catch (AnalysisException e) {
            return false;
        }
    }

    String buildMcqaQuery() throws AnalysisException {
        String selectColumns = getProjectedColumnNames().stream()
                .map(CommonUtils::quoteRef)
                .collect(Collectors.joining(", "));
        String whereClause = buildMcqaWhereClause();
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(selectColumns)
                .append(" FROM ")
                .append(buildMcqaTableName());
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE (").append(whereClause).append(")");
        }
        sql.append(" LIMIT ").append(limit).append(";");
        return sql.toString();
    }

    boolean isMcqaQueryEnabled() {
        return Boolean.parseBoolean(catalog.getProperties().getOrDefault(
                org.apache.doris.common.maxcompute.MCProperties.ENABLE_MCQA_QUERY,
                org.apache.doris.common.maxcompute.MCProperties.DEFAULT_ENABLE_MCQA_QUERY));
    }

    boolean isLimitWithinMcqaThreshold() {
        return limit < catalog.getMcqaQueryLimitThreshold();
    }

    private boolean hasLimit() {
        return limit > -1;
    }

    private boolean hasSupportedProjection() {
        if (slotDescriptors.isEmpty()) {
            return false;
        }
        List<String> projectedColumnNames = getProjectedColumnNames();
        if (projectedColumnNames.isEmpty()) {
            return false;
        }
        Set<String> allColumnNames = table.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        return allColumnNames.containsAll(projectedColumnNames);
    }

    private List<String> getProjectedColumnNames() {
        return slotDescriptors.stream()
                .map(SlotDescriptor::getColumn)
                .filter(column -> column != null)
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    private String buildMcqaTableName() {
        TableIdentifier tableIdentifier = table.getTableIdentifier();
        boolean enableNamespaceSchema = Boolean.parseBoolean(catalog.getProperties().getOrDefault(
                org.apache.doris.common.maxcompute.MCProperties.ENABLE_NAMESPACE_SCHEMA,
                org.apache.doris.common.maxcompute.MCProperties.DEFAULT_ENABLE_NAMESPACE_SCHEMA));
        if (enableNamespaceSchema) {
            return CommonUtils.quoteRef(tableIdentifier.getProject()) + "."
                    + CommonUtils.quoteRef(tableIdentifier.getSchema()) + "."
                    + CommonUtils.quoteRef(tableIdentifier.getTable());
        }
        return CommonUtils.quoteRef(tableIdentifier.getProject()) + "."
                + CommonUtils.quoteRef(tableIdentifier.getTable());
    }

    String buildMcqaWhereClause() throws AnalysisException {
        if (conjuncts.isEmpty()) {
            return "";
        }
        List<String> sqlFilters = new ArrayList<>(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            sqlFilters.add(convertExprToMcqaFilter(conjunct));
        }
        return String.join(") AND (", sqlFilters);
    }

    private String convertExprToMcqaFilter(Expr expr) throws AnalysisException {
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Only EQ partition predicates can be routed to MCQA");
            }
            String columnName = convertSlotRefToColumnName(expr.getChild(0));
            validatePartitionColumn(columnName);
            return CommonUtils.quoteRef(columnName) + " = "
                    + convertPartitionLiteralToMcqaValue(expr.getChild(1));
        }
        if (expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            if (inPredicate.isNotIn()) {
                throw new AnalysisException("NOT IN partition predicates are not supported for MCQA routing");
            }
            String columnName = convertSlotRefToColumnName(expr.getChild(0));
            validatePartitionColumn(columnName);
            List<String> values = new ArrayList<>(inPredicate.getChildren().size() - 1);
            for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                values.add(convertPartitionLiteralToMcqaValue(inPredicate.getChild(i)));
            }
            return CommonUtils.quoteRef(columnName) + " IN (" + String.join(", ", values) + ")";
        }
        throw new AnalysisException("Unsupported predicate " + expr + " for MCQA routing");
    }

    private String convertSlotRefToColumnName(Expr expr) throws AnalysisException {
        if (!(expr instanceof SlotRef)) {
            throw new AnalysisException("Unsupported non-slot expression " + expr + " in MCQA filter");
        }
        SlotRef slotRef = (SlotRef) expr;
        if (slotRef.getColumn() != null) {
            return slotRef.getColumn().getName();
        }
        String columnName = slotRef.getColumnName();
        if (columnName != null) {
            return columnName;
        }
        throw new AnalysisException("SlotRef " + expr + " does not reference a base column");
    }

    private void validatePartitionColumn(String columnName) throws AnalysisException {
        if (!table.getColumnNameToOdpsColumn().containsKey(columnName)) {
            throw new AnalysisException("Column " + columnName + " not found in table " + table.getName());
        }
    }

    private String convertPartitionLiteralToMcqaValue(Expr expr) throws AnalysisException {
        if (!(expr instanceof LiteralExpr)) {
            throw new AnalysisException("Unsupported non-literal expression " + expr + " in MCQA filter");
        }
        return CommonUtils.quoteStr(((LiteralExpr) expr).getStringValue());
    }
}
