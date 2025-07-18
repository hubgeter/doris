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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.rules.rewrite.NormalizeToSlot;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils.CollectNonWindowedAggFuncs;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * normalize aggregate's group keys and AggregateFunction's child to SlotReference
 * and generate a LogicalProject top on LogicalAggregate to hold to order of aggregate output,
 * since aggregate output's order could change when we do translate.
 * <p>
 * Apply this rule could simplify the processing of enforce and translate.
 * <pre>
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, K2#2 + 1],
 *   outputs:[k1#1, Alias(K2# + 1)#4, Alias(k1#1 + 1)#5, Alias(SUM(v1#3))#6,
 *            Alias(SUM(v1#3 + 1))#7, Alias(SUM(v1#3) + 1)#8])
 * </pre>
 * After rule:
 * <pre>
 * Project(k1#1, Alias(SR#9)#4, Alias(k1#1 + 1)#5, Alias(SR#10))#6, Alias(SR#11))#7, Alias(SR#10 + 1)#8)
 * +-- Aggregate(keys:[k1#1, SR#9], outputs:[k1#1, SR#9, Alias(SUM(v1#3))#10, Alias(SUM(v1#3 + 1))#11])
 *   +-- Project(k1#1, Alias(K2#2 + 1)#9, v1#3)
 * </pre>
 * Note: window function will be moved to upper project
 * all agg functions except the top agg should be pushed to Aggregate node.
 * example 1:
 * <pre>
 *    select min(x), sum(x) over () ...
 * the 'sum(x)' is top agg of window function, it should be moved to upper project
 * plan:
 *    project(sum(x) over())
 *        Aggregate(min(x), x)
 * </pre>
 * example 2:
 * <pre>
 *    select min(x), avg(sum(x)) over() ...
 * the 'sum(x)' should be moved to Aggregate
 * plan:
 *    project(avg(y) over())
 *         Aggregate(min(x), sum(x) as y)
 * </pre>
 * example 3:
 * <pre>
 *    select sum(x+1), x+1, sum(x+1) over() ...
 * window function should use x instead of x+1
 * plan:
 *    project(sum(x+1) over())
 *        Agg(sum(y), x)
 *            project(x+1 as y)
 * </pre>
 * More example could get from UT {NormalizeAggregateTest}
 */
public class NormalizeAggregate implements RewriteRuleFactory, NormalizeToSlot {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalHaving(logicalAggregate()
                        .whenNot(LogicalAggregate::isNormalized))
                        .thenApply(ctx -> normalizeAgg(ctx.root.child(), Optional.of(ctx.root), ctx.cascadesContext))
                        .toRule(RuleType.NORMALIZE_AGGREGATE),
                logicalAggregate()
                        .whenNot(LogicalAggregate::isNormalized)
                        .thenApply(ctx -> normalizeAgg(ctx.root, Optional.empty(), ctx.cascadesContext))
                        .toRule(RuleType.NORMALIZE_AGGREGATE));
    }

    @SuppressWarnings("checkstyle:UnusedLocalVariable")
    private LogicalPlan normalizeAgg(LogicalAggregate<Plan> aggregate, Optional<LogicalHaving<?>> having,
            CascadesContext ctx) {
        // The LogicalAggregate node may contain window agg functions and usual agg functions
        // we call window agg functions as window-agg and usual agg functions as trivial-agg for short
        // This rule simplify LogicalAggregate node by:
        // 1. Push down some exprs from old LogicalAggregate node to a new child LogicalProject Node,
        // 2. create a new LogicalAggregate with normalized group by exprs and trivial-aggs
        // 3. Pull up normalized old LogicalAggregate's output exprs to a new parent LogicalProject Node
        // Push down exprs:
        // 1. all group by exprs
        // 2. child contains subquery expr in trivial-agg
        // 3. child contains window expr in trivial-agg
        // 4. all input slots of trivial-agg
        // 5. expr(including subquery) in distinct trivial-agg
        // Normalize LogicalAggregate's output.
        // 1. normalize group by exprs by outputs of bottom LogicalProject
        // 2. normalize trivial-aggs by outputs of bottom LogicalProject
        // 3. build normalized agg outputs
        // Pull up exprs:
        // normalize all output exprs in old LogicalAggregate to build a parent project node, typically includes:
        // 1. simple slots
        // 2. aliases
        //    a. alias with no aggs child
        //    b. alias with trivial-agg child
        //    c. alias with window-agg

        // Push down exprs:
        // collect group by exprs
        Set<Expression> groupingByExprs = Utils.fastToImmutableSet(aggregate.getGroupByExpressions());

        // collect all trivial-agg
        List<NamedExpression> aggregateOutput = aggregate.getOutputExpressions();
        List<AggregateFunction> aggFuncs = CollectNonWindowedAggFuncs.collect(aggregateOutput);

        // split agg child as two part
        // TRUE part 1: need push down itself, if it contains subquery or window expression
        // FALSE part 2: need push down its input slots, if it DOES NOT contain subquery or window expression
        ImmutableSet.Builder<Expression> needPushDownSelfExprs = ImmutableSet.builder();
        ImmutableSet.Builder<Expression> needPushDownInputs = ImmutableSet.builder();
        for (AggregateFunction aggFunc : aggFuncs) {
            if (!aggFunc.isDistinct()) {
                for (Expression arg : aggFunc.children()) {
                    // should not push down literal under aggregate
                    // e.g. group_concat(distinct xxx, ','), the ',' literal show stay in aggregate
                    if (arg instanceof Literal) {
                        continue;
                    }
                    if (arg.containsType(SubqueryExpr.class, WindowExpression.class)) {
                        needPushDownSelfExprs.add(arg);
                    } else {
                        needPushDownInputs.add(arg);
                    }
                }
            } else {
                for (Expression arg : aggFunc.children()) {
                    // should not push down literal under aggregate
                    // e.g. group_concat(distinct xxx, ','), the ',' literal show stay in aggregate
                    if (arg instanceof Literal) {
                        continue;
                    }

                    Collection<? extends Expression> inputSlots
                            = arg instanceof OrderExpression ? arg.getInputSlots() : ImmutableList.of(arg);
                    for (Expression input : inputSlots) {
                        if (input instanceof SlotReference) {
                            needPushDownInputs.add(input);
                        } else {
                            needPushDownSelfExprs.add(input);
                        }
                    }
                }
            }
        }

        Set<Expression> needPushSelf = needPushDownSelfExprs.build();
        Set<Slot> needPushInputSlots = ExpressionUtils.getInputSlotSet(needPushDownInputs.build());

        Set<Alias> existsAlias =
                ExpressionUtils.mutableCollect(aggregateOutput, Alias.class::isInstance);

        // push down 3 kinds of exprs, these pushed exprs will be used to normalize agg output later
        // 1. group by exprs
        // 2. trivialAgg children
        // 3. trivialAgg input slots
        // We need to distinguish between expressions in aggregate function arguments and group by expressions.
        NormalizeToSlotContext groupByExprContext = NormalizeToSlotContext.buildContext(existsAlias, groupingByExprs);
        Set<Alias> existsAliasAndGroupByAlias = getExistsAlias(existsAlias, groupByExprContext.getNormalizeToSlotMap());
        Set<Expression> argsOfAggFuncNeedPushDown = Sets.union(needPushSelf, needPushInputSlots);
        NormalizeToSlotContext argsOfAggFuncNeedPushDownContext = NormalizeToSlotContext
                .buildContext(existsAliasAndGroupByAlias, argsOfAggFuncNeedPushDown);
        NormalizeToSlotContext bottomSlotContext = argsOfAggFuncNeedPushDownContext.mergeContext(groupByExprContext);

        Set<NamedExpression> pushedGroupByExprs =
                bottomSlotContext.pushDownToNamedExpression(groupingByExprs);
        Set<NamedExpression> pushedTrivialAggChildren =
                bottomSlotContext.pushDownToNamedExpression(needPushSelf);
        Set<NamedExpression> pushedTrivialAggInputSlots =
                bottomSlotContext.pushDownToNamedExpression(needPushInputSlots);
        Set<NamedExpression> bottomProjects = Sets.union(pushedGroupByExprs,
                Sets.union(pushedTrivialAggChildren, pushedTrivialAggInputSlots));

        // use group by context to normalize agg functions to process
        //   sql like: select sum(a + 1) from t group by a + 1
        //
        // before normalize:
        // agg(output: sum(a[#0] + 1)[#2], group_by: alias(a + 1)[#1])
        // +-- project(a[#0], (a[#0] + 1)[#1])
        //
        // after normalize:
        // agg(output: sum(alias(a + 1)[#1])[#2], group_by: alias(a + 1)[#1])
        // +-- project((a[#0] + 1)[#1])

        // normalize group by exprs by bottomProjects
        List<Expression> normalizedGroupExprs =
                bottomSlotContext.normalizeToUseSlotRef(groupingByExprs);

        // normalize trivial-aggs by bottomProjects
        List<AggregateFunction> normalizedAggFuncs =
                bottomSlotContext.normalizeToUseSlotRef(aggFuncs);
        if (normalizedAggFuncs.stream().anyMatch(agg -> !agg.children().isEmpty()
                && agg.child(0).containsType(AggregateFunction.class))) {
            throw new AnalysisException(
                    "aggregate function cannot contain aggregate parameters");
        }

        // build normalized agg output
        NormalizeToSlotContext normalizedAggFuncsToSlotContext =
                NormalizeToSlotContext.buildContext(existsAlias, normalizedAggFuncs);

        // agg output include 2 parts
        // pushedGroupByExprs and normalized agg functions

        ImmutableList.Builder<NamedExpression> normalizedAggOutputBuilder
                = ImmutableList.builderWithExpectedSize(groupingByExprs.size() + normalizedAggFuncs.size());
        for (NamedExpression pushedGroupByExpr : pushedGroupByExprs) {
            normalizedAggOutputBuilder.add(pushedGroupByExpr.toSlot());
        }
        normalizedAggOutputBuilder.addAll(
                normalizedAggFuncsToSlotContext.pushDownToNamedExpression(normalizedAggFuncs)
        );
        // create new agg node
        ImmutableList<NamedExpression> aggOutput = normalizedAggOutputBuilder.build();
        ImmutableList.Builder<NamedExpression> newAggOutputBuilder
                = ImmutableList.builderWithExpectedSize(aggOutput.size());
        for (NamedExpression output : aggOutput) {
            Expression rewrittenExpr = output.rewriteDownShortCircuit(
                    e -> e instanceof MultiDistinction ? ((MultiDistinction) e).withMustUseMultiDistinctAgg(true) : e);
            newAggOutputBuilder.add((NamedExpression) rewrittenExpr);
        }
        ImmutableList<NamedExpression> normalizedAggOutput = newAggOutputBuilder.build();

        // create upper projects by normalize all output exprs in old LogicalAggregate
        // In aggregateOutput, the expressions inside the agg function can be rewritten
        // with expressions in aggregate function arguments and group by expressions,
        // but the ones outside the agg function can only be rewritten with group by expressions.
        // After the above two rewrites are completed, use aggregate output agg functions to rewrite.
        List<NamedExpression> upperProjects = normalizeOutput(aggregateOutput,
                groupByExprContext, argsOfAggFuncNeedPushDownContext, normalizedAggFuncsToSlotContext);

        // verify project used slots are all coming from agg's output
        List<Slot> slotsUsedInUpperProject = collectAllUsedSlots(upperProjects);
        if (!slotsUsedInUpperProject.isEmpty()) {
            Set<ExprId> aggOutputExprIds = new HashSet<>(slotsUsedInUpperProject.size());
            for (NamedExpression expression : normalizedAggOutput) {
                aggOutputExprIds.add(expression.getExprId());
            }
            Set<Slot> missingSlotsInAggregate = new HashSet<>(slotsUsedInUpperProject.size());
            for (Slot slot : slotsUsedInUpperProject) {
                if (!aggOutputExprIds.contains(slot.getExprId()) && !(slot instanceof SlotNotFromChildren)) {
                    missingSlotsInAggregate.add(slot);
                }
            }
            if (!missingSlotsInAggregate.isEmpty()) {
                if (SqlModeHelper.hasOnlyFullGroupBy()) {
                    throw new AnalysisException(String.format("%s not in aggregate's output", missingSlotsInAggregate
                            .stream().map(NamedExpression::getName).collect(Collectors.joining(", "))));
                } else {
                    // for any slots missing in aggregate's output, we should add a any_value(slot) into
                    // aggregate's output list and slot itself into bottom project's output list
                    bottomProjects = Sets.union(bottomProjects, missingSlotsInAggregate);
                    Map<Expression, Expression> replaceMap = Maps.newHashMap();
                    for (Slot slot : missingSlotsInAggregate) {
                        Alias anyValue = new Alias(new AnyValue(slot), slot.getName());
                        replaceMap.put(slot, anyValue.toSlot());
                        newAggOutputBuilder.add(anyValue);
                    }
                    upperProjects = upperProjects.stream()
                            .map(e -> (NamedExpression) ExpressionUtils.replace(e, replaceMap))
                            .collect(ImmutableList.toImmutableList());
                }
            }
        }
        // create normalized plan
        Plan bottomPlan;
        if (!bottomProjects.isEmpty()) {
            bottomPlan = new LogicalProject<>(ImmutableList.copyOf(bottomProjects), aggregate.child());
        } else {
            bottomPlan = aggregate.child();
        }
        // NOTICE: we must call newAggOutputBuilder.build() here, newAggOutputBuilder could be updated if we need
        //  to process non-standard aggregate: SELECT c1, c2 FROM t GROUP BY c1
        LogicalAggregate<?> newAggregate =
                aggregate.withNormalized(normalizedGroupExprs, newAggOutputBuilder.build(), bottomPlan);
        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx);
        LogicalProject<Plan> project = eliminateGroupByConstant(groupByExprContext, rewriteContext,
                normalizedGroupExprs, normalizedAggOutput, bottomProjects, aggregate, upperProjects, newAggregate);

        if (having.isPresent()) {
            Set<Slot> havingUsedSlots = having.get().getInputSlots();
            Set<Slot> aggOutputExprIds = newAggregate.getOutputSet();
            if (aggOutputExprIds.containsAll(havingUsedSlots)) {
                // when having just use output slots from agg, we push down having as parent of agg
                return project.withChildren(ImmutableList.of(
                        new LogicalHaving<>(
                                ExpressionUtils.replace(having.get().getConjuncts(), project.getAliasToProducer()),
                                project.child()
                        )));
            } else {
                return (LogicalPlan) having.get().withChildren(project);
            }
        } else {
            return project;
        }
    }

    private List<NamedExpression> normalizeOutput(List<NamedExpression> aggregateOutput,
            NormalizeToSlotContext groupByToSlotContext, NormalizeToSlotContext argsOfAggFuncNeedPushDownContext,
            NormalizeToSlotContext normalizedAggFuncsToSlotContext) {
        // build upper project, use two context to do pop up, because agg output maybe contain two part:
        // group by keys and agg expressions
        List<NamedExpression> upperProjects = new ArrayList<>();
        for (Expression expr : aggregateOutput) {
            Expression rewrittenExpr = expr.rewriteDownShortCircuit(
                    e -> normalizeAggFuncChildren(
                            argsOfAggFuncNeedPushDownContext, e));
            upperProjects.add((NamedExpression) rewrittenExpr);
        }
        upperProjects = groupByToSlotContext.normalizeToUseSlotRefWithoutWindowFunction(upperProjects);
        upperProjects = normalizedAggFuncsToSlotContext.normalizeToUseSlotRefWithoutWindowFunction(upperProjects);

        Builder<NamedExpression> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < aggregateOutput.size(); i++) {
            NamedExpression e = upperProjects.get(i);
            // process Expression like Alias(SlotReference#0)#0
            if (e instanceof Alias && e.child(0) instanceof SlotReference) {
                SlotReference slotReference = (SlotReference) e.child(0);
                if (slotReference.getExprId().equals(e.getExprId())) {
                    e = slotReference;
                }
            }
            // Make the output ExprId unchanged
            if (!e.getExprId().equals(aggregateOutput.get(i).getExprId())) {
                e = new Alias(aggregateOutput.get(i).getExprId(), e, aggregateOutput.get(i).getName());
            }
            builder.add(e);
        }
        return builder.build();
    }

    private List<Slot> collectAllUsedSlots(List<NamedExpression> expressions) {
        Set<Slot> inputSlots = ExpressionUtils.getInputSlotSet(expressions);
        List<SubqueryExpr> subqueries = ExpressionUtils.collectAll(expressions, SubqueryExpr.class::isInstance);
        List<Slot> slots = new ArrayList<>(inputSlots.size() + subqueries.size());
        for (SubqueryExpr subqueryExpr : subqueries) {
            slots.addAll(subqueryExpr.getCorrelateSlots());
        }
        slots.addAll(ExpressionUtils.getInputSlotSet(expressions));
        return slots;
    }

    private Set<Alias> getExistsAlias(Set<Alias> originAliases,
            Map<Expression, NormalizeToSlotTriplet> groupingExprMap) {
        Set<Alias> existsAlias = Sets.newHashSet();
        existsAlias.addAll(originAliases);
        for (NormalizeToSlotTriplet triplet : groupingExprMap.values()) {
            if (triplet.pushedExpr instanceof Alias) {
                Alias alias = (Alias) triplet.pushedExpr;
                existsAlias.add(alias);
            }
        }
        return existsAlias;
    }

    private Expression normalizeAggFuncChildren(NormalizeToSlotContext context, Expression expr) {
        if (expr instanceof AggregateFunction) {
            AggregateFunction function = (AggregateFunction) expr;
            List<Expression> normalizedRealExpressions = context.normalizeToUseSlotRef(function.getArguments());
            function = function.withChildren(normalizedRealExpressions);
            return function;
        } else {
            return expr;
        }
    }

    private LogicalProject<Plan> eliminateGroupByConstant(NormalizeToSlotContext groupByExprContext,
            ExpressionRewriteContext rewriteContext, List<Expression> normalizedGroupExprs,
            List<NamedExpression> normalizedAggOutput, Set<NamedExpression> bottomProjects,
            LogicalAggregate<Plan> aggregate, List<NamedExpression> upperProjects, LogicalAggregate<?> newAggregate) {
        // 1. Find the expressions in group by that can be folded into constants and build a map(slot, literal)
        Map<Expression, NormalizeToSlotTriplet> replaceMap = groupByExprContext.getNormalizeToSlotMap();
        if (replaceMap.isEmpty()) {
            return new LogicalProject<>(upperProjects, newAggregate);
        }
        Map<Slot, Expression> slotToLiteral = new HashMap<>();
        for (Map.Entry<Expression, NormalizeToSlotTriplet> entry : replaceMap.entrySet()) {
            Expression foldExpression = FoldConstantRuleOnFE.evaluate(entry.getKey(), rewriteContext);
            if (foldExpression.isConstant()) {
                slotToLiteral.put(entry.getValue().remainExpr, foldExpression);
            }
        }
        if (slotToLiteral.isEmpty()) {
            return new LogicalProject<>(upperProjects, newAggregate);
        }
        // 2. Regenerate a group by list without constant key
        List<Expression> newNormalizedGroupExprs = new ArrayList<>();
        for (Expression normalizedGroupExpr : normalizedGroupExprs) {
            if (!slotToLiteral.containsKey((Slot) normalizedGroupExpr)) {
                newNormalizedGroupExprs.add(normalizedGroupExpr);
            }
        }
        if (newNormalizedGroupExprs.size() == normalizedGroupExprs.size()) {
            return new LogicalProject<>(upperProjects, newAggregate);
        }
        if (newNormalizedGroupExprs.isEmpty()) {
            Alias tinyInt = new Alias(new TinyIntLiteral((byte) 1));
            bottomProjects = new HashSet<>(bottomProjects);
            bottomProjects.add(tinyInt);
            normalizedAggOutput = new ArrayList<>(normalizedAggOutput);
            Slot tinyIntSlot = tinyInt.toSlot();
            normalizedAggOutput.add(tinyIntSlot);
            newNormalizedGroupExprs.add(tinyIntSlot);
        }
        // 3. Replace the agg output expression and delete the constant group by key in the output
        ImmutableList.Builder<NamedExpression> nonConstAggOutput = ImmutableList.builder();
        for (NamedExpression ne : normalizedAggOutput) {
            if (ne instanceof Alias) {
                nonConstAggOutput.add(ExpressionUtils.replaceNameExpression(ne, slotToLiteral));
                continue;
            } else if (ne instanceof Slot) {
                if (!slotToLiteral.containsKey(ne)) {
                    nonConstAggOutput.add(ne);
                }
                continue;
            }
            nonConstAggOutput.add(ne);
        }

        // 4. The constant expression calculation in bottom projects needs to be deleted
        // and put into upperProjects for calculation
        Plan bottomPlan;
        if (!bottomProjects.isEmpty()) {
            ImmutableList.Builder<NamedExpression> builder = ImmutableList.builder();
            for (NamedExpression bottomProject : bottomProjects) {
                if (!slotToLiteral.containsKey(bottomProject.toSlot())) {
                    builder.add(bottomProject);
                }
            }
            bottomPlan = new LogicalProject<>(builder.build(), aggregate.child());
        } else {
            bottomPlan = aggregate.child();
        }
        LogicalAggregate<Plan> newAggAfterEliminate = aggregate.withNormalized(newNormalizedGroupExprs,
                nonConstAggOutput.build(), bottomPlan);
        // 5. This upperProjects needs to add the constant key that was deleted in the group by key
        // and change the reference to the constant key to a constant expression
        ImmutableList.Builder<NamedExpression> newUpperProjects = ImmutableList.builder();
        for (NamedExpression upperProject : upperProjects) {
            if (upperProject instanceof Alias) {
                newUpperProjects.add(ExpressionUtils.replaceNameExpression(upperProject, slotToLiteral));
                continue;
            } else if (upperProject instanceof Slot) {
                if (slotToLiteral.containsKey(upperProject)) {
                    Alias newLiteral = new Alias(upperProject.getExprId(), slotToLiteral.get(upperProject),
                            upperProject.getName());
                    newUpperProjects.add(newLiteral);
                    continue;
                }
            }
            newUpperProjects.add(upperProject);
        }
        return new LogicalProject<>(newUpperProjects.build(), newAggAfterEliminate);
    }
}
