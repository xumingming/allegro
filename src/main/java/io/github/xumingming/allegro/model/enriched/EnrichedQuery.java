package io.github.xumingming.allegro.model.enriched;

import io.github.xumingming.allegro.model.Operator;
import io.github.xumingming.allegro.model.OperatorType;
import io.github.xumingming.allegro.model.Query;
import io.github.xumingming.allegro.model.Stage;
import io.github.xumingming.allegro.model.logical.LogicalPlan;
import io.github.xumingming.allegro.model.logical.PlanNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Utils.checkState;

public class EnrichedQuery
{
    private Query query;
    private EnrichedStage rootStage;

    public EnrichedQuery(Query query)
    {
        this.query = query;
        init();
    }

    public void init()
    {
        this.rootStage = enrichStage(query.getRootStage());
    }

    private EnrichedStage enrichStage(Stage stage)
    {
        try {
            List<EnrichedStage> enrichedSubStages = stage.getSubStages().stream().map(this::enrichStage).collect(Collectors.toList());
            return new EnrichedStage(stage, enrichLogicalPlan(stage.getLogicalPlan()), enrichedSubStages);
        }
        catch (Exception e) {
            throw new RuntimeException("enrichStage failed for stage: " + query.getName() + ":" + stage.getStageId(), e);
        }
    }

    private EnrichedLogicalPlan enrichLogicalPlan(LogicalPlan logicalPlan)
    {
        return new EnrichedLogicalPlan(logicalPlan, enrichPlanNode(logicalPlan.getRoot()));
    }

    private EnrichedPlanNode enrichPlanNode(PlanNode planNode)
    {
        List<EnrichedPlanNode> sources = planNode.getSources().stream().map(this::enrichPlanNode).collect(Collectors.toList());
        return new EnrichedPlanNode(
                planNode,
                query.getOperatorsByPlanNodeId(planNode.getId()),
                sources);
    }

    public Query getQuery()
    {
        return query;
    }

    public void setQuery(Query query)
    {
        this.query = query;
    }

    public EnrichedStage getRootStage()
    {
        return rootStage;
    }

    public void setRootStage(EnrichedStage rootStage)
    {
        this.rootStage = rootStage;
    }

    public List<EnrichedStage> getStageList()
    {
        List<EnrichedStage> stages = new ArrayList<>();
        getStageList(stages, rootStage);

        return stages;
    }

    private void getStageList(List<EnrichedStage> stages, EnrichedStage currentStage)
    {
        stages.add(currentStage);
        for (EnrichedStage subStage : currentStage.getSubStages()) {
            getStageList(stages, subStage);
        }
    }

    public EnrichedStage getStageById(int id)
    {
        for (EnrichedStage stage : getStageList()) {
            if (id == Integer.parseInt(stage.getStage().simpleId())) {
                return stage;
            }
        }

        return null;
    }

    public List<Operator> getOperators()
    {
        List<EnrichedStage> stages = getStageList();
        List<Operator> operators = new ArrayList<>();
        for (EnrichedStage stage : stages) {
            operators.addAll(stage.getStage().getOperators());
        }

        return operators;
    }

    public Map<PlanNode.Type, List<EnrichedPlanNode>> getPlanNodesAsMap()
    {
        List<EnrichedStage> stages = getStageList();
        Map<String, EnrichedPlanNode> planNodeId2EnrichedPlanNodes = new HashMap<>();
        for (EnrichedStage stage : stages) {
            planNodeId2EnrichedPlanNodes.putAll(stage.getPlanNodesAsMap());
        }

        Map<PlanNode.Type, List<EnrichedPlanNode>> ret = planNodeId2EnrichedPlanNodes
                .values()
                .stream()
                .collect(
                        Collectors.groupingBy(
                                (EnrichedPlanNode enrichedPlanNode) -> enrichedPlanNode.getPlanNode().getType(),
                                Collectors.mapping(Function.identity(), Collectors.toList())));

        return ret;
    }

    public List<EnrichedPlanNode> getJoins()
    {
        return getPlanNodesAsMap().getOrDefault(PlanNode.Type.Join, new ArrayList<>());
    }

    public List<EnrichedPlanNode> getAggregations()
    {
        return getPlanNodesAsMap().getOrDefault(PlanNode.Type.Aggregation, new ArrayList<>());
    }

    public List<EnrichedPlanNode> getTableScans()
    {
        return getPlanNodesAsMap().getOrDefault(PlanNode.Type.TableScan, new ArrayList<>());
    }

    public Map<OperatorType, Duration> getOperatorCpuTimeDistribution()
    {
        Map<OperatorType, Duration> operatorTimeDistribution = new HashMap<>();
        List<Operator> operators = query.getOperators();
        for (int i = 0; i < operators.size(); i++) {
            Operator operator = operators.get(i);
            operatorTimeDistribution.put(
                    operator.getOperatorType(),
                    operatorTimeDistribution.getOrDefault(operator.getOperatorType(), Duration.ZERO).plus(operator.getCpuTime()));
        }
        return operatorTimeDistribution;
    }

    public Map<OperatorType, Duration> getOperatorWallTimeDistribution()
    {
        Map<OperatorType, Duration> operatorTimeDistribution = new HashMap<>();
        List<Operator> operators = query.getOperators();
        for (int i = 0; i < operators.size(); i++) {
            Operator operator = operators.get(i);
            operatorTimeDistribution.put(
                    operator.getOperatorType(),
                    operatorTimeDistribution.getOrDefault(operator.getOperatorType(), Duration.ZERO).plus(operator.getWallTime()));
        }
        return operatorTimeDistribution;
    }

    public Map<PlanNode.Type, Duration> getOperatorAvgElapseTimeDistribution()
    {
        Map<PlanNode.Type, Duration> operatorTimeDistribution = new HashMap<>();
        List<Operator> operators = query.getOperators();
        for (int i = 0; i < operators.size(); i++) {
            Operator operator = operators.get(i);
            PlanNode planNode = query.getPlanNodeById(operator.getRepresentativePlanNodeId());
            checkState(planNode != null,
                    "There is no planNode with id: " + operator.getRepresentativePlanNodeId()
                            + ", query: " + query.getName());

            int totalDrivers = operator.getTotalDrivers();
            checkState(totalDrivers > 0,
                    "operator: " + operator + " has no driver! "
                            + ", query: " + query.getName());

            operatorTimeDistribution.put(
                    planNode.getType(),
                    operatorTimeDistribution.getOrDefault(planNode.getType(), Duration.ZERO).plus(operator.getCpuTime().dividedBy(totalDrivers)));
        }
        return operatorTimeDistribution;
    }
}
