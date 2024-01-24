package io.github.xumingming.allegro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import com.facebook.presto.common.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.github.xumingming.allegro.model.Operator;
import io.github.xumingming.allegro.model.OperatorType;
import io.github.xumingming.allegro.model.Query;
import io.github.xumingming.allegro.model.Stage;
import io.github.xumingming.allegro.model.State;
import io.github.xumingming.allegro.model.Task;
import io.github.xumingming.allegro.model.logical.AggregationNode;
import io.github.xumingming.allegro.model.logical.JoinNode;
import io.github.xumingming.allegro.model.logical.LogicalPlan;
import io.github.xumingming.allegro.model.logical.PlanNode;
import io.github.xumingming.allegro.model.logical.RemoteSourceNode;
import io.github.xumingming.allegro.model.logical.TableScanNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.github.xumingming.allegro.model.OperatorType.Aggregation;
import static io.github.xumingming.allegro.model.OperatorType.FilterAndProject;
import static io.github.xumingming.allegro.model.OperatorType.FilterProject;
import static io.github.xumingming.allegro.model.OperatorType.HashAggregation;
import static io.github.xumingming.allegro.model.OperatorType.HashBuild;
import static io.github.xumingming.allegro.model.OperatorType.HashBuilder;
import static io.github.xumingming.allegro.model.OperatorType.HashProbe;
import static io.github.xumingming.allegro.model.OperatorType.HashSemiJoin;
import static io.github.xumingming.allegro.model.OperatorType.LocalExchange;
import static io.github.xumingming.allegro.model.OperatorType.LocalExchangeSink;
import static io.github.xumingming.allegro.model.OperatorType.LocalExchangeSource;
import static io.github.xumingming.allegro.model.OperatorType.LocalMerge;
import static io.github.xumingming.allegro.model.OperatorType.LocalMergeSource;
import static io.github.xumingming.allegro.model.OperatorType.LocalPartition;
import static io.github.xumingming.allegro.model.OperatorType.LookupJoin;
import static io.github.xumingming.allegro.model.OperatorType.LookupOuter;
import static io.github.xumingming.allegro.model.OperatorType.Merge;
import static io.github.xumingming.allegro.model.OperatorType.MergeExchange;
import static io.github.xumingming.allegro.model.OperatorType.NestedLoopBuild;
import static io.github.xumingming.allegro.model.OperatorType.NestedLoopJoin;
import static io.github.xumingming.allegro.model.OperatorType.NestedLoopJoinBuild;
import static io.github.xumingming.allegro.model.OperatorType.NestedLoopJoinProbe;
import static io.github.xumingming.allegro.model.OperatorType.PartialAggregation;
import static io.github.xumingming.allegro.model.OperatorType.ScanFilterAndProject;
import static io.github.xumingming.allegro.model.OperatorType.SetBuilder;
import static io.github.xumingming.allegro.model.OperatorType.TableScan;
import static io.github.xumingming.beauty.Beauty.drawError;
import static java.lang.String.format;

public class PlanParser
{
    private static final Map<OperatorType, OperatorType> OPERATOR_TYPE_MAPPING = new HashMap<>();

    private PlanParser()
    {
    }

    public static Query parse(Context context, String plan)
    {
        try {
            JSONObject planJson = JSON.parseObject(plan);
            Query query = new Query();
            // $
            query.setId(planJson.getString("queryId"));
            query.setQuery(planJson.getString("query"));
            query.setState(State.valueOf(planJson.getString("state")));

            if (query.getState() == State.FAILED) {
                // $.failureInfo
                JSONObject failureInfo = planJson.getJSONObject("failureInfo");
                query.setErrorMessage(failureInfo.getString("message"));
            }

            // $.outputStage
            JSONObject rootStageJson = planJson.getJSONObject("outputStage");
            query.setRootStage(parseStage(context, rootStageJson));

            // $.queryStats
            JSONObject queryStatsJson = planJson.getJSONObject("queryStats");
            query.setRawInputDataSize(DataSize.valueOf(queryStatsJson.getString("rawInputDataSize")));

            query.setElapsedTime(parseDuration(queryStatsJson.getString("elapsedTime")));
            query.setPlanningTime(parseDuration(queryStatsJson.getString("totalPlanningTime")));
            query.setExecutionTime(parseDuration(queryStatsJson.getString("executionTime")));
            query.setFinishingTime(parseDuration(safeGetDurationAsString(queryStatsJson, "finishingTime")));
            query.setCpuTime(parseDuration(queryStatsJson.getString("totalCpuTime")));
            query.setPeakMemory(DataSize.valueOf(queryStatsJson.getString("peakTotalMemoryReservation")));

            return query;
        }
        catch (RuntimeException e) {
            drawError("parse " + context.getQueryName() + " failed!");
            throw e;
        }
    }

    private static String safeGetDurationAsString(JSONObject json, String name)
    {
        return json.containsKey(name) ? json.getString(name) : "0s";
    }

    private static Duration parseDuration(String str)
    {
        io.airlift.units.Duration airliftDuration = io.airlift.units.Duration.valueOf(str);
        return Duration.ofMillis(airliftDuration.toMillis());
    }

    private static Stage parseStage(Context context, JSONObject stageJson)
    {
        Stage stage = new Stage();
        checkState(stageJson != null, "stageJson is null for query: " + context.getQueryName());
        stage.setStageId(stageJson.getString("stageId"));
        try {
            // $.stageStats
            JSONObject latestAttemptExecutionInfo = stageJson.getJSONObject("latestAttemptExecutionInfo");
            JSONObject stageStatsJson = latestAttemptExecutionInfo.getJSONObject("stats");
            stage.setPeakMemoryReservation(DataSize.valueOf(stageStatsJson.getString("peakUserMemoryReservation")));
            stage.setTasks(parseTasks(context, latestAttemptExecutionInfo.getJSONArray("tasks")));

            // $.plan
            LogicalPlan logicalPlan = parseLogicalPlan(stageJson.getJSONObject("plan"));
            stage.setLogicalPlan(logicalPlan);

            List<Stage> subStages = new ArrayList<>();

            // $.subStages
            JSONArray subStagesArr = stageJson.getJSONArray("subStages");
            for (int i = 0; i < subStagesArr.size(); i++) {
                subStages.add(parseStage(context, subStagesArr.getJSONObject(i)));
            }
            stage.setSubStages(subStages);

            return stage;
        }
        catch (Exception e) {
            throw new RuntimeException(format("parse query: %s, stage: %s failed!", context.getQueryName(), stage.getStageId()), e);
        }
    }

    private static LogicalPlan parseLogicalPlan(JSONObject logicalPlanJson)
    {
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.setId(logicalPlanJson.getString("id"));
        logicalPlan.setRoot(parsePlanNode(logicalPlanJson.getJSONObject("root")));
        JSONArray variablesArray = logicalPlanJson.getJSONArray("variables");
        Map<String, TypeSignature> symbols = new HashMap<>();
        for (int i = 0; i < variablesArray.size(); i++) {
            JSONObject variable = variablesArray.getJSONObject(i);
            symbols.put(variable.getString("name"), TypeSignature.parseTypeSignature(variable.getString("type")));
        }
        logicalPlan.setSymbols(symbols);

        return logicalPlan;
    }

    private static List<String> parseOutputVariables(JSONArray outputVariablesArray)
    {
        List<String> ret = new ArrayList<>();
        for (int i = 0; i < outputVariablesArray.size(); i++) {
            ret.add(outputVariablesArray.getJSONObject(i).getString("name"));
        }

        return ret;
    }

    public static PlanNode parsePlanNode(JSONObject planNodeJson)
    {
        PlanNode planNode = null;
        String planTypeStr = planNodeJson.getString("@type");
        planTypeStr = planTypeStr.substring(planTypeStr.lastIndexOf(".") + 1);
        if (planTypeStr.endsWith("Node")) {
            planTypeStr = planTypeStr.substring(0, planTypeStr.length() - 4);
        }
        if (planTypeStr == null) {
            throw new IllegalArgumentException("Invalid planNode: " + planNodeJson.toJSONString());
        }
        PlanNode.Type type = PlanNode.Type.valueOf(planTypeStr);
        switch (type) {
            case Join:
                planNode = new JoinNode();
                JoinNode joinNode = (JoinNode) planNode;
                joinNode.setLeft(parsePlanNode(planNodeJson.getJSONObject("left")));
                joinNode.setRight(parsePlanNode(planNodeJson.getJSONObject("right")));
                joinNode.setJoinType(JoinNode.JoinType.valueOf(planNodeJson.getString("type")));
                if (planNodeJson.containsKey("filter")) {
                    joinNode.setFilter(planNodeJson.getString("filter"));
                }

                joinNode.setOutputSymbols(parseOutputVariables(planNodeJson.getJSONArray("outputVariables")));

                List<JoinNode.CriteriaItem> criteria = new ArrayList<>();
                JSONArray criteriaJsonArr = planNodeJson.getJSONArray("criteria");
                for (int i = 0; i < criteriaJsonArr.size(); i++) {
                    JSONObject criteriaItemJson = criteriaJsonArr.getJSONObject(i);
                    criteria.add(new JoinNode.CriteriaItem(
                            criteriaItemJson.getJSONObject("left").getString("name"),
                            criteriaItemJson.getJSONObject("left").getString("type"),
                            criteriaItemJson.getJSONObject("right").getString("name"),
                            criteriaItemJson.getJSONObject("right").getString("type")));
                }
                joinNode.setCriteria(criteria);
                joinNode.setSources(ImmutableList.of(joinNode.getLeft(), joinNode.getRight()));
                break;
            case Aggregation:
                planNode = new AggregationNode();
                AggregationNode aggregationNode = (AggregationNode) planNode;
                aggregationNode.setStep(AggregationNode.Step.valueOf(planNodeJson.getString("step")));
                // $.groupingSet
                JSONObject groupingSetJson = planNodeJson.getJSONObject("groupingSets");
                // $.groupingSet.groupingKeys
                JSONArray groupingKeysArr = groupingSetJson.getJSONArray("groupingKeys");
                Map<String, String> groupingKeys = new HashMap<>();
                for (int i = 0; i < groupingKeysArr.size(); i++) {
                    groupingKeys.put(groupingKeysArr.getJSONObject(i).getString("name"), groupingKeysArr.getJSONObject(i).getString("type"));
                }
                aggregationNode.setGroupingKeys(groupingKeys);
                break;
            case TableScan:
                planNode = new TableScanNode();
                TableScanNode tableScanNode = (TableScanNode) planNode;
                JSONObject connectorHandleJson = planNodeJson.getJSONObject("table").getJSONObject("connectorHandle");
                tableScanNode.setSchemaName(connectorHandleJson.getString("schema"));
                tableScanNode.setTableName(connectorHandleJson.getString("tableName"));
                break;
            case RemoteSource:
                planNode = new RemoteSourceNode();
                RemoteSourceNode remoteSourceNode = (RemoteSourceNode) planNode;
                JSONArray sourceFragmentIdsJsonArray = planNodeJson.getJSONArray("sourceFragmentIds");
                List<String> sourceFragmentIds = new ArrayList<>();
                for (int i = 0; i < sourceFragmentIdsJsonArray.size(); i++) {
                    sourceFragmentIds.add(sourceFragmentIdsJsonArray.getString(i));
                }
                remoteSourceNode.setRemoteFragmentIds(sourceFragmentIds);
                break;
            default:
                planNode = new PlanNode();
                break;
        }

        planNode.setId(planNodeJson.getString("id"));
        planNode.setType(type);

        if (planNodeJson.containsKey("source")) {
            if (planNodeJson.containsKey("filter")) {
                planNode.setSources(ImmutableList.of(parsePlanNode(planNodeJson.getJSONObject("source")), parsePlanNode(planNodeJson.getJSONObject("filter"))));
            }
            else if (planNodeJson.containsKey("filteringSource")) {
                // semijoin
                planNode.setSources(ImmutableList.of(parsePlanNode(planNodeJson.getJSONObject("source")), parsePlanNode(planNodeJson.getJSONObject("filteringSource"))));
            }
            else {
                planNode.setSource(parsePlanNode(planNodeJson.getJSONObject("source")));
            }
        }
        else if (planNodeJson.containsKey("sources")) {
            JSONArray sourcesJsonArray = planNodeJson.getJSONArray("sources");
            List<PlanNode> sources = new ArrayList<>();
            for (int i = 0; i < sourcesJsonArray.size(); i++) {
                JSONObject sourceJson = sourcesJsonArray.getJSONObject(i);
                sources.add(parsePlanNode(sourceJson));
            }
            planNode.setSources(sources);
        }

        return planNode;
    }

    public static List<Task> parseTasks(Context context, JSONArray tasks)
    {
        List<Task> ret = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            JSONObject taskJson = tasks.getJSONObject(i);
            ret.add(parseTask(context, taskJson));
        }

        return ret;
    }

    private static Task parseTask(Context context, JSONObject taskJson)
    {
        Task task = new Task();

        // $.stats
        JSONObject statsJson = taskJson.getJSONObject("stats");

        if (statsJson != null) {
            task.setElapsedTime(Duration.ofNanos(statsJson.getLong("elapsedTimeInNanos")));
            JSONArray pipelines = statsJson.getJSONArray("pipelines");

            task.setOperators(parseOperators(context, pipelines));
            task.setPeakMemory(DataSize.succinctBytes(statsJson.getLong("peakTotalMemoryInBytes")));
            task.setRawInputDataSize(DataSize.succinctBytes(statsJson.getLong("rawInputDataSizeInBytes")));
        }

        // $.taskStatus
        JSONObject taskStatusJson = taskJson.getJSONObject("taskStatus");
        task.setTaskId(taskStatusJson.getString("taskId"));
        return task;
    }

    public static List<Operator> parseOperators(Context context, JSONArray pipelinesArray)
    {
        List<Operator> ret = new ArrayList<>();
        for (int i = 0; i < pipelinesArray.size(); i++) {
            JSONObject pipelineJson = pipelinesArray.getJSONObject(i);

            JSONArray operatorSummariesArray = pipelineJson.getJSONArray("operatorSummaries");
            for (int j = 0; j < operatorSummariesArray.size(); j++) {
                JSONObject operatorSummaryJson = operatorSummariesArray.getJSONObject(j);
                Operator operator = new Operator();

                operator.setOperatorType(toOperatorType(operatorSummaryJson.getString("operatorType")));
                if (context.normalizeOperatorType) {
                    operator.setOperatorType(normalizeOperatorType(operator.getOperatorType()));
                }
                operator.setPlanNodeId(operatorSummaryJson.getString("planNodeId"));

                operator.setInputPosition(operatorSummaryJson.getLong("inputPositions"));
                operator.setInputDataSize(DataSize.valueOf(operatorSummaryJson.getString("inputDataSize")));

                operator.setOutputPosition(operatorSummaryJson.getLong("outputPositions"));
                operator.setOutputDataSize(DataSize.valueOf(operatorSummaryJson.getString("outputDataSize")));

                operator.setPeakMemory(DataSize.valueOf(operatorSummaryJson.getString("peakTotalMemoryReservation")));

                operator.setAddInputCpu(parseDuration(operatorSummaryJson.getString("addInputCpu")));
                operator.setGetOutputCpu(parseDuration(operatorSummaryJson.getString("getOutputCpu")));
                operator.setFinishCpu(parseDuration(operatorSummaryJson.getString("finishCpu")));

                operator.setAddInputWall(parseDuration(operatorSummaryJson.getString("addInputWall")));
                operator.setGetOutputWall(parseDuration(operatorSummaryJson.getString("getOutputWall")));
                operator.setFinishWall(parseDuration(operatorSummaryJson.getString("finishWall")));

                operator.setTotalDrivers(operatorSummaryJson.getInteger("totalDrivers"));

                ret.add(operator);
            }
        }

        return ret;
    }

    /**
     * Normalize operatorType so we can compare operator's metrics between vanilla presto and native presto.
     */
    private static OperatorType normalizeOperatorType(OperatorType operatorType)
    {
        if (OPERATOR_TYPE_MAPPING.containsKey(operatorType)) {
            return OPERATOR_TYPE_MAPPING.get(operatorType);
        }

        return operatorType;
    }

    private static OperatorType toOperatorType(String operatorTypeStr)
    {
        if (operatorTypeStr.endsWith("Operator")) {
            operatorTypeStr = operatorTypeStr.substring(0, operatorTypeStr.length() - "Operator".length());
        }

        return OperatorType.valueOf(operatorTypeStr);
    }

    public static class Context
    {
        private String queryName;
        private boolean normalizeOperatorType;

        public static Context of(String queryName, boolean normalizeOperatorType)
        {
            Context context = new Context();
            context.queryName = queryName;
            context.normalizeOperatorType = normalizeOperatorType;

            return context;
        }

        public String getQueryName()
        {
            return queryName;
        }

        public boolean isNormalizeOperatorType()
        {
            return normalizeOperatorType;
        }
    }

    static {
        OPERATOR_TYPE_MAPPING.put(FilterAndProject, ScanFilterAndProject);
        OPERATOR_TYPE_MAPPING.put(FilterProject, ScanFilterAndProject);
        OPERATOR_TYPE_MAPPING.put(TableScan, ScanFilterAndProject);

        // Aggregation.
        OPERATOR_TYPE_MAPPING.put(PartialAggregation, Aggregation);
        OPERATOR_TYPE_MAPPING.put(HashAggregation, Aggregation);

        // Join.
        OPERATOR_TYPE_MAPPING.put(HashBuild, HashBuilder);
        OPERATOR_TYPE_MAPPING.put(NestedLoopJoinBuild, HashBuilder);
        OPERATOR_TYPE_MAPPING.put(NestedLoopBuild, HashBuilder);
        OPERATOR_TYPE_MAPPING.put(NestedLoopJoinBuild, HashBuilder);
        OPERATOR_TYPE_MAPPING.put(SetBuilder, HashBuilder);

        OPERATOR_TYPE_MAPPING.put(NestedLoopJoin, LookupJoin);
        OPERATOR_TYPE_MAPPING.put(LookupOuter, LookupJoin);
        OPERATOR_TYPE_MAPPING.put(NestedLoopJoinBuild, HashBuilder);
        OPERATOR_TYPE_MAPPING.put(HashSemiJoin, LookupJoin);
        OPERATOR_TYPE_MAPPING.put(NestedLoopJoinProbe, LookupJoin);
        OPERATOR_TYPE_MAPPING.put(HashProbe, LookupJoin);

        // Exchange
        OPERATOR_TYPE_MAPPING.put(LocalExchange, LocalExchangeSource);
        OPERATOR_TYPE_MAPPING.put(LocalPartition, LocalExchangeSink);
        OPERATOR_TYPE_MAPPING.put(LocalMerge, LocalMergeSource);
        OPERATOR_TYPE_MAPPING.put(MergeExchange, Merge);
    }

    static {
        ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
    }
}
