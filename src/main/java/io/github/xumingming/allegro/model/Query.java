package io.github.xumingming.allegro.model;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.github.xumingming.allegro.model.enriched.EnrichedPlanNode;
import io.github.xumingming.allegro.model.logical.PlanNode;
import io.github.xumingming.allegro.model.logical.RemoteSourceNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class Query
{
    private String name;
    private String id;
    private State state;

    private DataSize rawInputDataSize;
    private DataSize shuffleDataSize;
    private DataSize tableScanDataSize;
    private Duration tableScanTime;

    private Duration elapsedTime;
    private Duration planningTime;
    private Duration executionTime;
    private Duration finishingTime;

    private Duration cpuTime;

    private Stage rootStage;
    private String query;
    private DataSize peakMemory;
    private String errorMessage;

    public String getErrorMessage()
    {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage)
    {
        this.errorMessage = errorMessage;
    }

    public Stage getRootStage()
    {
        return rootStage;
    }

    public void setRootStage(Stage rootStage)
    {
        this.rootStage = rootStage;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    public void setRawInputDataSize(DataSize rawInputDataSize)
    {
        this.rawInputDataSize = rawInputDataSize;
    }

    public DataSize getShuffleDataSize()
    {
        return shuffleDataSize;
    }

    public void setShuffleDataSize(DataSize shuffleDataSize)
    {
        this.shuffleDataSize = shuffleDataSize;
    }

    public DataSize getTableScanDataSize()
    {
        return tableScanDataSize;
    }

    public void setTableScanDataSize(DataSize tableScanDataSize)
    {
        this.tableScanDataSize = tableScanDataSize;
    }

    public Duration getTableScanTime()
    {
        return tableScanTime;
    }

    public void setTableScanTime(Duration tableScanTime)
    {
        this.tableScanTime = tableScanTime;
    }

    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    public void setElapsedTime(Duration elapsedTime)
    {
        this.elapsedTime = elapsedTime;
    }

    public Duration getPlanningTime()
    {
        return planningTime;
    }

    public void setPlanningTime(Duration planningTime)
    {
        this.planningTime = planningTime;
    }

    public Duration getExecutionTime()
    {
        return executionTime;
    }

    public void setExecutionTime(Duration executionTime)
    {
        this.executionTime = executionTime;
    }

    public Duration getFinishingTime()
    {
        return finishingTime;
    }

    public void setFinishingTime(Duration finishingTime)
    {
        this.finishingTime = finishingTime;
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public void setCpuTime(Duration cpuTime)
    {
        this.cpuTime = cpuTime;
    }

    public String getQuery()
    {
        return query;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public DataSize getPeakMemory()
    {
        return peakMemory;
    }

    public void setPeakMemory(DataSize peakMemory)
    {
        this.peakMemory = peakMemory;
    }

    /**
     * Name: /tmp/hello.json SimpleName: hello
     *
     * @return
     */
    public String getSimpleName()
    {
        return name.substring(name.lastIndexOf("/") + 1, name.lastIndexOf("."));
    }

    public List<Stage> getStageList()
    {
        List<Stage> stages = new ArrayList<>();
        getStageList(stages, rootStage);

        return stages;
    }

    private void getStageList(List<Stage> stages, Stage currentStage)
    {
        stages.add(currentStage);
        for (Stage subStage : currentStage.getSubStages()) {
            getStageList(stages, subStage);
        }
    }

    public Stage getStageById(int id)
    {
        for (Stage stage : getStageList()) {
            if (id == Integer.parseInt(stage.simpleId())) {
                return stage;
            }
        }

        return null;
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    public List<Operator> getOperators()
    {
        List<Stage> stages = getStageList();
        List<Operator> operators = new ArrayList<>();
        for (Stage stage : stages) {
            operators.addAll(stage.getOperators());
        }

        return operators;
    }

    private Map<String, List<Operator>> getOperatorsAsMap()
    {
        List<Operator> operators = getOperators();
        Map<String, List<Operator>> ret = new HashMap<>();

        for (Operator operator : operators) {
            String planNodeId = operator.getPlanNodeId();
            ret.putIfAbsent(planNodeId, new ArrayList<>());
            ret.get(planNodeId).add(operator);
        }

        return ret;
    }

    public List<Operator> getOperatorsByPlanNodeId(String planNodeId)
    {
        return getOperatorsAsMap().get(planNodeId);
    }

    /**
     * One PlanNode might correspond to multiple operator, we take the representative one.
     */
    @Deprecated
    public Operator getRepresentativeOperatorByPlanNodeId(String planNodeId)
    {
        List<Operator> operators = getOperatorsByPlanNodeId(planNodeId);
        checkState(operators != null && operators.size() > 0,
                "planNode: " + planNodeId + "(" + getPlanNodesAsMap().get(planNodeId).getType() + ") has no corresponding operator!");
        return operators.get(0);
    }

    public Map<String, PlanNode> getPlanNodesAsMap()
    {
        List<Stage> stages = getStageList();
        Map<String, PlanNode> ret = new HashMap<>();
        for (Stage stage : stages) {
            ret.putAll(stage.getPlanNodesAsMap());
        }

        return ret;
    }

    public PlanNode getPlanNodeById(String planNodeId)
    {
        return getPlanNodesAsMap().get(planNodeId);
    }

    public Map<PlanNode.Type, List<EnrichedPlanNode>> getPlanNodeAndOperatorsAsMap()
    {
        Map<String, List<Operator>> operatorMap = getOperatorsAsMap();
        Map<String, PlanNode> planNodeMap = getPlanNodesAsMap();

        Map<PlanNode.Type, List<EnrichedPlanNode>> ret = new HashMap<>();
        for (Map.Entry<String, PlanNode> entry : planNodeMap.entrySet()) {
            EnrichedPlanNode enrichedPlanNode = new EnrichedPlanNode(
                    entry.getValue(),
                    operatorMap.get(entry.getKey()),
                    ImmutableList.of());
            ret.putIfAbsent(entry.getValue().getType(), new ArrayList<>());
            ret.get(entry.getValue().getType())
                    .add(enrichedPlanNode);
        }

        return ret;
    }

    public boolean hasJoin()
    {
        return getPlanNodeAndOperatorsAsMap().containsKey(PlanNode.Type.Join);
    }

    @Deprecated
    public List<EnrichedPlanNode> getJoins()
    {
        if (state == State.FINISHED) {
            return getPlanNodeAndOperatorsAsMap().getOrDefault(PlanNode.Type.Join, new ArrayList<>());
        }
        return Collections.emptyList();
    }

    public boolean hasAggregation()
    {
        if (state == State.FINISHED) {
            return getPlanNodeAndOperatorsAsMap().containsKey(PlanNode.Type.Aggregation);
        }
        return false;
    }

    @Deprecated
    public List<EnrichedPlanNode> getAggregations()
    {
        if (state == State.FINISHED) {
            return getPlanNodeAndOperatorsAsMap().getOrDefault(PlanNode.Type.Aggregation, new ArrayList<>());
        }

        return Collections.emptyList();
    }

    public boolean hasTableScan()
    {
        if (state == State.FINISHED) {
            return getPlanNodeAndOperatorsAsMap().containsKey(PlanNode.Type.TableScan);
        }

        return false;
    }

    @Deprecated
    public List<EnrichedPlanNode> getTableScans()
    {
        if (state == State.FINISHED) {
            return getPlanNodeAndOperatorsAsMap().getOrDefault(PlanNode.Type.TableScan, new ArrayList<>());
        }

        return Collections.emptyList();
    }

    public void init()
    {
        if (state == State.FINISHED) {
            reconnectPlanNodesBetweenStagesThroughRemoteSourceNode();
        }
    }

    private void reconnectPlanNodesBetweenStagesThroughRemoteSourceNode()
    {
        List<Stage> stages = getStageList();
        for (int i = 0; i < stages.size() - 1; i++) {
            Stage stage = stages.get(i);

            List<PlanNode> leafPlanNodes = stage.getLeafPlanNodes();
            if (!leafPlanNodes.isEmpty()) {
                for (PlanNode node : leafPlanNodes) {
                    if (node.getType() == PlanNode.Type.RemoteSource) {
                        RemoteSourceNode remoteSourceNode = (RemoteSourceNode) node;
                        Stage downstreamStage = this.getStageById(remoteSourceNode.getRemoteFragmentIdsAsInt().get(0));
                        PlanNode remoteDownstreamNode = downstreamStage.getLogicalPlan().getRoot();
                        remoteSourceNode.setSource(remoteDownstreamNode);
                    }
                }
            }
        }
    }
}
