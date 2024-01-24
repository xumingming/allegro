package io.github.xumingming.allegro.model;

import io.airlift.units.DataSize;
import io.github.xumingming.allegro.model.logical.LogicalPlan;
import io.github.xumingming.allegro.model.logical.PlanNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Stage
{
    private String stageId;
    private Duration wallTime;

    private DataSize peakMemoryReservation;
    private List<Task> tasks;
    private List<Stage> subStages;
    private LogicalPlan logicalPlan;

    public String getStageId()
    {
        return stageId;
    }

    public void setStageId(String stageId)
    {
        this.stageId = stageId;
    }

    public DataSize getPeakMemoryReservation()
    {
        return peakMemoryReservation;
    }

    public void setPeakMemoryReservation(DataSize peakMemoryReservation)
    {
        this.peakMemoryReservation = peakMemoryReservation;
    }

    public Duration getElapsedTime()
    {
        return tasks.stream().map(Task::getElapsedTime).max(Comparator.naturalOrder()).get();
    }

    public List<Task> getTasks()
    {
        return tasks;
    }

    public void setTasks(List<Task> tasks)
    {
        this.tasks = tasks;
    }

    public List<Stage> getSubStages()
    {
        return subStages;
    }

    public void setSubStages(List<Stage> subStages)
    {
        this.subStages = subStages;
    }

    public Duration getWallTime()
    {
        return wallTime;
    }

    public void setWallTime(Duration wallTime)
    {
        this.wallTime = wallTime;
    }

    public LogicalPlan getLogicalPlan()
    {
        return logicalPlan;
    }

    public void setLogicalPlan(LogicalPlan logicalPlan)
    {
        this.logicalPlan = logicalPlan;
    }

    public String simpleId()
    {
        return stageId.substring(stageId.lastIndexOf(".") + 1);
    }

    public Map<String, PlanNode> getPlanNodesAsMap()
    {
        return logicalPlan.getPlanNodesAsMap();
    }

    public List<PlanNode> getLeafPlanNodes()
    {
        Map<String, PlanNode> planNodeMap = getPlanNodesAsMap();
        return planNodeMap.values().stream().filter(x -> !x.hasDownstream()).collect(Collectors.toList());
    }

    public List<Operator> getOperators()
    {
        List<Operator> operators = new ArrayList<>();

        // HACK.
        Task sampleTask = tasks.get(0);
        if (sampleTask.getOperators() == null && tasks.size() >= 2) {
            sampleTask = tasks.get(1);
        }

        if (sampleTask.getOperators() == null && tasks.size() >= 3) {
            sampleTask = tasks.get(2);
        }

        int operatorCount = sampleTask.getOperators().size();
        for (int i = 0; i < operatorCount; i++) {
            Operator sampleOperator = sampleTask.getOperators().get(i);
            Operator mergedOperator = Operator.of(
                    simpleId(),
                    sampleOperator.getPlanNodeId(),
                    sampleOperator.getOperatorType());
            mergedOperator.merge(sampleOperator);
            for (int j = 1; j < tasks.size(); j++) {
                mergedOperator.merge(tasks.get(j).getOperators().get(i));
            }
            operators.add(mergedOperator);
        }
        return operators;
    }
}
