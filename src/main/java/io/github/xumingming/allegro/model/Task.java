package io.github.xumingming.allegro.model;

import io.airlift.units.DataSize;

import java.time.Duration;
import java.util.List;

public class Task
{
    private String taskId;
    private Duration elapsedTime;
    private List<Operator> operators;
    private DataSize peakMemory;
    private DataSize rawInputDataSize;

    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    public void setElapsedTime(Duration elapsedTime)
    {
        this.elapsedTime = elapsedTime;
    }

    public List<Operator> getOperators()
    {
        return operators;
    }

    public void setOperators(List<Operator> operators)
    {
        this.operators = operators;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public void setTaskId(String taskId)
    {
        this.taskId = taskId;
    }

    public DataSize getPeakMemory()
    {
        return peakMemory;
    }

    public void setPeakMemory(DataSize peakMemory)
    {
        this.peakMemory = peakMemory;
    }

    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    public void setRawInputDataSize(DataSize rawInputDataSize)
    {
        this.rawInputDataSize = rawInputDataSize;
    }

    public Duration getWallTime()
    {
        Duration result = Duration.ZERO;

        for (Operator operator : operators) {
            result = result.plus(operator.getWallTime());
        }

        return result;
    }
}
