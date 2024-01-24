package io.github.xumingming.allegro;

import io.airlift.units.DataSize;

import java.time.Duration;
import java.util.Optional;

public class Result
{
    private final String runName;
    private final String queryName;
    private final Duration elapsedTime;
    private final Status status;
    private final Optional<String> errorMessage;

    private Duration cpuTime;
    private DataSize peakMemory;

    public Result(String runName, String queryName, Status status, Duration elapsedTime, Optional<String> errorMessage)
    {
        this.runName = runName;
        this.queryName = queryName;
        this.status = status;
        this.elapsedTime = elapsedTime;
        this.errorMessage = errorMessage;
    }

    public static Result success(String runName, String queryName, Duration elapseTime)
    {
        return new Result(runName, queryName, Status.SUCCESS, elapseTime, Optional.empty());
    }

    public static Result failure(String runName, String queryName, Duration elapseTime, String errorMessage)
    {
        return new Result(runName, queryName, Status.FAILURE, elapseTime, Optional.of(errorMessage));
    }

    public String getQueryName()
    {
        return queryName;
    }

    public Status getStatus()
    {
        return status;
    }

    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }

    public String getRunName()
    {
        return runName;
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public void setCpuTime(Duration cpuTime)
    {
        this.cpuTime = cpuTime;
    }

    public DataSize getPeakMemory()
    {
        return peakMemory;
    }

    public void setPeakMemory(DataSize peakMemory)
    {
        this.peakMemory = peakMemory;
    }

    @Override
    public String toString()
    {
        return "Result{" +
                "runName='" + runName + '\'' +
                ", queryName='" + queryName + '\'' +
                ", elapsedTime=" + elapsedTime +
                ", status=" + status +
                ", errorMessage=" + errorMessage +
                ", cpuTime=" + cpuTime +
                ", peakMemory=" + peakMemory +
                '}';
    }

    public enum Status
    {
        SUCCESS,
        FAILURE
    }
}
