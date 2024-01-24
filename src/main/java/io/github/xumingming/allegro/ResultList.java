package io.github.xumingming.allegro;

import java.util.List;

import static io.github.xumingming.allegro.Utils.checkState;

public class ResultList
{
    private List<Result> results;

    public ResultList(List<Result> results)
    {
        checkState(results != null && results.size() > 0, "results is null or empty!");
        this.results = results;
    }

    public String getQueryName()
    {
        return results.get(0).getQueryName();
    }

    public Result get(int index)
    {
        return results.get(index);
    }

    public double getElapseTimeSpeedup(int index)
    {
        checkState(index > 0, "getElapseTimeSpeedup only support index which is greater than 0.");
        Result baseline = results.get(index - 1);
        Result optimized = results.get(index);
        return baseline.getElapsedTime().toMillis() * 1.0 / optimized.getElapsedTime().toMillis();
    }

    public double getElapseTimeTotalSpeedup(int index)
    {
        checkState(index > 0, "getElapseTimeSpeedup only support index which is greater than 0.");
        Result baseline = results.get(0);
        Result optimized = results.get(index);
        return baseline.getElapsedTime().toMillis() * 1.0 / optimized.getElapsedTime().toMillis();
    }

    public double getCpuTimeSpeedup(int index)
    {
        checkState(index > 0, "getCpuTimeSpeedup only support index which is greater than 0.");
        Result baseline = results.get(index - 1);
        Result optimized = results.get(index);

        if (baseline.getCpuTime() == null || baseline.getCpuTime().toMillis() == 0 || optimized.getCpuTime() == null || optimized.getCpuTime().toMillis() == 0) {
            return 1.0;
        }

        return baseline.getCpuTime().toMillis() * 1.0 / optimized.getCpuTime().toMillis();
    }

    public double getCpuTimeTotalSpeedup(int index)
    {
        checkState(index > 0, "getCpuTimeSpeedup only support index which is greater than 0.");
        Result baseline = results.get(0);
        Result optimized = results.get(index);

        if (baseline.getCpuTime() == null || baseline.getCpuTime().toMillis() == 0 || optimized.getCpuTime() == null || optimized.getCpuTime().toMillis() == 0) {
            return 1.0;
        }

        return baseline.getCpuTime().toMillis() * 1.0 / optimized.getCpuTime().toMillis();
    }

    public double getPeakMemorySpeedup(int index)
    {
        checkState(index > 0, "getPeakMemorySpeedup only support index which is greater than 0.");
        Result baseline = results.get(index - 1);
        Result optimized = results.get(index);

        if (baseline.getCpuTime() == null || baseline.getPeakMemory().toBytes() == 0 || optimized.getCpuTime() == null || optimized.getPeakMemory().toBytes() == 0) {
            return 1.0;
        }

        return baseline.getPeakMemory().toBytes() * 1.0 / optimized.getPeakMemory().toBytes();
    }

    public double getPeakMemoryTotalSpeedup(int index)
    {
        checkState(index > 0, "getPeakMemorySpeedup only support index which is greater than 0.");
        Result baseline = results.get(0);
        Result optimized = results.get(index);

        if (baseline.getCpuTime() == null || baseline.getPeakMemory().toBytes() == 0 || optimized.getCpuTime() == null || optimized.getPeakMemory().toBytes() == 0) {
            return 1.0;
        }

        return baseline.getPeakMemory().toBytes() * 1.0 / optimized.getPeakMemory().toBytes();
    }
}
