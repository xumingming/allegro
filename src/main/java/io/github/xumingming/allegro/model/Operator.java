package io.github.xumingming.allegro.model;

import io.airlift.units.DataSize;

import java.time.Duration;
import java.util.Objects;

import static java.lang.String.format;

public class Operator
{
    private OperatorType operatorType;
    private String planNodeId;
    private String stageId;
    private DataSize inputDataSize;
    private long inputPosition;
    private DataSize outputDataSize;
    private long outputPosition;

    private DataSize peakMemory;

    private Duration addInputCpu;
    private Duration getOutputCpu;
    private Duration finishCpu;

    private Duration addInputWall;
    private Duration getOutputWall;
    private Duration finishWall;
    private int totalDrivers;

    public static Operator of(String stageId, String planNodeId, OperatorType operatorType)
    {
        Operator operator = new Operator();
        operator.setStageId(stageId);
        operator.setPlanNodeId(planNodeId);
        operator.setOperatorType(operatorType);

        operator.setPeakMemory(DataSize.valueOf("0B"));

        operator.setAddInputCpu(Duration.ZERO);
        operator.setGetOutputCpu(Duration.ZERO);
        operator.setFinishCpu(Duration.ZERO);

        operator.setAddInputWall(Duration.ZERO);
        operator.setGetOutputWall(Duration.ZERO);
        operator.setFinishWall(Duration.ZERO);

        return operator;
    }

    private static DataSize dataSizeAdd(DataSize ds1, DataSize ds2)
    {
        if (ds1 == null && ds2 == null) {
            return DataSize.succinctBytes(0);
        }

        if (ds1 == null) {
            return ds2;
        }

        if (ds2 == null) {
            return ds1;
        }

        return DataSize.succinctBytes(ds1.toBytes() + ds2.toBytes());
    }

    public void merge(Operator that)
    {
        this.inputPosition += that.inputPosition;
        this.inputDataSize = dataSizeAdd(this.inputDataSize, that.inputDataSize);

        this.outputPosition += that.outputPosition;
        this.outputDataSize = dataSizeAdd(this.outputDataSize, that.outputDataSize);

        this.addInputCpu = this.addInputCpu.plus(that.addInputCpu);
        this.getOutputCpu = this.getOutputCpu.plus(that.getOutputCpu);
        this.finishCpu = this.finishCpu.plus(that.finishCpu);

        this.addInputWall = this.addInputWall.plus(that.addInputWall);
        this.getOutputWall = this.getOutputWall.plus(that.getOutputWall);
        this.finishWall = this.finishWall.plus(that.finishWall);

        this.totalDrivers += that.totalDrivers;
    }

    public String getPlanNodeId()
    {
        return planNodeId;
    }

    public void setPlanNodeId(String planNodeId)
    {
        this.planNodeId = planNodeId;
    }

    public String getRepresentativePlanNodeId()
    {
        if (planNodeId.indexOf(",") < 0) {
            return planNodeId;
        }

        return planNodeId.substring(0, planNodeId.indexOf(","));
    }

    public OperatorType getOperatorType()
    {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType)
    {
        this.operatorType = operatorType;
    }

    public long getInputPosition()
    {
        return inputPosition;
    }

    public void setInputPosition(long inputPosition)
    {
        this.inputPosition = inputPosition;
    }

    public long getOutputPosition()
    {
        return outputPosition;
    }

    public void setOutputPosition(long outputPosition)
    {
        this.outputPosition = outputPosition;
    }

    public double getSelectivity()
    {
        return outputPosition * 1.0 / inputPosition;
    }

    public DataSize getPeakMemory()
    {
        return peakMemory;
    }

    public void setPeakMemory(DataSize peakMemory)
    {
        this.peakMemory = peakMemory;
    }

    public Duration getAddInputWall()
    {
        return addInputWall;
    }

    public void setAddInputWall(Duration addInputWall)
    {
        this.addInputWall = addInputWall;
    }

    public Duration getGetOutputWall()
    {
        return getOutputWall;
    }

    public void setGetOutputWall(Duration getOutputWall)
    {
        this.getOutputWall = getOutputWall;
    }

    public Duration getFinishWall()
    {
        return finishWall;
    }

    public void setFinishWall(Duration finishWall)
    {
        this.finishWall = finishWall;
    }

    public Duration getWallTime()
    {
        return addInputWall.plus(getOutputWall).plus(finishWall);
    }

    public Duration getCpuTime()
    {
        return addInputCpu.plus(getOutputCpu).plus(finishCpu);
    }

    public Duration getTableScanCpuTime()
    {
        return getCpuTime();
    }

    public Duration getAvgCpuTime()
    {
        return getTotalDrivers() == 0 ? Duration.ZERO : getCpuTime().dividedBy(getTotalDrivers());
    }

    public String getStageId()
    {
        return stageId;
    }

    public void setStageId(String stageId)
    {
        this.stageId = stageId;
    }

    public DataSize getInputDataSize()
    {
        return inputDataSize;
    }

    public void setInputDataSize(DataSize inputDataSize)
    {
        this.inputDataSize = inputDataSize;
    }

    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    public void setOutputDataSize(DataSize outputDataSize)
    {
        this.outputDataSize = outputDataSize;
    }

    @Override
    public String toString()
    {
        return "Operator{" +
                "operatorType=" + operatorType +
                ", planNodeId='" + planNodeId + '\'' +
                ", stageId='" + stageId + '\'' +
                ", inputDataSize=" + inputDataSize +
                ", inputPosition=" + inputPosition +
                ", outputDataSize=" + outputDataSize +
                ", outputPosition=" + outputPosition +
                ", peakMemory=" + peakMemory +
                ", addInputWall=" + addInputWall +
                ", getOutputWall=" + getOutputWall +
                ", finishWall=" + finishWall +
                '}';
    }

    public String toPrettyString()
    {
        switch (operatorType) {
            case TableScan:
                return format("TableScan[(#%s): %s]", getPlanNodeId(), getOutputPosition());
            default:
                return format("%s(#%s): %s", operatorType, getPlanNodeId(), getOutputPosition());
        }
    }

    public Duration getAddInputCpu()
    {
        return addInputCpu;
    }

    public void setAddInputCpu(Duration addInputCpu)
    {
        this.addInputCpu = addInputCpu;
    }

    public Duration getGetOutputCpu()
    {
        return getOutputCpu;
    }

    public void setGetOutputCpu(Duration getOutputCpu)
    {
        this.getOutputCpu = getOutputCpu;
    }

    public Duration getFinishCpu()
    {
        return finishCpu;
    }

    public void setFinishCpu(Duration finishCpu)
    {
        this.finishCpu = finishCpu;
    }

    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    public void setTotalDrivers(int totalDrivers)
    {
        this.totalDrivers = totalDrivers;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Operator operator = (Operator) o;
        return inputPosition == operator.inputPosition
                && outputPosition == operator.outputPosition
                && totalDrivers == operator.totalDrivers
                && operatorType == operator.operatorType
                && Objects.equals(planNodeId, operator.planNodeId)
                && Objects.equals(stageId, operator.stageId)
                && Objects.equals(inputDataSize, operator.inputDataSize)
                && Objects.equals(outputDataSize, operator.outputDataSize)
                && Objects.equals(peakMemory, operator.peakMemory)
                && Objects.equals(addInputCpu, operator.addInputCpu)
                && Objects.equals(getOutputCpu, operator.getOutputCpu)
                && Objects.equals(finishCpu, operator.finishCpu)
                && Objects.equals(addInputWall, operator.addInputWall)
                && Objects.equals(getOutputWall, operator.getOutputWall)
                && Objects.equals(finishWall, operator.finishWall);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operatorType, planNodeId, stageId, inputDataSize,
                inputPosition, outputDataSize, outputPosition, peakMemory,
                addInputCpu, getOutputCpu, finishCpu, addInputWall, getOutputWall,
                finishWall, totalDrivers);
    }
}
