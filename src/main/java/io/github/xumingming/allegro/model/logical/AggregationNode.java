package io.github.xumingming.allegro.model.logical;

import io.github.xumingming.allegro.model.CombinedType;
import io.github.xumingming.allegro.model.SimpleType;

import java.util.Map;
import java.util.stream.Collectors;

public class AggregationNode
        extends PlanNode
{
    private Step step;
    private Map<String, String> groupingKeys;

    public Step getStep()
    {
        return step;
    }

    public void setStep(Step step)
    {
        this.step = step;
    }

    public Map<String, String> getGroupingKeys()
    {
        return groupingKeys;
    }

    public void setGroupingKeys(Map<String, String> groupingKeys)
    {
        this.groupingKeys = groupingKeys;
    }

    public CombinedType getAggregationKeyType()
    {
        return new CombinedType(
                groupingKeys
                        .entrySet()
                        .stream()
                        .map(x -> SimpleType.valueOf(x.getValue()))
                        .collect(Collectors.toList()));
    }

    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }
}
