package io.github.xumingming.allegro.model.enriched;

import io.github.xumingming.allegro.model.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Map;

public class EnrichedLogicalPlan
{
    private LogicalPlan logicalPlan;
    private EnrichedPlanNode root;

    public EnrichedLogicalPlan(LogicalPlan logicalPlan, EnrichedPlanNode root)
    {
        this.logicalPlan = logicalPlan;
        this.root = root;
    }

    public LogicalPlan getLogicalPlan()
    {
        return logicalPlan;
    }

    public void setLogicalPlan(LogicalPlan logicalPlan)
    {
        this.logicalPlan = logicalPlan;
    }

    public EnrichedPlanNode getRoot()
    {
        return root;
    }

    public void setRoot(EnrichedPlanNode root)
    {
        this.root = root;
    }

    public Map<String, EnrichedPlanNode> getPlanNodesAsMap()
    {
        return getPlanNodesAsMap(root);
    }

    private Map<String, EnrichedPlanNode> getPlanNodesAsMap(EnrichedPlanNode planNode)
    {
        Map<String, EnrichedPlanNode> ret = new HashMap<>();
        ret.put(planNode.getPlanNode().getId(), planNode);
        for (EnrichedPlanNode subNode : planNode.getSources()) {
            ret.putAll(getPlanNodesAsMap(subNode));
        }

        return ret;
    }
}
