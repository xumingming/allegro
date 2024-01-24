package io.github.xumingming.allegro.model.logical;

import com.facebook.presto.common.type.TypeSignature;

import java.util.HashMap;
import java.util.Map;

public class LogicalPlan
{
    private String id;
    private PlanNode root;
    private Map<String, TypeSignature> symbols;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public void setRoot(PlanNode root)
    {
        this.root = root;
    }

    public Map<String, TypeSignature> getSymbols()
    {
        return symbols;
    }

    public void setSymbols(Map<String, TypeSignature> symbols)
    {
        this.symbols = symbols;
    }

    public Map<String, PlanNode> getPlanNodesAsMap()
    {
        return getPlanNodesAsMap(root);
    }

    private Map<String, PlanNode> getPlanNodesAsMap(PlanNode planNode)
    {
        Map<String, PlanNode> ret = new HashMap<>();
        ret.put(planNode.getId(), planNode);
        for (PlanNode subNode : planNode.getSources()) {
            ret.putAll(getPlanNodesAsMap(subNode));
        }

        return ret;
    }
}
