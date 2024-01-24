package io.github.xumingming.allegro.model.enriched;

import io.github.xumingming.allegro.model.Operator;
import io.github.xumingming.allegro.model.logical.JoinNode;
import io.github.xumingming.allegro.model.logical.PlanNode;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class EnrichedPlanNode
{
    private PlanNode planNode;
    private List<Operator> operators;
    private List<EnrichedPlanNode> sources;

    public EnrichedPlanNode(PlanNode planNode, List<Operator> operators, List<EnrichedPlanNode> sources)
    {
        this.planNode = planNode;
        this.operators = operators;
        this.sources = sources;
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }

    public void setPlanNode(PlanNode planNode)
    {
        this.planNode = planNode;
    }

    public JoinNode getJoinNode()
    {
        checkState(planNode instanceof JoinNode);
        return (JoinNode) planNode;
    }

    public List<Operator> getOperators()
    {
        return operators;
    }

    public void setOperators(List<Operator> operators)
    {
        this.operators = operators;
    }

    public List<EnrichedPlanNode> getSources()
    {
        return sources;
    }

    public void setSources(List<EnrichedPlanNode> sources)
    {
        this.sources = sources;
    }

    public String getStageId()
    {
        return operators.get(0).getStageId();
    }

    public Operator getOperator()
    {
        return operators.get(0);
    }

    public Operator getProbeOperator()
    {
        checkState(planNode instanceof JoinNode, "Only JoinNode has Probe Operator!");
        return operators.get(0);
    }

    public Operator getBuildOperator()
    {
        checkState(planNode instanceof JoinNode, "Only JoinNode has Build Operator!");
        return operators.get(1);
    }
}
