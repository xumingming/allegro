package io.github.xumingming.allegro.model.logical;

import com.google.common.collect.ImmutableList;
import io.github.xumingming.allegro.model.CombinedType;
import io.github.xumingming.allegro.model.SimpleType;

import java.util.List;
import java.util.stream.Collectors;

public class JoinNode
        extends PlanNode
{
    private PlanNode left;
    private PlanNode right;
    private JoinType joinType;

    private List<CriteriaItem> criteria;
    private String filter;
    private List<String> outputSymbols;
    private List<String> leftOutputSymbols;
    private List<String> rightOutputSymbols;

    public PlanNode getLeft()
    {
        return left;
    }

    public void setLeft(PlanNode left)
    {
        this.left = left;
    }

    public PlanNode getRight()
    {
        return right;
    }

    public void setRight(PlanNode right)
    {
        this.right = right;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    public List<CriteriaItem> getCriteria()
    {
        return criteria;
    }

    public void setCriteria(List<CriteriaItem> criteria)
    {
        this.criteria = criteria;
    }

    public CombinedType getJoinKeyType()
    {
        return new CombinedType(getCriteria()
                .stream()
                .map(criteriaItem -> criteriaItem.getLeftType())
                .map(x -> SimpleType.valueOf(x))
                .collect(Collectors.toList()));
    }

    public PlanNode getProbe()
    {
        return getLeft();
    }

    public PlanNode getBuild()
    {
        return getRight();
    }

    public JoinType getJoinType()
    {
        return joinType;
    }

    public void setJoinType(JoinType joinType)
    {
        this.joinType = joinType;
    }

    public String getFilter()
    {
        return filter;
    }

    public void setFilter(String filter)
    {
        this.filter = filter;
    }

    public boolean hasFilter()
    {
        return this.filter != null;
    }

    public List<String> getOutputSymbols()
    {
        return outputSymbols;
    }

    public void setOutputSymbols(List<String> outputSymbols)
    {
        this.outputSymbols = outputSymbols;
    }

    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        ANTI,
        SEMI
    }

    public static class CriteriaItem
    {
        private String left;
        private String leftType;
        private String right;
        private String rightType;

        public CriteriaItem(String left, String right)
        {
            this.left = left;
            this.right = right;
        }

        public CriteriaItem(String left, String leftType, String right, String rightType)
        {
            this.left = left;
            this.right = right;
        }

        public String getLeft()
        {
            return left;
        }

        public void setLeft(String left)
        {
            this.left = left;
        }

        public String getLeftType()
        {
            return leftType;
        }

        public void setLeftType(String leftType)
        {
            this.leftType = leftType;
        }

        public String getRight()
        {
            return right;
        }

        public void setRight(String right)
        {
            this.right = right;
        }

        public String getRightType()
        {
            return rightType;
        }

        public void setRightType(String rightType)
        {
            this.rightType = rightType;
        }
    }
}
