package io.github.xumingming.allegro.model.logical;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class PlanNode
{
    protected String id;
    protected Type type;
    private List<PlanNode> sources;
    private String tableName;

    public String getUnderlineTableName()
    {
        if (this.getTableName() != null) {
            return getTableName();
        }

        if (this.getSources().isEmpty()) {
            return "-";
        }

        String ret = this.getSources().get(0).getUnderlineTableName();
        if (ret != null) {
            return ret;
        }

        if (this.getSources().size() == 1) {
            return "-";
        }

        ret = this.getSources().get(1).getUnderlineTableName();
        if (ret != null) {
            return ret;
        }

        return "-";
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public List<PlanNode> getSources()
    {
        return sources == null ? ImmutableList.of() : sources;
    }

    public void setSources(List<PlanNode> sources)
    {
        this.sources = sources;
    }

    public void setSource(PlanNode source)
    {
        this.sources = ImmutableList.of(source);
    }

    public Type getType()
    {
        return type;
    }

    public void setType(Type type)
    {
        this.type = type;
    }

    public boolean hasDownstream()
    {
        return !getSources().isEmpty();
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
        PlanNode planNode = (PlanNode) o;
        return Objects.equals(id, planNode.id) && type == planNode.type && Objects.equals(sources, planNode.sources) && Objects.equals(tableName, planNode.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, type, sources, tableName);
    }

    @Override
    public String toString()
    {
        return "PlanNode{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", sources=" + sources +
                ", tableName='" + tableName + '\'' +
                '}';
    }

    public enum Type
    {
        Join,
        Aggregation,
        TopN,
        Project,
        TableScan,
        RemoteSource,
        Output,
        Exchange,
        Filter,
        Window,
        Limit,
        Scalar,
        SemiJoin,
        AssignUniqueId,
        MarkDistinct,
        TableWriter,
        TableCommit,
        Sort,
        GroupId,
        EnforceSingleRow
    }
}
