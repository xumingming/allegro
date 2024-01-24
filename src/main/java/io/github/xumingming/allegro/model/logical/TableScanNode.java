package io.github.xumingming.allegro.model.logical;

public class TableScanNode
        extends PlanNode
{
    private String schemaName;
    private String tableName;

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }
}
