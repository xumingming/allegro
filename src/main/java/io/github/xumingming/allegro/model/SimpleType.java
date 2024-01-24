package io.github.xumingming.allegro.model;

public enum SimpleType
{
    integer("int", 1),
    bigint("long", 2),
    date("date", 3),
    decimal("dec", 4),
    varchar("str", 5);

    private String simpleName;
    private int weight;

    SimpleType(String simpleName, int weight)
    {
        this.simpleName = simpleName;
        this.weight = weight;
    }

    public String getSimpleName()
    {
        return simpleName;
    }

    public int getWeight()
    {
        return weight;
    }
}
