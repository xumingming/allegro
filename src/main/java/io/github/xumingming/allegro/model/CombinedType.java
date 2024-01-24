package io.github.xumingming.allegro.model;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CombinedType
        implements Comparable<CombinedType>
{
    private List<SimpleType> types;

    public CombinedType(List<SimpleType> types)
    {
        this.types = types;
    }

    public int getCombinedWeight()
    {
        return types.stream().mapToInt(SimpleType::getWeight).sum();
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
        CombinedType that = (CombinedType) o;
        return Objects.equals(types, that.types);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(types);
    }

    @Override
    public int compareTo(CombinedType that)
    {
        return Integer.valueOf(this.getCombinedWeight()).compareTo(that.getCombinedWeight());
    }

    @Override
    public String toString()
    {
        if (!types.isEmpty()) {
            return types.stream().map(SimpleType::getSimpleName).collect(Collectors.joining("_"));
        }
        else {
            return "<global>";
        }
    }
}
