package io.github.xumingming.allegro;

import java.util.Comparator;
import java.util.function.Function;

public class ComparingStringByNumberPart<T>
        implements Comparator<T>
{
    private Function<T, String> stringSupplier;

    public ComparingStringByNumberPart(Function<T, String> stringSupplier)
    {
        this.stringSupplier = stringSupplier;
    }

    public static long extractNumberFromString(String str)
    {
        return Long.parseLong(str.replaceAll("[^0-9]", ""));
    }

    @Override
    public int compare(T t1, T t2)
    {
        if (t1 == null) {
            return -1;
        }

        if (t2 == null) {
            return 1;
        }

        return Long.valueOf(extractNumberFromString(stringSupplier.apply(t1))).compareTo(extractNumberFromString(stringSupplier.apply(t2)));
    }
}
