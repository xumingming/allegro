package io.github.xumingming.allegro;

import java.util.Objects;

public class RunNameAndSuite
{
    private final String runName;
    private final Suite suite;

    public RunNameAndSuite(String runName, Suite suite)
    {
        this.runName = runName;
        this.suite = suite;
    }

    public String getRunName()
    {
        return runName;
    }

    public Suite getSuite()
    {
        return suite;
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
        RunNameAndSuite that = (RunNameAndSuite) o;
        return Objects.equals(runName, that.runName) && suite == that.suite;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(runName, suite);
    }

    @Override
    public String toString()
    {
        return "RunNameAndSuite{" + "runName='" + runName + '\'' + ", suite=" + suite + '}';
    }
}
