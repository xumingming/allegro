package io.github.xumingming.allegro;

public class AllegroConf
{
    /**
     * Name for current run. A run contains running the queries in the querySet.
     */
    private String runName;
    /**
     * The config for the basic suite.
     */
    private SuiteConf baseline;

    public String getRunName()
    {
        return runName;
    }

    public void setRunName(String runName)
    {
        this.runName = runName;
    }

    public SuiteConf getBaseline()
    {
        return baseline;
    }

    public void setBaseline(SuiteConf baseline)
    {
        this.baseline = baseline;
    }

    private void init(String runName, String dbName)
    {
        baseline.setRunName(runName);
        if (dbName != null) {
            baseline.setDbName(dbName);
        }
    }

    public String initAndValidate(String runName, String dbName)
    {
        init(runName, dbName);
        String error = baseline.validate();
        if (error != null) {
            return error;
        }

        return null;
    }
}
