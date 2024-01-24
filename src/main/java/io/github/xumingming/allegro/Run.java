package io.github.xumingming.allegro;

import java.util.Date;

public class Run
{
    private String runName;
    private SuiteConf baselineConf;
    private Date endTime;

    public Run(String runName, SuiteConf baselineConf)
    {
        this.runName = runName;
        this.baselineConf = baselineConf;
    }

    public String getRunName()
    {
        return runName;
    }

    public void setRunName(String runName)
    {
        this.runName = runName;
    }

    public SuiteConf getBaselineConf()
    {
        return baselineConf;
    }

    public Date getEndTime()
    {
        return endTime;
    }

    public void setEndTime(Date endTime)
    {
        this.endTime = endTime;
    }
}
