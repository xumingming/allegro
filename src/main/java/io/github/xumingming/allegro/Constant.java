package io.github.xumingming.allegro;

import static java.lang.String.format;

public class Constant
{
    public static final String CONFIG_FILE_PATH = System.getProperty("user.home") + "/.allegro/config.yaml";
    public static final String RESULT_DIR = System.getProperty("user.home") + "/.allegro/results";
    public static final String SYSTEM_PREFIX = "allegro:";

    private Constant()
    {}

    public static String getQuerySetResultDir(String runName)
    {
        return format("%s/%s", RESULT_DIR, runName);
    }

    public static String getSummaryFilePath(String runName)
    {
        return getQuerySetResultDir(runName) + "/summary.csv";
    }

    public static String getRunConfigPath(String runName)
    {
        return getQuerySetResultDir(runName) + "/" + runName + ".yaml";
    }

    public static String getQuerySetRawJsonDir(String runName)
    {
        return getQuerySetResultDir(runName) + "/raw_jsons";
    }

    public static String getQueryRawJsonPath(String runName, String queryName)
    {
        return getQuerySetRawJsonDir(runName) + "/" + queryName + ".json";
    }
}
