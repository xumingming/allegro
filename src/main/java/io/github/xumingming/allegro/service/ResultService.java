package io.github.xumingming.allegro.service;

import io.github.xumingming.allegro.Result;
import io.github.xumingming.allegro.Run;
import io.github.xumingming.allegro.SuiteConf;
import io.github.xumingming.allegro.service.impl.DefaultResultService;

import java.time.Duration;
import java.util.List;

public interface ResultService
{
    static ResultService create()
    {
        return new DefaultResultService();
    }

    void insert(Result result);

    List<Result> listByRunName(String runName, boolean includeMemoryAndCpuInfo, boolean normalizeOperatorType);

    Duration calculateTotalElapseTimeByRunName(String runName, boolean normalizeOperatorType);

    List<String> listRunNames();

    List<Run> listRuns();

    String prepareAndGetSuiteConfPath(String runName);

    void writeSuiteConf(SuiteConf suiteConf);

    SuiteConf readSuiteConf(String runName);

    void saveQueryRawJson(String runName, String queryName, String queryRawJson);
}
