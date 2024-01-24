package io.github.xumingming.allegro.service.impl;

import io.github.xumingming.allegro.ComparingStringByNumberPart;
import io.github.xumingming.allegro.Result;
import io.github.xumingming.allegro.Run;
import io.github.xumingming.allegro.SuiteConf;
import io.github.xumingming.allegro.model.Query;
import io.github.xumingming.allegro.service.AnalysisService;
import io.github.xumingming.allegro.service.ResultService;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Constant.RESULT_DIR;
import static io.github.xumingming.allegro.Constant.getQueryRawJsonPath;
import static io.github.xumingming.allegro.Constant.getQuerySetRawJsonDir;
import static io.github.xumingming.allegro.Constant.getQuerySetResultDir;
import static io.github.xumingming.allegro.Constant.getRunConfigPath;
import static io.github.xumingming.allegro.Constant.getSummaryFilePath;
import static io.github.xumingming.allegro.Utils.append;
import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.getObjectMapper;
import static io.github.xumingming.allegro.Utils.listSubDirs;
import static io.github.xumingming.allegro.Utils.read;
import static java.lang.String.format;

public class DefaultResultService
        implements ResultService
{
    private AnalysisService analysisService = AnalysisService.create();

    @Override
    public void insert(Result result)
    {
        initResultDirIfNecessary();
        String resultStr = format("%s,%s,%s\n",
                result.getQueryName(),
                result.getStatus(),
                result.getElapsedTime().toMillis());
        append(getSummaryFilePath(result.getRunName()), resultStr);
    }

    public String prepareAndGetSuiteConfPath(String runName)
    {
        String dir = getQuerySetResultDir(runName);
        new File(dir).mkdirs();

        return getRunConfigPath(runName);
    }

    @Override
    public List<Result> listByRunName(String runName, boolean includeMemoryAndCpuInfo, boolean normalizeOperatorType)
    {
        checkResultDir();
        String content = read(getSummaryFilePath(runName));
        String[] items = content.split("\n");
        List<Result> allResults = new ArrayList<>();
        for (String item : items) {
            String[] parts = item.split(",");
            Result result = new Result(
                    runName,
                    parts[0],
                    Result.Status.valueOf(parts[1]),
                    Duration.ofMillis(Long.valueOf(parts[2])),
                    Optional.empty());
            if (includeMemoryAndCpuInfo) {
                Query query = analysisService.getQuery(getQuerySetRawJsonDir(runName), parts[0], normalizeOperatorType);
                Result.Status status = Result.Status.valueOf(parts[1]);
                result = new Result(
                        runName,
                        parts[0],
                        Result.Status.valueOf(parts[1]),
                        Duration.ofMillis(Long.valueOf(parts[2])),
                        status == Result.Status.SUCCESS ? Optional.empty() : Optional.of(query.getErrorMessage()));
                result.setCpuTime(query.getCpuTime());
                result.setPeakMemory(query.getPeakMemory());
            }

            allResults.add(result);
        }

        List<Result> ret = allResults.stream()
                .sorted(new ComparingStringByNumberPart<>(Result::getQueryName))
                .collect(Collectors.toList());

        return ret;
    }

    @Override
    public Duration calculateTotalElapseTimeByRunName(String runName, boolean normalizeOperatorType)
    {
        List<Result> results = listByRunName(runName, false, normalizeOperatorType);
        Duration total = Duration.ZERO;
        for (Result result : results) {
            total = total.plus(result.getElapsedTime());
        }

        return total;
    }

    @Override
    public List<String> listRunNames()
    {
        initResultDirIfNecessary();
        return listSubDirs(RESULT_DIR);
    }

    @Override
    public List<Run> listRuns()
    {
        List<String> runNames = listRunNames();

        List<Run> runs = new ArrayList<>();
        for (String runName : runNames) {
            SuiteConf baselineConf = readSuiteConf(runName);
            runs.add(new Run(runName, baselineConf));
        }

        return runs.stream()
                .collect(Collectors.toList());
    }

    @Override
    public void writeSuiteConf(SuiteConf suiteConf)
    {
        initResultDirIfNecessary();

        try {
            getObjectMapper().writeValue(new File(prepareAndGetSuiteConfPath(suiteConf.getRunName())), suiteConf);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SuiteConf readSuiteConf(String runName)
    {
        checkResultDir();
        try {
            File suiteConfFile = new File(getRunConfigPath(runName));
            if (!suiteConfFile.exists()) {
                return null;
            }

            SuiteConf ret = getObjectMapper().readValue(suiteConfFile, SuiteConf.class);
            ret.setRunName(runName);
            return ret;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveQueryRawJson(String runName, String queryName, String queryRawJson)
    {
        try {
            String dir = getQuerySetRawJsonDir(runName);
            new File(dir).mkdirs();

            append(getQueryRawJsonPath(runName, queryName), queryRawJson);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkResultDir()
    {
        checkState(new File(RESULT_DIR).exists(),
                format("allegro result directory(%s) not exists!", RESULT_DIR));

        checkState(new File(RESULT_DIR).isDirectory(),
                format("allegro result directory(%s) should be a directory!", RESULT_DIR));
    }

    private void initResultDirIfNecessary()
    {
        if (!new File(RESULT_DIR).exists()) {
            new File(RESULT_DIR).mkdirs();
        }
    }
}
