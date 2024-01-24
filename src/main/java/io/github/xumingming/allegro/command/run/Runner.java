package io.github.xumingming.allegro.command.run;

import io.github.xumingming.allegro.PrestoClient;
import io.github.xumingming.allegro.Result;
import io.github.xumingming.allegro.SuiteConf;
import io.github.xumingming.allegro.service.ConfigService;
import io.github.xumingming.allegro.service.ResultService;
import io.github.xumingming.beauty.Color;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.github.xumingming.allegro.Result.failure;
import static io.github.xumingming.allegro.Result.success;
import static io.github.xumingming.beauty.Beauty.draw;
import static io.github.xumingming.beauty.Beauty.drawError;
import static io.github.xumingming.beauty.Utils.duration;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Runner
{
    private final SuiteConf suiteConf;
    private final ResultService resultService = ResultService.create();
    private final PrestoClient prestoClient;
    private final ConfigService configService = ConfigService.create();
    private final boolean fetchQueryDetail;

    public Runner(SuiteConf suiteConf, boolean fetchQueryDetail)
    {
        this.suiteConf = requireNonNull(suiteConf, "suiteConf is null!");
        this.prestoClient = new PrestoClient(suiteConf.getCoordinatorIp(), suiteConf.getCoordinatorPort());
        this.fetchQueryDetail = fetchQueryDetail;
    }

    static String addHints(String sql, String hints)
    {
        String trimedSql = sql.trim();
        if (trimedSql.startsWith("/*+")) {
            sql = "/*+" + hints + "," + trimedSql.substring(3);
        }
        else {
            sql = "/*+" + hints + "*/" + sql;
        }

        return sql;
    }

    public void run()
    {
        List<String> fileNameList = configService.listQueries(suiteConf.getQuerySet());
        draw(String.format("Queries: %s\n", fileNameList));
        resultService.writeSuiteConf(suiteConf.cloneWithSensitiveInfoMasked());
        List<Result> results = new ArrayList<>();
        for (int i = 0; i < fileNameList.size(); i++) {
            String fileName = fileNameList.get(i);
            String queryName = getQueryNameFromFileName(fileName);
            String filePath = suiteConf.getQuerySet() + "/" + fileName;

            draw(format("Start running query: %s\n", queryName), Color.WHITE);
            String sql = configService.readQuery(filePath);

            final String traceId = suiteConf.getTraceId(queryName);
            sql = addHints(sql, String.format("%s=%s", "traceId", traceId));

            System.out.printf("Query: %s\n", sql);
            long startMs = System.currentTimeMillis();
            try {
                Optional<String> error = prestoClient.run(suiteConf.getJdbcUrl(traceId), suiteConf.getUser(), suiteConf.getPassword(), suiteConf.getSessionPropertiesAsMap(), sql);
                final long endMs = System.currentTimeMillis();
                Duration duration = Duration.ofMillis(endMs - startMs);
                if (error.isPresent()) {
                    results.add(failure(suiteConf.getRunName(), queryName, duration, error.get()));
                }
                else {
                    results.add(success(suiteConf.getRunName(), queryName, duration));
                }
            }
            catch (Exception e) {
                drawError(e.getMessage());
                results.add(failure(suiteConf.getRunName(), queryName, Duration.ofMillis(System.currentTimeMillis() - startMs), e.getMessage()));
            }

            Result currentResult = results.get(results.size() - 1);
            if (currentResult.getStatus() == Result.Status.SUCCESS) {
                draw(format("Run query: %s successfully, cost: %s\n",
                                currentResult.getQueryName(),
                                duration(currentResult.getElapsedTime())),
                        Color.WHITE);
            }
            else {
                drawError(format("Run query: %s failed! Reason: %s, cost: %s\n",
                        currentResult.getQueryName(),
                        currentResult.getErrorMessage().get(),
                        duration(currentResult.getElapsedTime())));
            }

            if (fetchQueryDetail) {
                String queryInfo = prestoClient.getQueryRawJsonByTraceId(traceId);
                resultService.saveQueryRawJson(suiteConf.getRunName(), queryName, queryInfo);
            }
            resultService.insert(currentResult);
        }
    }

    public String getQueryNameFromFileName(String fileName)
    {
        // /root/hello/world/q123.sql -> q123.
        return fileName.substring(fileName.lastIndexOf("/") + 1, fileName.lastIndexOf("."));
    }
}
