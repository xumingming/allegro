package io.github.xumingming.allegro.service;

import io.github.xumingming.allegro.model.OperatorType;
import io.github.xumingming.allegro.model.Query;
import io.github.xumingming.allegro.service.impl.DefaultAnalysisService;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface AnalysisService
{
    static AnalysisService create()
    {
        return new DefaultAnalysisService();
    }

    Map<OperatorType, Duration> getOperatorCpuTimeDistribution(String filePathToQuerySet, List<String> queryNames, boolean normalizeOperatorType);

    Map<OperatorType, Duration> getOperatorWallTimeDistribution(String filePathToQuerySet, List<String> queryNames, boolean normalizeOperatorType);

    /**
     * Get query by querySet & queryName
     *
     * @param filePathToQuerySet
     * @param queryName
     * @return
     */
    Query getQuery(String filePathToQuerySet, String queryName, boolean normalizeOperatorType);
}
