package io.github.xumingming.allegro.service.impl;

import io.github.xumingming.allegro.model.OperatorType;
import io.github.xumingming.allegro.model.Query;
import io.github.xumingming.allegro.model.enriched.EnrichedQuery;
import io.github.xumingming.allegro.service.AnalysisService;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.github.xumingming.allegro.Utils.extractInfoFromQueriesAsMap;
import static io.github.xumingming.allegro.Utils.parseQuery;

public class DefaultAnalysisService
        implements AnalysisService
{
    @Override
    public Map<OperatorType, Duration> getOperatorCpuTimeDistribution(String filePathToQuerySet, List<String> queryNames, boolean normalizeOperatorType)
    {
        return getOperatorTimeDistributionHelper(
                filePathToQuerySet,
                query -> {
                    EnrichedQuery enrichedQuery = new EnrichedQuery(query);
                    return enrichedQuery.getOperatorCpuTimeDistribution();
                },
                queryNames,
                normalizeOperatorType);
    }

    @Override
    public Map<OperatorType, Duration> getOperatorWallTimeDistribution(String filePathToQuerySet, List<String> queryNames, boolean normalizeOperatorType)
    {
        return getOperatorTimeDistributionHelper(
                filePathToQuerySet,
                query -> {
                    EnrichedQuery enrichedQuery = new EnrichedQuery(query);
                    return enrichedQuery.getOperatorWallTimeDistribution();
                },
                queryNames,
                normalizeOperatorType);
    }

    public Map<OperatorType, Duration> getOperatorTimeDistributionHelper(String filePathToQuerySet, Function<Query, Map<OperatorType, Duration>> timeExtractor, List<String> queryNames, boolean normalizeOperatorType)
    {
        Map<String, Map<OperatorType, Duration>> queryNameToOperatorTimeDistribution = extractInfoFromQueriesAsMap(
                filePathToQuerySet,
                query -> query.getSimpleName(),
                timeExtractor,
                normalizeOperatorType);

        Map<String, Map<OperatorType, Duration>> newQueryNameToOperatorTimeDistribution = new HashMap<>();
        if (!queryNames.isEmpty()) {
            for (String queryName : queryNames) {
                if (queryNameToOperatorTimeDistribution.containsKey(queryName)) {
                    newQueryNameToOperatorTimeDistribution.put(queryName, queryNameToOperatorTimeDistribution.get(queryName));
                }
            }
        }
        else {
            newQueryNameToOperatorTimeDistribution = queryNameToOperatorTimeDistribution;
        }

        Map<OperatorType, Duration> ret = new HashMap<>();

        for (Map<OperatorType, Duration> queryLevelDistribution : newQueryNameToOperatorTimeDistribution.values()) {
            for (Map.Entry<OperatorType, Duration> typeToDuration : queryLevelDistribution.entrySet()) {
                if (!ret.containsKey(typeToDuration.getKey())) {
                    ret.put(typeToDuration.getKey(), typeToDuration.getValue());
                }
                else {
                    ret.put(typeToDuration.getKey(), ret.get(typeToDuration.getKey()).plus(typeToDuration.getValue()));
                }
            }
        }

        return ret;
    }

    @Override
    public Query getQuery(String filePathToQuerySet, String queryName, boolean normalizeOperatorType)
    {
        Query query = parseQuery(filePathToQuerySet + "/" + queryName + ".json", normalizeOperatorType);
        return query;
    }
}
