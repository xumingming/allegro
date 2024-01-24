package io.github.xumingming.allegro.command.view;

import io.airlift.units.DataSize;
import io.github.xumingming.allegro.ComparingStringByNumberPart;
import io.github.xumingming.allegro.Result;
import io.github.xumingming.allegro.ResultList;
import io.github.xumingming.allegro.RunNameAndSuite;
import io.github.xumingming.allegro.SuiteConf;
import io.github.xumingming.allegro.model.OperatorType;
import io.github.xumingming.allegro.service.AnalysisService;
import io.github.xumingming.allegro.service.ResultService;
import io.github.xumingming.beauty.Color;
import io.github.xumingming.beauty.Column;
import io.github.xumingming.beauty.TableStyle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Constant.getQuerySetRawJsonDir;
import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.drawSuiteConf;
import static io.github.xumingming.allegro.Utils.speedup;
import static io.github.xumingming.beauty.Beauty.draw;
import static io.github.xumingming.beauty.Beauty.drawH2Title;
import static io.github.xumingming.beauty.Beauty.table;
import static io.github.xumingming.beauty.Column.column;
import static io.github.xumingming.beauty.Utils.dataSize;
import static io.github.xumingming.beauty.Utils.duration;
import static java.lang.String.format;

public class CompareResultAction
{
    private static final Color TABLE_HEAD_COLOR = Color.WHITE;

    private AnalysisService analysisService = AnalysisService.create();
    private List<RunNameAndSuite> runNameAndSuites;
    private Optional<List<String>> names;
    private List<String> queryNames;
    private boolean normalize;
    private TableStyle tableStyle;

    private ResultService resultService = ResultService.create();

    public CompareResultAction(List<RunNameAndSuite> runNameAndSuites, Optional<List<String>> names, List<String> queryNames, boolean normalize, TableStyle tableStyle)
    {
        if (names.isPresent()) {
            checkState(runNameAndSuites.size() == names.get().size(), format("runNameAndSuites and names are not of the same size(%s vs %s)", runNameAndSuites.size(), names.get().size()));
        }
        this.runNameAndSuites = runNameAndSuites;
        this.names = names;
        this.queryNames = queryNames;
        this.normalize = normalize;
        this.tableStyle = tableStyle;
    }

    public void act()
    {
        int suiteCount = runNameAndSuites.size();
        List<Result> results = new ArrayList<>();
        List<SuiteConf> suiteConfs = new ArrayList<>();
        List<String> displayNames = new ArrayList<>();
        if (names.isPresent()) {
            displayNames = names.get();
        }

        for (int i = 0; i < suiteCount; i++) {
            RunNameAndSuite runNameAndSuite = runNameAndSuites.get(i);

            List<Result> current = resultService.listByRunName(runNameAndSuite.getRunName(), true, normalize);
            current = filterByQueryNames(current);
            results.addAll(current);

            SuiteConf suiteConf = resultService.readSuiteConf(runNameAndSuite.getRunName());
            suiteConfs.add(suiteConf);

            if (!names.isPresent()) {
                displayNames.add(suiteConfs.get(i).getRunName());
            }
        }

        // Print the conf first.
        for (int i = 0; i < suiteCount; i++) {
            RunNameAndSuite runNameAndSuite = runNameAndSuites.get(i);
            drawSuiteConf(resultService.readSuiteConf(runNameAndSuite.getRunName()));
        }

        Map<String, List<Result>> queryToResults = results.stream().collect(Collectors.groupingBy(Result::getQueryName));
        List<ResultList> resultLists = new ArrayList<>();
        for (Map.Entry<String, List<Result>> pair : queryToResults.entrySet()) {
            List<Result> sortedResults = sortResultListByRunNameAndSuite(pair.getValue(), runNameAndSuites);
            resultLists.add(new ResultList(sortedResults));
        }

        List<Long> elapseTimeTotalMillisList = new ArrayList<>();
        for (int i = 0; i < suiteCount; i++) {
            int currentIndex = i;
            long elapseTimeTotalMillis = resultLists.stream()
                    .map(resultList -> resultList.get(currentIndex))
                    .map(Result::getElapsedTime)
                    .mapToLong(Duration::toMillis).sum();

            elapseTimeTotalMillisList.add(elapseTimeTotalMillis);
        }

        List<Long> cpuTimeTotalMillisList = new ArrayList<>();
        for (int i = 0; i < suiteCount; i++) {
            int currentIndex = i;
            long cpuTimeTotalMillis = resultLists.stream()
                    .map(resultList -> resultList.get(currentIndex))
                    .map(Result::getCpuTime)
                    .mapToLong(Duration::toMillis).sum();

            cpuTimeTotalMillisList.add(cpuTimeTotalMillis);
        }

        List<Long> peakMemoryBytesList = new ArrayList<>();
        for (int i = 0; i < suiteCount; i++) {
            int currentIndex = i;
            long peakMemoryBytes = resultLists.stream()
                    .map(resultList -> resultList.get(currentIndex))
                    .map(Result::getPeakMemory)
                    .mapToLong(DataSize::toBytes).sum();

            peakMemoryBytesList.add(peakMemoryBytes);
        }

        List<Result> summaryResults = new ArrayList<>(suiteCount);
        for (int i = 0; i < suiteCount; i++) {
            RunNameAndSuite runNameAndSuite = runNameAndSuites.get(i);

            Result summaryResult = Result.success(runNameAndSuite.getRunName(), runNameAndSuite.getSuite(), "Total", Duration.ofMillis(elapseTimeTotalMillisList.get(i)));
            summaryResult.setCpuTime(Duration.ofMillis(cpuTimeTotalMillisList.get(i)));
            summaryResult.setPeakMemory(DataSize.succinctBytes(peakMemoryBytesList.get(i)));
            summaryResults.add(summaryResult);
        }

        ResultList summary = new ResultList(summaryResults);
        resultLists.sort(new ComparingStringByNumberPart<>(ResultList::getQueryName));
        resultLists.add(summary);

        compareElapseTime(displayNames, resultLists);
        compareCpuTime(displayNames, resultLists);
        comparePeakMemory(displayNames, resultLists);
        compareTopOperatorsByCpuTime(displayNames, suiteConfs);
        compareTopOperatorsByWallTime(displayNames, suiteConfs);
    }

    private List<Result> sortResultListByRunNameAndSuite(List<Result> results, List<RunNameAndSuite> runNameAndSuites)
    {
        checkState(
                results.size() == runNameAndSuites.size(),
                format("results and runNameAndSuites are not of the same size. (%s vs %s), query: %s", results.size(), runNameAndSuites.size(), results.get(0).getQueryName()));
        List<Result> ret = new ArrayList<>(results.size());

        for (int i = 0; i < runNameAndSuites.size(); i++) {
            RunNameAndSuite runNameAndSuite = runNameAndSuites.get(i);
            Optional<Result> resultOpt = results
                    .stream()
                    .filter(
                            result -> result.getRunName().equals(runNameAndSuite.getRunName()))
                    .findFirst();
            checkState(resultOpt.isPresent(), format("No result for %s#%s, results are: %s", runNameAndSuite.getRunName(), runNameAndSuite.getSuite(), results));
            ret.add(resultOpt.get());
        }

        return ret;
    }

    private void compareElapseTime(List<String> displayNames, List<ResultList> resultLists)
    {
        drawH2Title("Elapse Time");
        List<Column<ResultList>> columnsToShow = new ArrayList<>();
        columnsToShow.add(column("Query", (ResultList result) -> result.getQueryName()));

        for (int i = 0; i < displayNames.size(); i++) {
            final int currentIndex = i;
            columnsToShow.add(column(displayNames.get(i), (ResultList result) -> duration(result.get(currentIndex).getElapsedTime())));
            if (i > 0) {
                columnsToShow.add(column(
                        "Speedup",
                        (ResultList result) -> speedup(result.getElapseTimeSpeedup(currentIndex)),
                        (ResultList result) -> result.getElapseTimeSpeedup(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }

            if (i == displayNames.size() - 1 && i > 1) {
                columnsToShow.add(column(
                        "Total Speedup",
                        (ResultList result) -> speedup(result.getElapseTimeTotalSpeedup(currentIndex)),
                        (ResultList result) -> result.getElapseTimeTotalSpeedup(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }
        }
        draw(table(resultLists, columnsToShow, TABLE_HEAD_COLOR, tableStyle));
    }

    private void compareCpuTime(List<String> displayNames, List<ResultList> resultLists)
    {
        drawH2Title("Cpu Time");
        List<Column<ResultList>> columnsToShow = new ArrayList<>();
        columnsToShow.add(column("Query", (ResultList result) -> result.getQueryName()));

        for (int i = 0; i < displayNames.size(); i++) {
            final int currentIndex = i;
            columnsToShow.add(column(displayNames.get(i), (ResultList result) -> duration(result.get(currentIndex).getCpuTime())));
            if (i > 0) {
                columnsToShow.add(column(
                        "Speedup",
                        (ResultList result) -> speedup(result.getCpuTimeSpeedup(currentIndex)),
                        (ResultList result) -> result.getCpuTimeSpeedup(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }

            // total speedup
            if (i == displayNames.size() - 1 && i > 1) {
                columnsToShow.add(column(
                        "Total Speedup",
                        (ResultList result) -> speedup(result.getCpuTimeTotalSpeedup(currentIndex)),
                        (ResultList result) -> result.getCpuTimeTotalSpeedup(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }
        }
        draw(table(resultLists, columnsToShow, TABLE_HEAD_COLOR, tableStyle));
    }

    private void comparePeakMemory(List<String> displayNames, List<ResultList> resultLists)
    {
        drawH2Title("Peak Memory");
        List<Column<ResultList>> columnsToShow = new ArrayList<>();
        columnsToShow.add(column("Query", (ResultList result) -> result.getQueryName()));

        for (int i = 0; i < displayNames.size(); i++) {
            final int currentIndex = i;
            columnsToShow.add(column(displayNames.get(i), (ResultList result) -> dataSize(result.get(currentIndex).getPeakMemory())));
            if (i > 0) {
                columnsToShow.add(column(
                        "Speedup",
                        (ResultList result) -> speedup(result.getPeakMemorySpeedup(currentIndex)),
                        (ResultList result) -> result.getPeakMemorySpeedup(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }

            // total speedup
            if (i == displayNames.size() - 1 && i > 1) {
                columnsToShow.add(column(
                        "Total Speedup",
                        (ResultList result) -> speedup(result.getPeakMemoryTotalSpeedup(currentIndex)),
                        (ResultList result) -> result.getPeakMemoryTotalSpeedup(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }
        }
        draw(table(resultLists, columnsToShow, TABLE_HEAD_COLOR, tableStyle));
    }

    private void compareTopOperatorsByCpuTime(List<String> displayNames, List<SuiteConf> suiteConfs)
    {
        compareTopOperatorsHelper(
                "Top Operators(CPU Time)",
                displayNames,
                suiteConfs,
                runName -> analysisService.getOperatorCpuTimeDistribution(getQuerySetRawJsonDir(runName), queryNames, normalize));
    }

    private void compareTopOperatorsByWallTime(List<String> displayNames, List<SuiteConf> suiteConfs)
    {
        compareTopOperatorsHelper(
                "Top Operators(Wall Time)",
                displayNames,
                suiteConfs,
                runName -> analysisService.getOperatorWallTimeDistribution(getQuerySetRawJsonDir(runName), queryNames, normalize));
    }

    private void compareTopOperatorsHelper(String name, List<String> displayNames, List<SuiteConf> suiteConfs, Function<String, Map<OperatorType, Duration>> operatorTimeExtractor)
    {
        List<Map<OperatorType, Duration>> distributions = new ArrayList<>();
        List<Duration> totals = new ArrayList<>();
        for (int i = 0; i < displayNames.size(); i++) {
            SuiteConf suiteConf = suiteConfs.get(i);
            Map<OperatorType, Duration> distribution = operatorTimeExtractor.apply(suiteConf.getRunName());
            distributions.add(distribution);
            totals.add(distribution.values().stream().reduce(Duration.ZERO, Duration::plus));
        }

        List<CpuTimePair> operatorCpuTimes = new ArrayList<>();
        Set<OperatorType> interestedTypes = new HashSet<>();
        interestedTypes.addAll(distributions.get(0).keySet().stream().collect(Collectors.toList()));
        interestedTypes.addAll(distributions.get(1).keySet().stream().collect(Collectors.toList()));

        for (OperatorType type : interestedTypes) {
            List<Duration> durations = distributions.stream().map(x -> x.get(type)).collect(Collectors.toList());
            operatorCpuTimes.add(new CpuTimePair(
                    type.name(),
                    durations,
                    totals));
        }

        drawH2Title(name);
        showTopOperators(displayNames, operatorCpuTimes);
    }

    private void showTopOperators(List<String> displayNames, List<CpuTimePair> data)
    {
        data.sort((CpuTimePair leftPair, CpuTimePair rightPair) -> {
            if (leftPair.get(0) == null) {
                return 1;
            }

            if (rightPair.get(0) == null) {
                return -1;
            }

            return rightPair.get(0).compareTo(leftPair.get(0));
        });

        List<Column<CpuTimePair>> columnsToShow = new ArrayList<>();
        columnsToShow.add(column("Operator", (CpuTimePair cpuTimePair) -> cpuTimePair.getName()));

        for (int i = 0; i < displayNames.size(); i++) {
            final int currentIndex = i;
            columnsToShow.add(
                    column(
                            displayNames.get(i),
                            (CpuTimePair cpuTimePair) -> format("%s", duration(cpuTimePair.get(currentIndex)))));
            if (i > 0) {
                columnsToShow.add(column(
                        "Speedup",
                        (CpuTimePair cpuTimePair) -> speedup(cpuTimePair.getOptimization(currentIndex)),
                        (CpuTimePair cpuTimePair) -> cpuTimePair.getOptimization(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }

            // Total speedup.
            if (i == displayNames.size() - 1 && i > 1) {
                columnsToShow.add(column(
                        "Total Speedup",
                        (CpuTimePair cpuTimePair) -> speedup(cpuTimePair.getTotalOptimization(currentIndex)),
                        (CpuTimePair cpuTimePair) -> cpuTimePair.getTotalOptimization(currentIndex) > 0 ? Color.GREEN : Color.RED));
            }
        }

        draw(table(data, columnsToShow, TABLE_HEAD_COLOR, tableStyle));
    }

    private List<Result> filterByQueryNames(List<Result> results)
    {
        if (!queryNames.isEmpty()) {
            results = results.stream()
                    .filter(x -> queryNames.contains(x.getQueryName()))
                    .collect(Collectors.toList());
        }
        return results;
    }

    private static class CpuTimePair
    {
        private final String name;
        private final List<Duration> durations;
        private final List<Duration> totalDurations;

        public CpuTimePair(String name, List<Duration> durations, List<Duration> totalDurations)
        {
            this.name = name;
            this.durations = durations;
            this.totalDurations = totalDurations;
        }

        public String getName()
        {
            return name;
        }

        public Duration get(int index)
        {
            return durations.get(index);
        }

        public Duration getTotal(int index)
        {
            return totalDurations.get(index);
        }

        public double getPercentage(int index)
        {
            Duration duration = get(index);
            Duration totalDuration = getTotal(index);
            return duration.toMillis() * 1.0 / totalDuration.toMillis();
        }

        public double getOptimization(int index)
        {
            checkState(index > 0, "getOptimization only support index which is greater than 0.");
            Duration baseline = get(index - 1);
            Duration optimized = get(index);

            if (baseline == null || optimized == null || baseline.toMillis() == 0 || optimized.toMillis() == 0) {
                return 0;
            }

            return baseline.toMillis() * 1.0 / optimized.toMillis();
        }

        public double getTotalOptimization(int index)
        {
            checkState(index > 0, "getTotalOptimization only support index which is greater than 0.");
            Duration baseline = get(0);
            Duration optimized = get(index);

            if (baseline == null || optimized == null || baseline.toMillis() == 0 || optimized.toMillis() == 0) {
                return 0;
            }
            return baseline.toMillis() * 1.0 / optimized.toMillis();
        }
    }
}
