package io.github.xumingming.allegro.command.view;

import io.github.xumingming.allegro.Result;
import io.github.xumingming.allegro.SuiteConf;
import io.github.xumingming.allegro.model.OperatorType;
import io.github.xumingming.allegro.service.AnalysisService;
import io.github.xumingming.allegro.service.ResultService;
import io.github.xumingming.beauty.BarItem;
import io.github.xumingming.beauty.Color;
import io.github.xumingming.beauty.Column;
import io.github.xumingming.beauty.TableStyle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Constant.getQuerySetRawJsonDir;
import static io.github.xumingming.beauty.BarChart.durationBarChart;
import static io.github.xumingming.beauty.Beauty.barChartAsString;
import static io.github.xumingming.beauty.Beauty.draw;
import static io.github.xumingming.beauty.Beauty.drawH2Title;
import static io.github.xumingming.beauty.Beauty.table;
import static io.github.xumingming.beauty.Column.column;
import static io.github.xumingming.beauty.Utils.dataSize;
import static io.github.xumingming.beauty.Utils.duration;
import static io.github.xumingming.beauty.Utils.percentage;
import static java.lang.String.format;

public class ViewSingleResultAction
{
    private AnalysisService analysisService = AnalysisService.create();
    private String runName;
    private List<String> queryNames;
    private TableStyle tableStyle;
    private boolean normalize;

    private ResultService resultService = ResultService.create();

    public ViewSingleResultAction(String runName, List<String> queryNames, boolean normalize, TableStyle tableStyle)
    {
        this.runName = runName;
        this.queryNames = queryNames;
        this.normalize = normalize;
        this.tableStyle = tableStyle;
    }

    private static String getShortErrorMessage(String errorMessage)
    {
        if (errorMessage.length() > 50) {
            return errorMessage.substring(0, 50) + "...";
        }

        return errorMessage;
    }

    public void act()
    {
        List<Result> results = resultService.listByRunName(runName, true, normalize);
        results = filterByQueryNames(results);

        SuiteConf suiteConf = resultService.readSuiteConf(runName);
        long totalMillis = results.stream()
                .map(Result::getElapsedTime)
                .mapToLong(Duration::toMillis)
                .sum();

        boolean containsFailure = results.stream().anyMatch(result -> result.getStatus() == Result.Status.FAILURE);
        Result total = Result.success(runName, "Total", Duration.ofMillis(totalMillis));
        if (containsFailure) {
            total = Result.failure(runName, "Total", Duration.ofMillis(totalMillis), "-");
        }
        List<Result> resultsIncludeTotal = new ArrayList<>();
        resultsIncludeTotal.addAll(results);
        resultsIncludeTotal.add(total);

        drawH2Title(runName);

        List<Column<Result>> columnsToShow = Arrays.asList(
                column("Query", (Result result) -> result.getQueryName()),
                column("Time", (Result result) -> duration(result.getElapsedTime())),
                column(
                        "State",
                        (Result result) -> result.getStatus(),
                        (Result result) -> result.getStatus() == Result.Status.FAILURE ? Color.RED : Color.NONE));

        boolean hasError = results.stream()
                .anyMatch(result -> result.getStatus() == Result.Status.FAILURE);
        // If there is any error, display the error message column.
        if (hasError) {
            columnsToShow = Arrays.asList(
                    column("Query", (Result result) -> result.getQueryName()),
                    column("Time", (Result result) -> duration(result.getElapsedTime())),
                    column(
                            "State",
                            (Result result) -> result.getStatus(),
                            (Result result) -> result.getStatus() == Result.Status.FAILURE ? Color.RED : Color.NONE),
                    column(
                            "Error",
                            (Result result) -> result.getErrorMessage().isPresent() ? getShortErrorMessage(result.getErrorMessage().get()) : "",
                            (Result result) -> result.getStatus() == Result.Status.FAILURE ? Color.RED : Color.NONE));
        }
        draw(table(resultsIncludeTotal, columnsToShow, Color.WHITE, tableStyle));
        draw(barChartAsString(durationBarChart(results
                .stream()
                .map(result -> new BarItem<>(result.getQueryName(), result.getElapsedTime()))
                .collect(Collectors.toList()))));

        displayCpuTime(results);
        displayPeakMemory(results);
        displayTopOperatorsByCpuTime(suiteConf, queryNames);
        displayTopOperatorsByWallTime(suiteConf, queryNames);
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

    private void displayCpuTime(List<Result> results)
    {
        drawH2Title("CPU Time");
        draw(table(
                results,
                Arrays.asList(
                        column("Query", (Result result) -> result.getQueryName()),
                        column("CPU Time", (Result result) -> duration(result.getCpuTime()))),
                Color.WHITE,
                tableStyle));
    }

    private void displayPeakMemory(List<Result> results)
    {
        drawH2Title("Peak Memory");
        draw(table(
                results,
                Arrays.asList(
                        column("Query", (Result result) -> result.getQueryName()),
                        column("Peak Memory", (Result result) -> dataSize(result.getPeakMemory()))),
                Color.WHITE,
                tableStyle));
    }

    private void displayTopOperatorsByCpuTime(SuiteConf suiteConf, List<String> queryNames)
    {
        displayTopOperatorsHelper(
                "Top Operators(CPU Time)",
                suiteConf,
                runName -> analysisService.getOperatorCpuTimeDistribution(getQuerySetRawJsonDir(runName), queryNames, normalize));
    }

    private void displayTopOperatorsByWallTime(SuiteConf suiteConf, List<String> queryNames)
    {
        displayTopOperatorsHelper(
                "Top Operators(Wall Time)",
                suiteConf,
                runName -> analysisService.getOperatorWallTimeDistribution(getQuerySetRawJsonDir(runName), queryNames, normalize));
    }

    private void displayTopOperatorsHelper(String name, SuiteConf suiteConf, Function<String, Map<OperatorType, Duration>> timeExtractor)
    {
        Map<OperatorType, Duration> distribution = timeExtractor.apply(suiteConf.getRunName());

        Duration totalCpuTime = distribution.values().stream().reduce(Duration.ZERO, Duration::plus);

        List<CpuTimeAndPercentage> data = new ArrayList<>();
        List<OperatorType> interestedTypes = distribution.keySet().stream().collect(Collectors.toList());
        for (OperatorType type : interestedTypes) {
            data.add(new CpuTimeAndPercentage(
                    type.name(),
                    distribution.get(type),
                    totalCpuTime));
        }

        // Order by CPU Time desc.
        data.sort((CpuTimeAndPercentage pair1, CpuTimeAndPercentage pair2) -> pair2.getCpuTime().compareTo(pair1.getCpuTime()));

        drawH2Title(name);
        draw(table(
                data,
                Arrays.asList(
                        column("Operator", (CpuTimeAndPercentage cpuTimeAndPercentage) -> cpuTimeAndPercentage.getName()),
                        column("Time/Percentage", (CpuTimeAndPercentage cpuTimeAndPercentage) -> format("%s(%s)", duration(cpuTimeAndPercentage.cpuTime), percentage(cpuTimeAndPercentage.getFirstPercentage())))),
                Color.WHITE,
                tableStyle));
    }

    private static class CpuTimeAndPercentage
    {
        private final String name;
        private final Duration cpuTime;
        private final Duration totalCpuTime;

        public CpuTimeAndPercentage(String name, Duration cpuTime, Duration totalCpuTime)
        {
            this.name = name;
            this.cpuTime = cpuTime;
            this.totalCpuTime = totalCpuTime;
        }

        public String getName()
        {
            return name;
        }

        public Duration getCpuTime()
        {
            return cpuTime;
        }

        public Duration getTotalCpuTime()
        {
            return totalCpuTime;
        }

        public double getFirstPercentage()
        {
            return cpuTime.toMillis() * 1.0 / totalCpuTime.toMillis();
        }
    }
}
