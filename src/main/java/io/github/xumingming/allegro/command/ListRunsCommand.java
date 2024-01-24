package io.github.xumingming.allegro.command;

import io.github.xumingming.allegro.Constant;
import io.github.xumingming.allegro.Run;
import io.github.xumingming.allegro.service.ResultService;
import picocli.CommandLine;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Utils.formatDateTime;
import static io.github.xumingming.beauty.Beauty.draw;
import static io.github.xumingming.beauty.Beauty.table;
import static io.github.xumingming.beauty.Column.column;
import static io.github.xumingming.beauty.Utils.duration;

@CommandLine.Command(
        name = "list",
        description = "List the runs.",
        subcommands = CommandLine.HelpCommand.class)
public class ListRunsCommand
        implements Callable<Integer>
{
    @CommandLine.Option(
            names = {"-f", "--filter"},
            description = "filter which will be applied on runName")
    private String filter;

    @CommandLine.Option(
            names = {"-e", "--exclude"},
            description = "exclude the run from showing")
    private List<String> excludes;

    @CommandLine.Option(
            names = {"-m", "--normalize"},
            defaultValue = "true",
            description = "Do we normalize the operator name.")
    private boolean normalize;

    private ResultService resultService = ResultService.create();

    @Override
    public Integer call()
            throws Exception
    {
        List<Run> runs = resultService.listRuns()
                .stream()
                .filter(run -> filter == null ? true : run.getRunName().contains(filter))
                .filter(run -> (excludes == null || excludes.isEmpty()) ? true : !excludes.contains(run.getRunName()))
                .collect(Collectors.toList());

        Map<String, Duration> runName2ElapseTime = new HashMap<>();
        for (Run run : runs) {
            runName2ElapseTime.put(run.getRunName(), resultService.calculateTotalElapseTimeByRunName(run.getRunName(), normalize));
            String summaryPath = Constant.getSummaryFilePath(run.getRunName());
            run.setEndTime(new Date(new File(summaryPath).lastModified()));
        }

        // Sort it.
        runs.sort((Run left, Run right) -> left.getEndTime().compareTo(right.getEndTime()));
        draw(table(
                runs,
                Arrays.asList(
                        column("Run", (Run run) -> run.getRunName()),
                        column("QuerySet", (Run run) -> run.getBaselineConf().getQuerySet()),
                        column("Schema", (Run run) -> run.getBaselineConf().getDbName()),
                        column("Date", (Run run) -> formatDateTime(run.getEndTime())),
                        column("Elapse Time", (Run run) -> duration(runName2ElapseTime.get(run.getRunName()))))));
        return 0;
    }
}
