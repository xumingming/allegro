package io.github.xumingming.allegro.command.view;

import io.github.xumingming.allegro.service.AnalysisService;
import io.github.xumingming.allegro.service.ResultService;
import io.github.xumingming.beauty.TableStyle;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.isNotBlank;
import static io.github.xumingming.beauty.Beauty.drawError;

@CommandLine.Command(
        name = "view",
        description = "view the result of a run.",
        subcommands = CommandLine.HelpCommand.class)
public class ViewResultCommand
        implements Callable<Integer>
{
    private AnalysisService analysisService = AnalysisService.create();
    private ResultService resultService = ResultService.create();

    @CommandLine.Parameters(
            description = "the name for run we want to view the result. Can specify more than one run which will result in compare view")
    private List<String> runs;

    @CommandLine.Option(
            names = {"-n", "--names"},
            description = "the beautiful names for each of the run to show.")
    private String names;

    @CommandLine.Option(
            names = {"-f", "--format"},
            defaultValue = "CLICKHOUSE",
            description = "the format of the table we want to use when show the results.")
    private String format;

    @CommandLine.Option(
            names = {"-q", "--queries"},
            description = "Queries to show.")
    private String queries;

    @CommandLine.Option(
            names = {"-d", "--denormalize"},
            defaultValue = "true",
            description = "Do we normalize the operator name.")
    private boolean normalize;

    public ViewResultCommand()
    {}

    @Override
    public Integer call()
            throws Exception
    {
        try {
            List<String> validRunNames = resultService.listRunNames();
            for (String run : runs) {
                checkState(validRunNames.contains(run), "Invalid run name: " + run);
            }

            List<String> queryNames = new ArrayList<>();
            if (isNotBlank(queries)) {
                queryNames = Arrays.stream(queries.split(",")).map(x -> x.trim()).collect(Collectors.toList());
            }

            TableStyle tableStyle = TableStyle.valueOf(format.toUpperCase(Locale.ROOT));
            if (runs.size() == 1) {
                new ViewSingleResultAction(runs.get(0), queryNames, normalize, tableStyle).act();
            }
            else {
                if (names == null) {
                    new CompareResultAction(
                            runs,
                            Optional.empty(),
                            queryNames,
                            normalize,
                            tableStyle).act();
                }
                else {
                    new CompareResultAction(
                            runs,
                            Optional.of(Arrays.stream(names.split(",")).collect(Collectors.toList())),
                            queryNames,
                            normalize,
                            tableStyle).act();
                }
            }
            return 0;
        }
        catch (Exception e) {
            drawError(e.getMessage());
            return 1;
        }
    }
}
