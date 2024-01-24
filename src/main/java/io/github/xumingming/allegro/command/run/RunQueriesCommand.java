package io.github.xumingming.allegro.command.run;

import io.github.xumingming.allegro.AllegroConf;
import io.github.xumingming.allegro.service.ConfigService;
import io.github.xumingming.allegro.service.ResultService;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.drawSuiteConf;
import static io.github.xumingming.allegro.Utils.isBlank;
import static io.github.xumingming.beauty.Beauty.drawError;
import static io.github.xumingming.beauty.Beauty.drawH1Title;

@CommandLine.Command(
        name = "run",
        description = "Run queries.",
        subcommands = CommandLine.HelpCommand.class)
public class RunQueriesCommand
        implements Callable<Integer>
{
    @CommandLine.Option(
            names = {"-q", "--queries"},
            description = "the set of queries to execute. e.g. can be a dir(e.g. /tmp/queries) or allegro builtin queries(e.g. allegro:tpch)")
    private String querySet;

    @CommandLine.Parameters(
            index = "0",
            defaultValue = "test",
            description = "the name for current run")
    private String runName;

    @CommandLine.Option(
            names = {"-d", "--db"},
            description = "the db to run against")
    private String dbName;

    private ConfigService configService = ConfigService.create();
    private ResultService resultService = ResultService.create();

    @Override
    public Integer call()
            throws Exception
    {
        try {
            AllegroConf allegroConf = initAndValidateAllegroConf();
            drawH1Title("Allegro Conf");
            drawSuiteConf(allegroConf.getBaseline());
            Runner runner = new Runner(allegroConf.getBaseline(), allegroConf.getBaseline().isFetchQueryDetail());
            runner.run();
        }
        catch (Exception e) {
            drawError(e.getMessage());
            return 1;
        }

        return 0;
    }

    protected AllegroConf initAndValidateAllegroConf()
    {
        AllegroConf allegroConf = configService.readAllegroConf();
        checkState(!isBlank(runName), "runName is not specified!");
        checkState(!resultService.listRunNames().contains(runName), "runName: " + runName + " is already used!");
        allegroConf.setRunName(runName);

        if (this.querySet != null) {
            allegroConf.getBaseline().setQuerySet(this.querySet);
        }

        // Validate config.
        String error = allegroConf.initAndValidate(allegroConf.getRunName(), dbName);
        if (error != null) {
            drawError(error);
            System.exit(1);
        }
        return allegroConf;
    }
}
