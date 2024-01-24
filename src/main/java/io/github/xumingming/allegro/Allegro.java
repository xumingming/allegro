package io.github.xumingming.allegro;

import io.github.xumingming.allegro.command.ListRunsCommand;
import io.github.xumingming.allegro.command.run.RunQueriesCommand;
import io.github.xumingming.allegro.command.view.ViewResultCommand;
import picocli.CommandLine;

import static io.github.xumingming.beauty.Beauty.drawError;

// TODO: support specify session property
@CommandLine.Command(
        name = "Allegro",
        description = "Run & Analyze queries on Presto.",
        subcommands = {
                CommandLine.HelpCommand.class,
                ViewResultCommand.class,
                ListRunsCommand.class,
                RunQueriesCommand.class})
public class Allegro
{
    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "display this help message")
    boolean usageHelpRequested;

    private Allegro()
    {}

    public static void main(String[] args)
    {
        CommandLine commandLine = new CommandLine(new Allegro());
        commandLine.parseArgs(args);
        if (commandLine.isUsageHelpRequested()) {
            commandLine.usage(System.out);
            return;
        }

        try {
            int exitCode = commandLine.execute(args);
            System.exit(exitCode);
        }
        catch (Exception e) {
            // Something wrong.
            drawError(e.getMessage());
            System.exit(1);
        }
    }
}
