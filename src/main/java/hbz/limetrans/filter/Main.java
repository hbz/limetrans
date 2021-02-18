package hbz.limetrans.filter;

import hbz.limetrans.util.Cli;

import java.io.IOException;

public final class Main {

    private Main() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] aArgs) throws IOException, Cli.CliException {
        final Cli cli = new Cli(Main.class, "FILE...")
            .addOption("f", "filter",    "Filter condition", true)
            .addOption("h", "help",      "Print help output")
            .addOption("k", "key",       "Key field (default: 001)", false)
            .addOption("O", "operator",  "Logical operator (all, any (default), none)", false)
            .addOption("o", "output",    "Output file (default: STDOUT)", false)
            .addOption("P", "pretty",    "Pretty-print JSON output")
            .addOption("p", "processor", "Input processor (default: MARCXML)", false);

        if (cli.parse(aArgs)) {
            new LibraryMetadataFilter(cli.getAsSettings("input")).process();
        }
    }

}
