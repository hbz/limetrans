package hbz.limetrans.filter;

import hbz.limetrans.util.Cli;

import java.io.IOException;

public final class Main {

    private Main() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] aArgs) throws IOException, Cli.CliException {
        final Cli cli = new Cli(Main.class, "FILE...")
            .addOption("f", "filter", true, "Filter condition")
            .addOption("h", "help", false, "Print help output")
            .addOption("k", "key", true, "Key field (default: 001)")
            .addOption("O", "operator", true, "Logical operator (all, any (default), none)")
            .addOption("o", "output", true, "Output file (default: STDOUT)")
            .addOption("P", "pretty", false, "Pretty-print JSON output")
            .addOption("p", "processor", true, "Input processor (default: MARCXML)");

        if (cli.parse(aArgs)) {
            new LibraryMetadataFilter(cli.getAsSettings("input")).process();
        }
    }

}
