package hbz.limetrans.filter;

import hbz.limetrans.util.Cli;

import java.io.IOException;

public final class Main {

    public static void main(final String[] aArgs) throws IOException, Cli.CliException {
        final Cli cli = new Cli(Main.class, "FILE...")
            .addOption("f", "filter", true, "Filter condition")
            .addOption("h", "help", false, "Print help output")
            .addOption("o", "output", true, "Output file (default: STDOUT)");

        if (cli.parse(aArgs)) {
          new LibraryMetadataFilter(cli.getAsSettings("input")).process();
        }
    }

}
