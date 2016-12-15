package hbz.limetrans.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.xbib.common.settings.Settings;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

public class Cli {

    private final Options mOptions = new Options();
    private final String mArgsLine;
    private final String mProgram;

    private CommandLine mCommandLine = null;
    private boolean mHasOptions = false;

    public Cli(final String aProgram, final String aArgsLine) {
        mProgram = aProgram;
        mArgsLine = aArgsLine;
    }

    public Cli(final Class program, final String aArgsLine) {
        this(program.getName(), aArgsLine);
    }

    public Cli addOption(final String opt, final String longOpt,
            final boolean hasArg, final String description) {
        mOptions.addOption(opt, longOpt, hasArg, description);
        mHasOptions = true;
        return this;
    }

    public String getHelp() {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final PrintWriter pw = new PrintWriter(output);

        String cmdLineSyntax = mProgram;
        if (mArgsLine != null) {
          cmdLineSyntax += " " + mArgsLine;
        }

        final String header = mHasOptions ? "\nOptions:" : null;
        final String footer = null;

        final int width = 80;
        final int leftPad = 1;
        final int descPad = 3;

        final boolean autoUsage = true;

        new HelpFormatter().printHelp(pw, width, cmdLineSyntax,
                header, mOptions, leftPad, descPad, footer, autoUsage);

        pw.close();

        return output.toString();
    }

    public boolean parse(final String[] aArgs) throws CliException {
        String error = null;

        try {
            mCommandLine = new DefaultParser().parse(mOptions, aArgs);
        }
        catch (ParseException e) {
            throw new CliException(this, e);
        }

        if (mCommandLine.hasOption("h")) {
            System.out.println(getHelp());
            return false;
        }

        return true;
    }

    public Settings getAsSettings(final String aArgsKey) {
        final Settings.Builder settingsBuilder = Settings.settingsBuilder();

        for (final Option option : mCommandLine.getOptions()) {
            settingsBuilder.put(option.getLongOpt(), option.getValue());
        }

        if (aArgsKey != null) {
            settingsBuilder.putArray(aArgsKey, getArgs());
        }

        return settingsBuilder.build();
    }

    public Settings getAsSettings() {
        return getAsSettings(null);
    }

    public String[] getArgs() {
        return mCommandLine.getArgs();
    }

    public class CliException extends Exception {

        public CliException(final Cli aCli, final ParseException aCause) {
            super(aCause.getMessage() + "\n" + aCli.getHelp());
        }

    }

}
