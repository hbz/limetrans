package hbz.limetrans.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

public class Cli {

    private final Options mOptions = new Options();
    private final String mArgsLine;
    private final String mProgram;

    private CommandLine mCommandLine;
    private boolean mHasOptions;

    public Cli(final String aProgram, final String aArgsLine) {
        mProgram = aProgram;
        mArgsLine = aArgsLine;
    }

    public Cli(final Class<?> aProgram, final String aArgsLine) {
        this(aProgram.getName(), aArgsLine);
    }

    public Cli addOption(final String aOpt, final String aLongOpt,
            final boolean aHasArg, final String aDescription) {
        mOptions.addOption(aOpt, aLongOpt, aHasArg, aDescription);
        mHasOptions = true;
        return this;
    }

    public String getHelp() {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        printHelp(output);

        return output.toString();
    }

    public void printHelp(final OutputStream aOutputStream) {
        final PrintWriter pw = new PrintWriter(aOutputStream);

        String cmdLineSyntax = mProgram;
        if (mArgsLine != null) {
            cmdLineSyntax += " " + mArgsLine;
        }

        final int helpWidth = 80;
        final int helpDescPad = 3;

        new HelpFormatter().printHelp(pw, helpWidth, cmdLineSyntax,
                mHasOptions ? "\nOptions:" : null, mOptions, 1, helpDescPad, null, true);

        pw.close();
    }

    public boolean parse(final String[] aArgs) throws CliException {
        try {
            mCommandLine = new DefaultParser().parse(mOptions, aArgs);
        }
        catch (final ParseException e) {
            throw new CliException(this, e);
        }

        if (mCommandLine.hasOption("h")) {
            printHelp(System.out);
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
            settingsBuilder.put(new String[]{aArgsKey}, getArgs());
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

        private static final long serialVersionUID = 1587965155138860229L;

        public CliException(final Cli aCli, final ParseException aCause) {
            super(aCause.getMessage() + "\n" + aCli.getHelp());
        }

    }

}
