package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import java.io.IOException;
import java.util.Arrays;

public final class Main {

    private Main() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] aArgs) throws IOException {
        new Limetrans(setup(aArgs)).process();
    }

    private static Settings setup(final String[] aArgs) throws IOException {
        if (aArgs.length < 1) {
            throw new IllegalArgumentException("Could not process limetrans: configuration missing.");
        }

        if (aArgs.length > 1) {
            throw new IllegalArgumentException("Could not process limetrans: too many arguments: ".concat(Arrays.toString(aArgs)));
        }

        final String arg = aArgs[0].trim();
        if (arg.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: empty configuration argument.");
        }

        return Helpers.loadSettings(arg);
    }

}
