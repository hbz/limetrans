package hbz.limetrans;

import hbz.limetrans.util.Helpers;

import org.apache.commons.validator.routines.UrlValidator;
import org.xbib.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

public final class Main {

    private static final String[] PROTOCOLS = new String[]{"http", "https", "ftp", "file"};

    private Main() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] args) throws IOException {
        new LibraryMetadataTransformation(setup(args)).process();
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

        if (new UrlValidator(PROTOCOLS).isValid(arg)) {
            return Helpers.loadSettings(new URL(arg));
        }
        else {
            final File file = new File(arg);
            if (!file.exists()) {
                throw new IllegalArgumentException("Could not process limetrans: invalid configuration argument: ".concat(arg));
            }
            else {
                return Helpers.loadSettings(file);
            }
        }
    }

}
