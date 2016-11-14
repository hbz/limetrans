package hbz.limetrans;

import org.apache.commons.io.IOUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.xbib.common.settings.Settings;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.IOException;
import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class Main {

    private static final String[] PROTOCOLS = new String[] {"http", "https", "ftp", "file"};

    public static void main(final String[] args) throws IOException {
        final LibraryMetadataTransformation limetrans =
            new LibraryMetadataTransformation(setup(args));

        limetrans.transform();
        limetrans.index();
    }

    private static Settings setup(final String[] aArgs) throws IOException {
        URL configUrl;

        if (aArgs.length < 1) {
            throw new IllegalArgumentException("Could not process limetrans: configuration missing.");
        }

        if (aArgs.length > 1) {
            throw new IllegalArgumentException("Could not process limetrans: too many arguments: ".concat(aArgs.toString()));
        }

        final String arg = aArgs[0].trim();
        if (arg.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: empty configuration argument.");
        }

        if (new UrlValidator(PROTOCOLS).isValid(arg)) {
            configUrl = new URL(arg);
        }
        else {
            final File file = new File(arg);
            if (!file.exists()) {
                throw new IllegalArgumentException("Could not process limetrans: invalid configuration argument: ".concat(arg));
            }
            else {
                configUrl = file.toURI().toURL();
            }
        }

        return getSettings(configUrl);
    }

    private static Settings getSettings(final URL aUrl) throws IOException {
        final SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        final Settings settings = Settings.settingsBuilder()
                .put(settingsLoader.load(IOUtils.toString(aUrl, Charset.defaultCharset())))
                .replacePropertyPlaceholders()
                .build();

        return settings;
    }

}
