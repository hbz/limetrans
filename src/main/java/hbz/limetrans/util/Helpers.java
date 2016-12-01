package hbz.limetrans.util;

import org.apache.commons.io.IOUtils;
import org.xbib.common.settings.Settings;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

public class Helpers {

    public static Settings loadSettings(final URL aUrl) throws IOException {
        final SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        final Settings settings = Settings.settingsBuilder()
            .put(settingsLoader.load(slurpFile(aUrl)))
            .replacePropertyPlaceholders()
            .build();

        return settings;
    }

    public static String slurpFile(final URL aUrl) throws IOException {
        return IOUtils.toString(aUrl, Charset.defaultCharset());
    }

    public static String slurpFile(final String aPath) throws IOException {
        return slurpFile(new File(aPath).toURI().toURL());
    }

    public static String slurpFile(final String aPath, final Class aClass) throws IOException {
        return slurpFile(getResource(aClass, aPath));
    }

    public static URL getResource(final Class aClass, final String aPath) throws IOException {
        final URL url = aClass.getResource(aPath);
        if (url == null) {
            throw new FileNotFoundException("Resource not found for " + aClass.toString() + ": " + aPath);
        }
        else {
            return url;
        }
    }

}
