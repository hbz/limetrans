package hbz.limetrans;

import org.apache.commons.io.IOUtils;
import org.xbib.common.settings.Settings;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Helpers {

    public static Settings getSettingsFromUrl(final URL aUrl) throws IOException {
        final SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        final Settings settings = Settings.settingsBuilder()
            .put(settingsLoader.load(IOUtils.toString(aUrl, Charset.defaultCharset())))
            .replacePropertyPlaceholders()
            .build();

        return settings;
    }

    public static String slurpFile(final String aPath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(aPath)));
    }

}
