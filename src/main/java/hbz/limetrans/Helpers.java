package hbz.limetrans;

import org.apache.commons.io.IOUtils;
import org.xbib.common.settings.Settings;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * Created by boeselager on 22.11.16.
 */
public class Helpers {

    public static Settings getSettingsFromUrl(final URL aUrl) throws IOException {
        final SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        final Settings settings = Settings.settingsBuilder()
                .put(settingsLoader.load(IOUtils.toString(aUrl, Charset.defaultCharset())))
                .replacePropertyPlaceholders()
                .build();

        return settings;
    }
}
