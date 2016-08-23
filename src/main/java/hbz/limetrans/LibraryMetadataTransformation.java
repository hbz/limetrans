package hbz.limetrans;

import org.apache.commons.validator.routines.UrlValidator;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.source.FileOpener;

import org.xbib.common.settings.*;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

public final class LibraryMetadataTransformation {

    public static void main(final String[] args) throws IOException {

        String configUrl = getConfigUrl(args);
        Settings settings = getSettings(configUrl);

        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final Metamorph morph = new Metamorph(settings.get("transformation-rules"));

        // Setup transformation pipeline
        opener
                .setReceiver(decoder)
                .setReceiver(marcHandler)
                .setReceiver(morph);

        // Process transformation
        String input = null; // TODO: = settings.get("input") ==> to unified String
        opener.process(input);
        opener.closeStream();
    }

    private static Settings getSettings(String aUrl) throws IOException {
        InputStream in = System.in;
        try {
            URL url = new URL(aUrl);
            in = url.openStream();
        } catch (MalformedURLException e) {
            in = new FileInputStream(aUrl);
        }
        Reader reader = new InputStreamReader(in, "UTF-8");
        SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl);
        Settings settings = Settings.settingsBuilder()
                .put(settingsLoader.load(Settings.copyToString(reader)))
                .replacePropertyPlaceholders()
                .build();
        return settings;
    }

    private static String getConfigUrl(String[] aArgs) {
        if (aArgs.length < 1){
            throw new IllegalArgumentException("Could not process limetrans: configuration missing.");
        }
        if (aArgs.length > 1){
            throw new IllegalArgumentException("Could not process limetrans: too many arguments: ".concat(aArgs.toString()));
        }
        String trimmed = aArgs[0].trim();
        if (trimmed.isEmpty()){
            throw new IllegalArgumentException("Could not process limetrans: empty configuration argument.");
        }
        if (!new UrlValidator(new String[] {"http", "https", "ftp", "file"}).isValid(trimmed)){
            throw new IllegalArgumentException("Could not process limetrans: invalid configuration argument: ".concat(trimmed));
        }
        return trimmed;
    }

}
