package hbz.limetrans;

import org.apache.commons.io.IOUtils;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.JsonToElasticsearchBulk;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

public final class LibraryMetadataTransformation {

    public static void main(final String[] args) throws IOException {

        URL configUrl = ConfigurationChecker.getConfigUrlFrom(args);
        Settings settings = getSettings(configUrl);

        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final Metamorph morph = new Metamorph(settings.get("transformation-rules"));
        final JsonEncoder encoder = new JsonEncoder();
        encoder.setPrettyPrinting(true);
        final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk("id",
                settings.get("output.elasticsearch.index.type"),
                settings.get("output.elasticsearch.index.name"));
        final ObjectWriter<String> writer = new ObjectWriter<>(settings.get("output.json"));

        // Setup transformation pipeline
        opener
                .setReceiver(decoder)
                .setReceiver(marcHandler)
                .setReceiver(morph)
                .setReceiver(encoder)
                .setReceiver(esBulk)
                .setReceiver(writer);

        // Process transformation
        String input = null; // TODO: = settings.get("input") ==> to unified String
        opener.process(input);
        opener.closeStream();
    }

    private static Settings getSettings(URL aUrl) throws IOException {
        InputStream in = aUrl.openStream();
        Reader reader = new InputStreamReader(in, "UTF-8");
        SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        Settings settings = Settings.settingsBuilder()
                .put(settingsLoader.load(IOUtils.toString(aUrl)))
                .replacePropertyPlaceholders()
                .build();
        return settings;
    }

}
