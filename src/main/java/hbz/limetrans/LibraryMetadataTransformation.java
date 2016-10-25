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
import java.net.URL;

public final class LibraryMetadataTransformation {

    public static void main(final String[] args) throws IOException {

        Settings settings = setup(args);

        String transformedPath = transform(settings);

        index(settings, transformedPath);
    }

    private static void index(Settings aSettings, String aElasticsearchInputDataFile) throws IOException {
        final ElasticsearchProvider esProvider = new ElasticsearchProvider(aSettings.getAsSettings("output").getAsSettings("elasticsearch"));
        esProvider.initializeIndex(aSettings.get("output.elasticsearch.index.name"));
        esProvider.bulkIndex(aElasticsearchInputDataFile, aSettings.get("output.elasticsearch.index.name"));
    }

    private static String transform(Settings aSettings) {
        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final Metamorph morph = new Metamorph(aSettings.get("transformation-rules"));
        final JsonEncoder encoder = new JsonEncoder();
        encoder.setPrettyPrinting(true);
        final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk("id",
                aSettings.get("output.elasticsearch.index.type"),
                aSettings.get("output.elasticsearch.index.name"));
        final String outputFile = aSettings.get("output.json");
        final ObjectWriter<String> writer = new ObjectWriter<>(outputFile);

        // Setup transformation pipeline
        opener
                .setReceiver(decoder)
                .setReceiver(marcHandler)
                .setReceiver(morph)
                .setReceiver(encoder)
                .setReceiver(esBulk)
                .setReceiver(writer);

        // Process transformation
        Settings inputQueue = aSettings.getGroups("input").get("queue");
        String inputPath = inputQueue.get("path").concat(inputQueue.get("pattern"));
        opener.process(inputPath);
        opener.closeStream();
        return outputFile;
    }

    private static Settings setup(String[] args) throws IOException {
        URL configUrl = ConfigurationChecker.getConfigUrlFrom(args);
        return getSettings(configUrl);
    }

    private static Settings getSettings(URL aUrl) throws IOException {
        SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        Settings settings = Settings.settingsBuilder()
                .put(settingsLoader.load(IOUtils.toString(aUrl)))
                .replacePropertyPlaceholders()
                .build();
        return settings;
    }

}
