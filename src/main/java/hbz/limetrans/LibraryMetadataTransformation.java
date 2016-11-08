package hbz.limetrans;

import org.apache.commons.io.IOUtils;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.JsonToElasticsearchBulk;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;
import org.xbib.common.settings.loader.SettingsLoader;
import org.xbib.common.settings.loader.SettingsLoaderFactory;

import java.io.IOException;
import java.io.File;
import java.net.URL;

public final class LibraryMetadataTransformation {

    public static void main(final String[] args) throws IOException {
        final Settings settings = setup(args);

        final Settings inputQueue = settings.getGroups("input").get("queue");
        final String inputPath = inputQueue.get("path").concat(inputQueue.get("pattern"));

        final Settings outputSettings = settings.getAsSettings("output");

        Settings elasticsearchSettings = null;
        String esPath = null;
        if (outputSettings.containsSetting("elasticsearch")) {
            elasticsearchSettings = outputSettings.getAsSettings("elasticsearch");

            esPath = elasticsearchSettings.get("path");
            if (esPath == null) {
                final File tempFile = File.createTempFile("limetrans", ".jsonl");
                esPath = tempFile.getPath();
                tempFile.deleteOnExit();
            }
        }

        final String outputPath = outputSettings.get("json");
        if (outputPath == null && elasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        final String rulesPath = settings.get("transformation-rules");
        transform(inputPath, outputPath, rulesPath, elasticsearchSettings, esPath);

        if (elasticsearchSettings != null) {
            index(elasticsearchSettings, esPath);
        }
    }

    private static void index(final Settings aSettings, final String aInputPath) throws IOException {
        final ElasticsearchProvider esProvider = new ElasticsearchProvider(aSettings);
        final String indexName = aSettings.get("index.name");

        esProvider.initializeIndex(indexName);
        esProvider.bulkIndex(aInputPath, indexName);
        esProvider.close();
    }

    private static void transform(final String aInputPath, final String aOutputPath,
            final String aRulesPath, final Settings aElasticsearchSettings, final String esPath) {
        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final Metamorph morph = new Metamorph(aRulesPath);
        final JsonEncoder encoder = new JsonEncoder();
        final ObjectTee tee = new ObjectTee();

        // Setup transformation pipeline
        opener
                .setReceiver(decoder)
                .setReceiver(marcHandler)
                .setReceiver(morph)
                .setReceiver(encoder)
                .setReceiver(tee);

        if (aOutputPath != null) {
            //encoder.setPrettyPrinting(true);
            tee.addReceiver(new ObjectWriter<>(aOutputPath));
        }

        if (aElasticsearchSettings != null) {
            final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk("id",
                    aElasticsearchSettings.get("index.type"),
                    aElasticsearchSettings.get("index.name"));

            tee.addReceiver(esBulk);
            esBulk.setReceiver(new ObjectWriter<>(esPath));
        }

        // Process transformation
        opener.process(aInputPath);
        opener.closeStream();
    }

    private static Settings setup(final String[] args) throws IOException {
        URL configUrl = ConfigurationChecker.getConfigUrlFrom(args);
        return getSettings(configUrl);
    }

    private static Settings getSettings(final URL aUrl) throws IOException {
        SettingsLoader settingsLoader = SettingsLoaderFactory.loaderFromResource(aUrl.toString());

        Settings settings = Settings.settingsBuilder()
                .put(settingsLoader.load(IOUtils.toString(aUrl)))
                .replacePropertyPlaceholders()
                .build();
        return settings;
    }

}
