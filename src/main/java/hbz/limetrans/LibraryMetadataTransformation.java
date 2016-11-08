package hbz.limetrans;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.JsonToElasticsearchBulk;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;

import java.io.IOException;
import java.io.File;

public class LibraryMetadataTransformation {

    private final Settings mElasticsearchSettings;
    private final String mInputPath;
    private final String mOutputPath;
    private final String mRulesPath;

    private String mElasticsearchPath;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        final Settings inputQueue = aSettings.getGroups("input").get("queue");
        mInputPath = inputQueue.get("path").concat(inputQueue.get("pattern"));

        final Settings outputSettings = aSettings.getAsSettings("output");

        if (outputSettings.containsSetting("elasticsearch")) {
            mElasticsearchSettings = outputSettings.getAsSettings("elasticsearch");

            mElasticsearchPath = mElasticsearchSettings.get("path");
            if (mElasticsearchPath == null) {
                final File tempFile = File.createTempFile("limetrans", ".jsonl");
                mElasticsearchPath = tempFile.getPath();
                tempFile.deleteOnExit();
            }
        }
        else {
          mElasticsearchSettings = null;
          mElasticsearchPath = null;
        }

        mOutputPath = outputSettings.get("json");
        if (mOutputPath == null && mElasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        mRulesPath = aSettings.get("transformation-rules");
    }

    public void transform() {
        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final Metamorph morph = new Metamorph(mRulesPath);
        final JsonEncoder encoder = new JsonEncoder();
        final ObjectTee tee = new ObjectTee();

        // Setup transformation pipeline
        opener
                .setReceiver(decoder)
                .setReceiver(marcHandler)
                .setReceiver(morph)
                .setReceiver(encoder)
                .setReceiver(tee);

        if (mOutputPath != null) {
            //encoder.setPrettyPrinting(true);
            tee.addReceiver(new ObjectWriter<>(mOutputPath));
        }

        if (mElasticsearchSettings != null) {
            final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk("id",
                    mElasticsearchSettings.get("index.type"),
                    mElasticsearchSettings.get("index.name"));

            tee.addReceiver(esBulk);
            esBulk.setReceiver(new ObjectWriter<>(mElasticsearchPath));
        }

        // Process transformation
        opener.process(mInputPath);
        opener.closeStream();
    }

    public void index() throws IOException {
        if (mElasticsearchSettings != null) {
            final ElasticsearchProvider esProvider = new ElasticsearchProvider(mElasticsearchSettings);

            esProvider.initializeIndex();
            esProvider.bulkIndex(mElasticsearchPath);
            esProvider.close();
        }
    }

}
