package hbz.limetrans;

import org.culturegraph.mf.formeta.formatter.FormatterStyle;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.FormetaEncoder;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.JsonToElasticsearchBulk;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;

import java.io.IOException;
import java.io.File;

public class LibraryMetadataTransformation {

    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mInputPath;
    private final String mJsonPath;
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

        mFormetaPath = outputSettings.get("formeta");
        mJsonPath = outputSettings.get("json");
        if (mFormetaPath == null && mJsonPath == null && mElasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        mRulesPath = aSettings.get("transformation-rules");
    }

    public void transform() {
        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final Metamorph morph = new Metamorph(mRulesPath);
        final StreamTee streamTee = new StreamTee();

        opener
                .setReceiver(decoder)
                .setReceiver(marcHandler)
                .setReceiver(morph)
                .setReceiver(streamTee);

        if (mFormetaPath != null) {
            final FormetaEncoder formetaEncoder = new FormetaEncoder();

            streamTee.addReceiver(formetaEncoder);
            formetaEncoder.setStyle(FormatterStyle.MULTILINE);
            formetaEncoder.setReceiver(new ObjectWriter<>(mFormetaPath));
        }

        if (mJsonPath != null || mElasticsearchSettings != null) {
            final JsonEncoder jsonEncoder = new JsonEncoder();
            final ObjectTee objectTee = new ObjectTee();

            streamTee.addReceiver(jsonEncoder);
            jsonEncoder.setReceiver(objectTee);

            if (mJsonPath != null) {
                //jsonEncoder.setPrettyPrinting(true);
                objectTee.addReceiver(new ObjectWriter<>(mJsonPath));
            }

            if (mElasticsearchSettings != null) {
                final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk("id",
                        mElasticsearchSettings.get("index.type"),
                        mElasticsearchSettings.get("index.name"));

                objectTee.addReceiver(esBulk);
                esBulk.setReceiver(new ObjectWriter<>(mElasticsearchPath));
            }
        }

        opener.process(mInputPath);
        opener.closeStream();
    }

    public void index() throws IOException {
        if (mElasticsearchSettings != null) {
            final ElasticsearchProvider esProvider = new ElasticsearchProvider(mElasticsearchSettings);

            try {
                esProvider.initializeIndex();
                esProvider.bulkIndex(mElasticsearchPath);
            }
            finally {
                esProvider.close();
            }
        }
    }

}
