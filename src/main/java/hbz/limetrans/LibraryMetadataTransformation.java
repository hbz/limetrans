package hbz.limetrans;

import hbz.limetrans.util.FileQueue;

import org.culturegraph.mf.formeta.formatter.FormatterStyle;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.FormetaEncoder;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.JsonToElasticsearchBulk;
import org.culturegraph.mf.stream.pipe.ObjectTee;
import org.culturegraph.mf.stream.pipe.StreamTee;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.xbib.common.settings.Settings;

import java.io.File;
import java.io.IOException;

public class LibraryMetadataTransformation {

    private final FileQueue mInputQueue;
    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final boolean mIsUpdate;
    private final boolean mNormalizeUnicode;

    private String mElasticsearchPath;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        mInputQueue = new FileQueue(aSettings.getGroups("input").get("queue"));
        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: no input specified.");
        }

        final Settings outputSettings = aSettings.getAsSettings("output");

        if (outputSettings.containsSetting("elasticsearch")) {
            mElasticsearchSettings = outputSettings.getAsSettings("elasticsearch");

            mElasticsearchPath = mElasticsearchSettings.get("path");
            if (mElasticsearchPath == null) {
                try {
                    final File tempFile = File.createTempFile("limetrans", ".jsonl");
                    mElasticsearchPath = tempFile.getPath();
                    tempFile.deleteOnExit();
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to create temporary file", e);
                }
            }

            mIsUpdate = mElasticsearchSettings.getAsBoolean("update", false);
        }
        else {
            mElasticsearchSettings = null;
            mElasticsearchPath = null;
            mIsUpdate = false;
        }

        mFormetaPath = outputSettings.get("formeta");
        mJsonPath = outputSettings.get("json");
        if (mFormetaPath == null && mJsonPath == null && mElasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        mNormalizeUnicode = aSettings.getAsBoolean("normalize-unicode", true);
        mRulesPath = aSettings.get("transformation-rules");
    }

    public void transform() {
        final Metamorph morph = new Metamorph(mRulesPath);
        final StreamTee streamTee = new StreamTee();
        final ObjectTee objectTee = prepareJson(streamTee);

        transformJson(objectTee);
        transformFormeta(streamTee);
        transformElasticsearch(objectTee);

        morph.setReceiver(streamTee);
        mInputQueue.processMarcXml(morph, mNormalizeUnicode);
    }

    public void index() throws IOException {
        if (mElasticsearchSettings == null) {
            return;
        }

        final ElasticsearchProvider esProvider = new ElasticsearchProvider(mElasticsearchSettings);

        try {
            if (mIsUpdate) {
                esProvider.checkIndex();
            }
            else {
                esProvider.initializeIndex();
            }

            esProvider.bulkIndex(mElasticsearchPath);
        }
        finally {
            esProvider.close();
        }
    }

    public int getInputQueueSize() {
        return mInputQueue.size();
    }

    private ObjectTee prepareJson(final StreamTee aTee) {
        if (mJsonPath == null && mElasticsearchSettings == null) {
            return null;
        }

        final JsonEncoder jsonEncoder = new JsonEncoder();
        final ObjectTee objectTee = new ObjectTee();

        aTee.addReceiver(jsonEncoder);
        jsonEncoder.setReceiver(objectTee);

        return objectTee;
    }

    private void transformFormeta(final StreamTee aTee) {
        if (mFormetaPath == null) {
            return;
        }

        final FormetaEncoder formetaEncoder = new FormetaEncoder();

        aTee.addReceiver(formetaEncoder);
        formetaEncoder.setStyle(FormatterStyle.MULTILINE);
        formetaEncoder.setReceiver(new ObjectWriter<>(mFormetaPath));
    }

    private void transformJson(final ObjectTee aTee) {
        if (mJsonPath == null) {
            return;
        }

        aTee.addReceiver(new ObjectWriter<>(mJsonPath));
    }

    private void transformElasticsearch(final ObjectTee aTee) {
        if (mElasticsearchSettings == null) {
            return;
        }

        final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk(
                mElasticsearchSettings.get("index.idKey", "_id"),
                mElasticsearchSettings.get("index.type"),
                mElasticsearchSettings.get("index.name"),
                ".");

        aTee.addReceiver(esBulk);
        esBulk.setReceiver(new ObjectWriter<>(mElasticsearchPath));
    }

}
