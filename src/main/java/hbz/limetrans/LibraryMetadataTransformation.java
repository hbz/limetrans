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
import org.culturegraph.mf.stream.pipe.StreamUnicodeNormalizer;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;
import org.xbib.util.Finder.PathFile;
import org.xbib.util.Finder;

import java.io.File;
import java.io.IOException;
import java.util.Queue;

public class LibraryMetadataTransformation {

    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final Queue<PathFile> mInputQueue;
    private final String mJsonPath;
    private final String mRulesPath;
    private final boolean mIsUpdate;

    private String mElasticsearchPath;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        mInputQueue = prepareInputQueue(aSettings.getGroups("input").get("queue"));
        if (mInputQueue == null) {
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

        mRulesPath = aSettings.get("transformation-rules");
    }

    public void transform() {
        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final StreamUnicodeNormalizer normalizer = new StreamUnicodeNormalizer();
        final Metamorph morph = new Metamorph(mRulesPath);
        final StreamTee streamTee = new StreamTee();

        opener
            .setReceiver(decoder)
            .setReceiver(marcHandler)
            .setReceiver(normalizer)
            .setReceiver(morph)
            .setReceiver(streamTee);

        final ObjectTee objectTee = prepareJson(streamTee);

        transformJson(objectTee);
        transformFormeta(streamTee);
        transformElasticsearch(objectTee);

        for (final PathFile pathFile : mInputQueue) {
            opener.process(pathFile.toString());
        }

        opener.closeStream();
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

    private Queue<PathFile> prepareInputQueue(final Settings inputSettings) throws IOException {
        if (inputSettings == null) {
          return null;
        }

        final String path = inputSettings.get("path");
        final String pattern = inputSettings.get("pattern");

        if (path == null || pattern == null) {
          return null;
        }

        final Queue<PathFile> inputQueue = new Finder().find(
                inputSettings.get("base"), inputSettings.get("basepattern"), path, pattern)
            .sortBy(inputSettings.get("sort_by", "lastmodified"))
            .order(inputSettings.get("order", "asc"))
            .getPathFiles(inputSettings.getAsInt("max", -1));

        return inputQueue.isEmpty() ? null : inputQueue;
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

        final JsonToElasticsearchBulk esBulk = new JsonToElasticsearchBulk("id",
                mElasticsearchSettings.get("index.type"),
                mElasticsearchSettings.get("index.name"));

        aTee.addReceiver(esBulk);
        esBulk.setReceiver(new ObjectWriter<>(mElasticsearchPath));
    }

}
