package hbz.limetrans;

import hbz.limetrans.util.FileQueue;

import org.culturegraph.mf.formeta.FormetaEncoder;
import org.culturegraph.mf.formeta.formatter.FormatterStyle;
import org.culturegraph.mf.io.ObjectWriter;
import org.culturegraph.mf.json.JsonEncoder;
import org.culturegraph.mf.mangling.RecordIdChanger;
import org.culturegraph.mf.metamorph.Metamorph;
import org.culturegraph.mf.plumbing.StreamTee;
import org.xbib.common.settings.Settings;

import java.io.IOException;

public class LibraryMetadataTransformation {

    private final FileQueue mInputQueue;
    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final boolean mNormalizeUnicode;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        mInputQueue = new FileQueue(aSettings.getGroups("input").get("queue"));

        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: no input specified.");
        }

        final Settings outputSettings = aSettings.getAsSettings("output");

        mElasticsearchSettings = outputSettings.containsSetting("elasticsearch") ?
            outputSettings.getAsSettings("elasticsearch") : null;
        mFormetaPath = outputSettings.get("formeta");
        mJsonPath = outputSettings.get("json");

        if (mFormetaPath == null && mJsonPath == null && mElasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        mNormalizeUnicode = aSettings.getAsBoolean("normalize-unicode", true);
        mRulesPath = aSettings.get("transformation-rules");
    }

    public void process() {
        final Metamorph metamorph = new Metamorph(mRulesPath);
        final StreamTee streamTee = new StreamTee();

        transformJson(streamTee);
        transformFormeta(streamTee);
        transformElasticsearch(streamTee);

        metamorph.setReceiver(streamTee);
        mInputQueue.processMarcXml(metamorph, mNormalizeUnicode);
    }

    public int getInputQueueSize() {
        return mInputQueue.size();
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

    private void transformJson(final StreamTee aTee) {
        if (mJsonPath == null) {
            return;
        }

        final JsonEncoder jsonEncoder = new JsonEncoder();

        aTee.addReceiver(jsonEncoder);
        jsonEncoder.setReceiver(new ObjectWriter<>(mJsonPath));
    }

    private void transformElasticsearch(final StreamTee aTee) {
        if (mElasticsearchSettings == null) {
            return;
        }

        final RecordIdChanger recordIdChanger = new RecordIdChanger();
        final String idKey = mElasticsearchSettings.get("index.idKey");

        if (idKey != null) {
            recordIdChanger.setIdLiteral(idKey);
            recordIdChanger.setKeepIdLiteral(true);
        }

        final ElasticsearchIndexer elasticsearchIndexer =
            new ElasticsearchIndexer(mElasticsearchSettings.getAsMap());

        aTee.addReceiver(recordIdChanger);
        recordIdChanger.setReceiver(elasticsearchIndexer);
    }

}
