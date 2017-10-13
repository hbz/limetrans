package hbz.limetrans;

import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Helpers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.formeta.FormetaEncoder;
import org.metafacture.formeta.formatter.FormatterStyle;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.mangling.RecordIdChanger;
import org.metafacture.metamorph.Metamorph;
import org.metafacture.plumbing.StreamTee;
import org.metafacture.statistics.Counter;
import org.xbib.common.settings.Settings;

import java.io.IOException;

public class LibraryMetadataTransformation {

    private static final Logger mLogger = LogManager.getLogger();

    private final FileQueue mInputQueue;
    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final boolean mNormalizeUnicode;
    private final boolean mPrettyPrinting;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        mLogger.debug("Settings: {}", aSettings.getAsMap());

        mInputQueue = new FileQueue(aSettings.getGroups("input").get("queue"));

        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: no input specified.");
        }

        final Settings outputSettings = aSettings.getAsSettings("output");
        mPrettyPrinting = outputSettings.getAsBoolean("pretty-printing", false);

        mElasticsearchSettings = outputSettings.containsSetting("elasticsearch") ?
            outputSettings.getAsSettings("elasticsearch") : null;
        mFormetaPath = outputSettings.get("formeta");
        mJsonPath = outputSettings.get("json");

        if (mFormetaPath == null && mJsonPath == null && mElasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        mNormalizeUnicode = aSettings.getAsBoolean("normalize-unicode", true);
        mRulesPath = Helpers.getPath(aSettings.get("transformation-rules"), getClass());
    }

    public void process() {
        mLogger.info("Starting transformation: {}", mRulesPath);

        final Metamorph metamorph = new Metamorph(mRulesPath);
        final StreamTee streamTee = new StreamTee();
        final Counter counter = new Counter();

        transformJson(streamTee);
        transformFormeta(streamTee);
        transformElasticsearch(streamTee);

        metamorph
            .setReceiver(counter)
            .setReceiver(streamTee);

        mInputQueue.process(metamorph, mNormalizeUnicode);

        mLogger.info("Finished transformation ({})", counter);
    }

    public int getInputQueueSize() {
        return mInputQueue.size();
    }

    private void transformFormeta(final StreamTee aTee) {
        if (mFormetaPath == null) {
            return;
        }

        mLogger.info("Writing Formeta file: {}", mFormetaPath);

        final FormetaEncoder formetaEncoder = new FormetaEncoder();
        formetaEncoder.setStyle(mPrettyPrinting ?
                FormatterStyle.MULTILINE : FormatterStyle.VERBOSE);

        aTee.addReceiver(formetaEncoder);
        formetaEncoder.setReceiver(new ObjectWriter<>(mFormetaPath));
    }

    private void transformJson(final StreamTee aTee) {
        if (mJsonPath == null) {
            return;
        }

        mLogger.info("Writing JSON file: {}", mJsonPath);

        final JsonEncoder jsonEncoder = new JsonEncoder();
        jsonEncoder.setPrettyPrinting(mPrettyPrinting);

        aTee.addReceiver(jsonEncoder);
        jsonEncoder.setReceiver(new ObjectWriter<>(mJsonPath));
    }

    private void transformElasticsearch(final StreamTee aTee) {
        if (mElasticsearchSettings == null) {
            return;
        }

        mLogger.info("Indexing into Elasticsearch: {}", mElasticsearchSettings.getAsMap());

        final RecordIdChanger recordIdChanger = new RecordIdChanger();
        final String idKey = mElasticsearchSettings.get("index.idKey");

        if (idKey != null) {
            recordIdChanger.setIdLiteral(idKey);
            recordIdChanger.setKeepIdLiteral(true);
        }

        final ElasticsearchIndexer elasticsearchIndexer =
            new ElasticsearchIndexer(Helpers.convertSettings(mElasticsearchSettings));

        aTee.addReceiver(recordIdChanger);
        recordIdChanger.setReceiver(elasticsearchIndexer);
    }

}
