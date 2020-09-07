package hbz.limetrans;

import hbz.limetrans.filter.LibraryMetadataFilter;
import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Helpers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.formeta.FormetaEncoder;
import org.metafacture.formeta.formatter.FormatterStyle;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.mangling.RecordIdChanger;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.Metamorph;
import org.metafacture.plumbing.StreamTee;
import org.metafacture.statistics.Counter;
import org.xbib.common.settings.Settings;

import java.io.IOException;

public class LibraryMetadataTransformation { // checkstyle-disable-line ClassDataAbstractionCoupling

    private static final Logger LOGGER = LogManager.getLogger();

    private final FileQueue mInputQueue;
    private final Settings mElasticsearchSettings;
    private final String mFilterOperator;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final String[] mFilter;
    private final boolean mNormalizeUnicode;
    private final boolean mPrettyPrinting;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        LOGGER.debug("Settings: {}", aSettings.getAsMap());

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

        mFilter = aSettings.getAsArray("filter");
        mFilterOperator = aSettings.get("filterOperator", "any");
        mNormalizeUnicode = aSettings.getAsBoolean("normalize-unicode", true);
        mRulesPath = Helpers.getPath(aSettings.get("transformation-rules"), getClass());
    }

    public void process() {
        LOGGER.info("Starting transformation: {}", mRulesPath);

        final Metamorph metamorph = new Metamorph(mRulesPath);
        final StreamTee streamTee = new StreamTee();
        final Counter counter = new Counter();

        transformJson(streamTee);
        transformFormeta(streamTee);
        transformElasticsearch(streamTee);

        metamorph
            .setReceiver(counter)
            .setReceiver(streamTee);

        if (mFilter.length > 0) {
            final Filter filter = new Filter(LibraryMetadataFilter.buildMorphDef(mFilterOperator, mFilter));
            filter.setReceiver(metamorph);

            mInputQueue.process(filter, mNormalizeUnicode);
        }
        else {
            mInputQueue.process(metamorph, mNormalizeUnicode);
        }

        LOGGER.info("Finished transformation ({})", counter);
    }

    public int getInputQueueSize() {
        return mInputQueue.size();
    }

    private void transformFormeta(final StreamTee aTee) {
        if (mFormetaPath == null) {
            return;
        }

        LOGGER.info("Writing Formeta file: {}", mFormetaPath);

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

        LOGGER.info("Writing JSON file: {}", mJsonPath);

        final JsonEncoder jsonEncoder = new JsonEncoder();
        jsonEncoder.setPrettyPrinting(mPrettyPrinting);

        aTee.addReceiver(jsonEncoder);
        jsonEncoder.setReceiver(new ObjectWriter<>(mJsonPath));
    }

    private void transformElasticsearch(final StreamTee aTee) {
        if (mElasticsearchSettings == null) {
            return;
        }

        LOGGER.info("Indexing into Elasticsearch: {}", mElasticsearchSettings.getAsMap());

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
