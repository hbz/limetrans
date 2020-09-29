package hbz.limetrans;

import hbz.limetrans.filter.LibraryMetadataFilter;
import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.formeta.FormetaEncoder;
import org.metafacture.formeta.formatter.FormatterStyle;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.mangling.RecordIdChanger;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.Metamorph;
import org.metafacture.plumbing.StreamTee;
import org.metafacture.statistics.Counter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class LibraryMetadataTransformation { // checkstyle-disable-line ClassDataAbstractionCoupling

    private static final Logger LOGGER = LogManager.getLogger();

    private final FileQueue mInputQueue;
    private final Map<String, String> mVars = new HashMap<>();
    private final Settings mElasticsearchSettings;
    private final String mFilterKey;
    private final String mFilterOperator;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final String[] mFilter;
    private final boolean mPrettyPrinting;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        LOGGER.debug("Settings: {}", aSettings);

        mInputQueue = new FileQueue(aSettings.getAsSettings("input").getAsSettings("queue"));

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

        if (aSettings.containsSetting("isil")) {
            mVars.put("isil", aSettings.get("isil"));
        }

        final String defaultFilterOperator;
        final String defaultRulesPath;

        if (aSettings.containsSetting("alma")) {
            final String memberID = aSettings.get("alma");

            mVars.put("member", memberID);

            // Ex Libris (Deutschland) GmbH
            mVars.put("isil", "DE-632");

            final String filterPrefix;
            final String rulesSuffix;

            if (aSettings.containsSetting("alma-supplements")) {
                final Settings supplements = aSettings.getAsSettings("alma-supplements");
                Stream.of("description").forEach(k -> mVars.put("regexp." + k, supplements.get(k, ".*")));

                filterPrefix = "@";
                rulesSuffix = "-supplements";
            }
            else {
                filterPrefix = "!";
                rulesSuffix = "";
            }

            // 009 = HBZ-IDN Aleph NZ (-> "Extension Pack")
            mFilter = new String[]{"MBD  .M=" + memberID, filterPrefix + "009"};

            defaultFilterOperator = "all";
            defaultRulesPath = "classpath:/transformation/alma" + rulesSuffix + ".xml";
        }
        else {
            mFilter = aSettings.getAsArray("filter");

            defaultFilterOperator = "any";
            defaultRulesPath = null;
        }

        mFilterKey = aSettings.get("filterKey", LibraryMetadataFilter.DEFAULT_KEY);
        mFilterOperator = aSettings.get("filterOperator", defaultFilterOperator);
        mRulesPath = Helpers.getPath(getClass(), aSettings.get("transformation-rules", defaultRulesPath));
    }

    public void process() {
        process(null);
    }

    public void process(final StreamReceiver aReceiver) {
        LOGGER.info("Starting transformation: {}", mRulesPath);

        final Metamorph metamorph = new Metamorph(mRulesPath, mVars);
        final StreamTee streamTee = new StreamTee();
        final Counter counter = new Counter();

        transformJson(streamTee);
        transformFormeta(streamTee);
        transformElasticsearch(streamTee);

        metamorph
            .setReceiver(counter)
            .setReceiver(streamTee);

        if (aReceiver != null) {
            metamorph.setReceiver(aReceiver);
        }

        mInputQueue.process(metamorph, mFilter.length > 0 ?
                new Filter(LibraryMetadataFilter.buildMorphDef(mFilterKey, mFilterOperator, mFilter)) : null);

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

        LOGGER.info("Indexing into Elasticsearch: {}", mElasticsearchSettings);

        final RecordIdChanger recordIdChanger = new RecordIdChanger();
        final String idKey = ElasticsearchClient.getIndexSettings(mElasticsearchSettings).get("idKey");

        if (idKey != null) {
            recordIdChanger.setIdLiteral(idKey);
            recordIdChanger.setKeepIdLiteral(true);
        }

        final ElasticsearchIndexer elasticsearchIndexer =
            new ElasticsearchIndexer(mElasticsearchSettings);

        aTee.addReceiver(recordIdChanger);
        recordIdChanger.setReceiver(elasticsearchIndexer);
    }

}
