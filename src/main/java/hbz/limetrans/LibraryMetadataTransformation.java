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
    private final LibraryMetadataFilter mFilter;
    private final Map<String, String> mVars = new HashMap<>();
    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final boolean mPrettyPrinting;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException {
        LOGGER.debug("Settings: {}", aSettings);

        mInputQueue = new FileQueue(aSettings.getAsSettings("input").getAsSettings("queue"));

        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: no input specified.");
        }

        final String filterKey = aSettings.get("filterKey");
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

        final String defaultRulesPath;

        if (aSettings.containsSetting("alma")) {
            final String memberID = aSettings.get("alma");
            final String networkID = aSettings.get("alma-network", "49HBZ_NETWORK");

            mVars.put("member", memberID);
            mVars.put("network", networkID);
            mVars.put("id-suffix", aSettings.get("id-suffix"));

            // Ex Libris (Deutschland) GmbH
            mVars.putIfAbsent("isil", "DE-632");

            // Organization originating the system control number
            final String catalogid = aSettings.get("catalogid", "DE-605");
            mVars.put("catalogid", catalogid);

            final String rulesSuffix;

            // MBD$$M=memberID OR POR$$M=memberID OR POR$$A=memberID
            final LibraryMetadataFilter memberFilter = LibraryMetadataFilter.any()
                .add("MBD  .M|POR  .[MA]=" + memberID);

            // MBD$$M=49HBZ_NETWORK AND EXISTS(ITM)
            final LibraryMetadataFilter itemFilter = LibraryMetadataFilter.all()
                .add("MBD  .M=" + networkID, "@ITM  ");

            // leader@05=d
            final LibraryMetadataFilter suppressionFilter = LibraryMetadataFilter.none()
                .add("leader=~^.....d");

            mFilter = LibraryMetadataFilter.all(filterKey).add(memberFilter).add(suppressionFilter);

            if (aSettings.getAsBoolean("alma-portfolios", false)) {
                // POR$$M=49HBZ_NETWORK AND NOT EXISTS(POR$$A)
                memberFilter.add(LibraryMetadataFilter.all().add("POR  .M=" + networkID, "!POR  .A"));
                mVars.put("portfolio", networkID);
            }
            else {
                mVars.put("portfolio", "-");
            }

            if (aSettings.containsSetting("alma-supplements")) {
                final Settings supplements = aSettings.getAsSettings("alma-supplements");
                Stream.of("description").forEach(k -> mVars.put("regexp." + k, supplements.get(k, ".*")));

                rulesSuffix = "-supplements";
                mFilter.add(itemFilter);
            }
            else {
                rulesSuffix = "";
                mFilter.add(LibraryMetadataFilter.none().add(itemFilter));
            }

            defaultRulesPath = "classpath:/transformation/alma" + rulesSuffix + ".xml";
        }
        else {
            mFilter = new LibraryMetadataFilter(
                    aSettings.get("filterOperator", "any"), filterKey, aSettings.getAsArray("filter"));

            defaultRulesPath = null;
        }

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

        mInputQueue.process(metamorph, mFilter.isEmpty() ? null : mFilter.toFilter());

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
