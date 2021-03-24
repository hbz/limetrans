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

    private static final Map<String, String> ISIL_TO_MEMBER_ID = new HashMap<String, String>() {
        {
            put("DE-605", "49HBZ_NETWORK"); // hbz - Hochschulbibliothekszentrum des Landes Nordrhein-Westfalen
            put("DE-361", "49HBZ_BIE");     // Universitätsbibliothek Bielefeld
            put("DE-61",  "49HBZ_DUE");     // Universitäts- und Landesbibliothek Düsseldorf
            put("DE-A96", "49HBZ_FHA");     // Hochschulbibliothek der Fachhochschule Aachen
            put("DE-290", "49HBZ_UBD");     // Universitätsbibliothek Dortmund
            put("DE-465", "49HBZ_UDE");     // Universitätsbibliothek Duisburg-Essen (DE-464 = Campus Duisburg, DE-465 = Campus Essen)
            put("DE-468", "49HBZ_WUP");     // Universitätsbibliothek Wuppertal
        }
    };

    private static final Map<String, String> ISIL_TO_INSTITUTION_CODE = new HashMap<String, String>() {
        {
            put("DE-605", "6441");          // hbz - Hochschulbibliothekszentrum des Landes Nordrhein-Westfalen
            put("DE-361", "6442");          // Universitätsbibliothek Bielefeld
            put("DE-61",  "6443");          // Universitäts- und Landesbibliothek Düsseldorf
            put("DE-A96", "6444");          // Hochschulbibliothek der Fachhochschule Aachen
            put("DE-290", "6445");          // Universitätsbibliothek Dortmund
            put("DE-465", "6446");          // Universitätsbibliothek Duisburg-Essen (DE-464 = Campus Duisburg, DE-465 = Campus Essen)
            put("DE-468", "6447");          // Universitätsbibliothek Wuppertal
        }
    };

    private static final Map<String, String> INSTITUTION_CODE_TO_ISIL = new HashMap<String, String>() {
        {
            ISIL_TO_INSTITUTION_CODE.forEach((k, v) -> put(v, k));
        }
    };

    private final FileQueue mInputQueue;
    private final LibraryMetadataFilter mFilter;
    private final Map<String, Map<String, String>> mMaps = new HashMap<>();
    private final Map<String, String> mVars = new HashMap<>();
    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final boolean mPrettyPrinting;

    public LibraryMetadataTransformation(final Settings aSettings) throws IOException { // checkstyle-disable-line JavaNCSS|NPathComplexity
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
            final String isil = aSettings.get("isil");
            mVars.put("isil", isil);

            final int index = isil.indexOf('-');
            if (index > 0) {
                mVars.put("sigel", isil.substring(index + 1));
            }
        }

        final String defaultRulesPath;

        if (aSettings.containsSetting("alma")) {
            final Settings almaSettings = aSettings.getAsSettings("alma");

            // Ex Libris (Deutschland) GmbH
            mVars.putIfAbsent("isil", "DE-632");

            final String isil = mVars.get("isil");
            final String catalogid = aSettings.get("catalogid", "DE-605");

            // Organization originating the system control number
            mVars.put("catalogid", catalogid);

            final String memberID = ISIL_TO_MEMBER_ID.get(isil);
            final String networkID = ISIL_TO_MEMBER_ID.get(catalogid);
            final String institutionCode = ISIL_TO_INSTITUTION_CODE.get(isil);

            if (memberID == null || institutionCode == null) {
                throw new RuntimeException("Unknown ISIL: " + isil);
            }

            if (networkID == null) {
                throw new RuntimeException("Unknown catalog ID: " + catalogid);
            }

            mVars.put("member", memberID);
            mVars.put("network", networkID);
            mVars.put("institution-code", institutionCode);
            mVars.put("id-suffix", almaSettings.get("id-suffix"));

            //mMaps.put("isil-to-member-id", ISIL_TO_MEMBER_ID);
            //mMaps.put("isil-to-institution-code", ISIL_TO_INSTITUTION_CODE);
            mMaps.put("institution-code-to-isil", INSTITUTION_CODE_TO_ISIL);

            final String rulesSuffix;

            // MBD$$M=memberID OR POR$$M=memberID OR POR$$A=memberID
            final LibraryMetadataFilter memberFilter = LibraryMetadataFilter.any()
                .add("MBD  .M|POR  .[MA]=" + memberID);

            // MBD$$M=49HBZ_NETWORK AND ITM$$M=memberID
            final LibraryMetadataFilter itemFilter = LibraryMetadataFilter.all()
                .add("MBD  .M=" + networkID, "ITM  .M=" + memberID);

            // leader@05=d
            final LibraryMetadataFilter suppressionFilter = LibraryMetadataFilter.none()
                .add("leader=~^.....d");

            mFilter = LibraryMetadataFilter.all(filterKey).add(memberFilter).add(suppressionFilter);

            final Settings regexp = almaSettings.getAsSettings("regexp");
            Stream.of("description").forEach(k -> mVars.put("regexp." + k, regexp.get(k, ".*")));

            if (almaSettings.getAsBoolean("supplements", false)) {
                rulesSuffix = "-supplements";
                mFilter.add(itemFilter).add("035  .a=~^\\(" + catalogid + "\\)");
            }
            else {
                rulesSuffix = "";
                mFilter.add(LibraryMetadataFilter.none().add(itemFilter).add("035  .a=~^\\(DE-600\\)"));
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

        mMaps.forEach(metamorph::putMap);

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
