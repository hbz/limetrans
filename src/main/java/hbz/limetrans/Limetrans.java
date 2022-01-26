package hbz.limetrans;

import hbz.limetrans.filter.LimetransFilter;
import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.formeta.FormetaEncoder;
import org.metafacture.formeta.formatter.FormatterStyle;
import org.metafacture.framework.StreamPipe;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.io.FileOpener;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.mangling.RecordIdChanger;
import org.metafacture.metafix.Metafix;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.Metamorph;
import org.metafacture.metamorph.api.Maps;
import org.metafacture.plumbing.StreamTee;
import org.metafacture.statistics.Counter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Limetrans { // checkstyle-disable-line ClassDataAbstractionCoupling

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Map<String, String> INSTITUTION_CODE_TO_ISIL = new HashMap<>();

    private enum Isil {

        // checkstyle-disable-begin MethodParamPad
        DE_605  ("6441", "49HBZ_NETWORK", "hbz NRW"),
        DE_361  ("6442", "49HBZ_BIE",     "UB Bielefeld"),
        DE_61   ("6443", "49HBZ_DUE",     "ULB Düsseldorf"),
        DE_A96  ("6444", "49HBZ_FHA",     "FHB Aachen"),
        DE_290  ("6445", "49HBZ_UBD",     "UB Dortmund"),
        DE_465  ("6446", "49HBZ_UDE",     "UB Duisburg-Essen"),                  // DE-464 = Duisburg, DE-465 = Essen
        DE_468  ("6447", "49HBZ_WUP",     "UB Wuppertal"),
        DE_82   ("6448", "49HBZ_UBA",     "RWTH Aachen"),
        DE_6    ("6449", "49HBZ_ULM",     "UB Münster"),
        DE_Bi10 ("6450", "49HBZ_HBI",     "HS Bielefeld"),
        DE_Dm13 ("6451", "49HBZ_FDO",     "FH Dortmund"),
        DE_1044 ("6452", "49HBZ_BRS",     "HS Bonn-Rhein-Sieg"),
        DE_1393 ("6453", "49HBZ_RUW",     "HS Ruhr West"),                       // DE-1393 = Mülheim, DE-1393-BOT = Bottrop
        DE_Bm40 ("6454", "49HBZ_HBO",     "HS Bochum"),
        DE_Due62("6455", "49HBZ_HSD",     "HS Düsseldorf"),
        DE_1010 ("6456", "49HBZ_WHS",     "Westfälische HS Gelsenkirchen"),
        DE_1156 ("6459", "49HBZ_FUK",     "Folkwang Universität der Künste"),    // DE-1156 = Essen, DE-1156a = Bochum, DE-1156b = Duisburg, DE-1156c = Zollverein
        DE_Hag4 ("6461", "49HBZ_FSW",     "FH Hagen / Südwestfalen"),
        DE_467  ("6462", "49HBZ_SIE",     "UB Siegen"),
        DE_466  ("6463", "49HBZ_PAD",     "UB Paderborn"),
        DE_708  ("6464", "49HBZ_FUH",     "Fernuni Hagen"),
        DE_836  ("6485", "49HBZ_FHM",     "FH Münster / Kunstakademie Münster"), // DE-836 = FH, DE-Mue301 = Kunstakademie
        DE_386  ("7476", "49HBZ_RTU",     "UB Kaiserslautern-Landau");           // DE-386 = Kaiserslautern, DE-Lan1 = Landau
        // checkstyle-disable-end

        private static final String ISIL_SEPARATOR = "-";
        private static final String NAME_SEPARATOR = "_";

        private final String mInstitutionCode;
        private final String mIsil;
        private final String mMemberCode;
        private final String mMemberName;

        Isil(final String aInstitutionCode, final String aMemberCode, final String aMemberName) {
            mInstitutionCode = aInstitutionCode;
            mMemberCode = aMemberCode;
            mMemberName = aMemberName;

            mIsil = name().replace(NAME_SEPARATOR, ISIL_SEPARATOR);
            INSTITUTION_CODE_TO_ISIL.put(aInstitutionCode, mIsil);
        }

        private static Isil get(final String aIsil) {
            return valueOf(aIsil.replace(ISIL_SEPARATOR, NAME_SEPARATOR));
        }

        private String getInstitutionCode() {
            return mInstitutionCode;
        }

        private String getIsil() {
            return mIsil;
        }

        private String getMemberCode() {
            return mMemberCode;
        }

        private String getMemberName() {
            return mMemberName;
        }

    }

    private static final boolean METAFIX_IS_DEFAULT = false;

    public enum Type {

        METAFIX(".fix", METAFIX_IS_DEFAULT),
        METAMORPH(".xml", !METAFIX_IS_DEFAULT);

        private static final String PREFIX = "META";

        private static final Type DEFAULT = METAFIX_IS_DEFAULT ? METAFIX : METAMORPH;

        private final String mName;
        private final String mExtension;
        private final boolean mRequired;

        Type(final String aExtension, final boolean aRequired) {
            mName = name().substring(PREFIX.length());
            mExtension = aExtension;
            mRequired = aRequired;
        }

        public String getExtension() {
            return mExtension;
        }

        public boolean getRequired() {
            return mRequired;
        }

        @Override
        public String toString() {
            return mName;
        }

    }

    private final LimetransFilter mFilter;
    private final List<FileQueue> mInputQueues = new ArrayList<>();
    private final Map<String, Map<String, String>> mMaps = new HashMap<>();
    private final Map<String, String> mVars = new HashMap<>();
    private final Settings mElasticsearchSettings;
    private final String mFormetaPath;
    private final String mJsonPath;
    private final String mRulesPath;
    private final Type mType;
    private final boolean mPrettyPrinting;

    public Limetrans(final Settings aSettings) throws IOException {
        this(aSettings, Helpers.getEnumProperty("type", aSettings.get("type"),
                    Type.DEFAULT, LOGGER::info, k -> Type.PREFIX + k.toUpperCase()));
    }

    public Limetrans(final Settings aSettings, final Type aType) throws IOException {
        LOGGER.debug("Settings: {}", aSettings);

        mType = aType;

        initializeInput(aSettings);
        initializeVars(aSettings);

        final String filterKey = aSettings.get("filterKey");
        final Settings outputSettings = aSettings.getAsSettings("output");
        mPrettyPrinting = outputSettings.getAsBoolean("pretty-printing", false);

        mElasticsearchSettings = outputSettings.containsSetting("elasticsearch") ?
            outputSettings.getAsSettings("elasticsearch") : null;
        mFormetaPath = pathForType(outputSettings.get("formeta"));
        mJsonPath = pathForType(outputSettings.get("json"));

        if (mFormetaPath == null && mJsonPath == null && mElasticsearchSettings == null) {
            throw new IllegalArgumentException("Could not process limetrans: no output specified.");
        }

        final String defaultRulesPath;

        if (aSettings.containsSetting("alma")) {
            mFilter = LimetransFilter.all(filterKey);

            final String rulesSuffix = initializeAlma(aSettings);

            defaultRulesPath = Helpers.CLASSPATH_PREFIX + "/transformation/alma" + rulesSuffix + "%s";
        }
        else {
            mFilter = new LimetransFilter(
                    aSettings.get("filterOperator", "any"), filterKey, aSettings.getAsArray("filter"));

            defaultRulesPath = null;
        }

        mRulesPath = Helpers.getPath(getClass(), pathForType(aSettings.get("transformation-rules", defaultRulesPath)));
    }

    private void initializeInput(final Settings aSettings) {
        aSettings.getAsSettings("input").forEach((s, k) -> {
            if (k.startsWith("queue")) {
                try {
                    final FileQueue inputQueue = new FileQueue(s.getAsSettings(k));

                    if (inputQueue.isEmpty()) {
                        LOGGER.warn("Empty input queue: {}", k);
                    }
                    else {
                        mInputQueues.add(inputQueue);
                    }
                }
                catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            else {
                LOGGER.warn("Unsupported input type: {}", k);
            }
        });

        if (mInputQueues.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: no input specified.");
        }
    }

    private void initializeVars(final Settings aSettings) {
        if (aSettings.containsSetting("isil")) {
            final String isil = aSettings.get("isil");
            mVars.put("isil", isil);

            final int index = isil.indexOf('-');
            if (index > 0) {
                mVars.put("sigel", isil.substring(index + 1));
            }
        }
    }

    private String initializeAlma(final Settings aSettings) {
        // Export ("PubHub") BGZF contains a large XML file; increase limits for XML parser.
        // »The accumulated size of entities is "50,000,001" that exceeded the "50,000,000" limit set by "FEATURE_SECURE_PROCESSING".«
        // https://docs.oracle.com/en/java/javase/13/security/java-api-xml-processing-jaxp-security-guide.html#GUID-82F8C206-F2DF-4204-9544-F96155B1D258__TABLE_RQ1_3PY_HHB
        System.setProperty("jdk.xml.totalEntitySizeLimit", "0");

        final Settings almaSettings = aSettings.getAsSettings("alma");

        // Ex Libris (Deutschland) GmbH
        mVars.putIfAbsent("isil", "DE-632");

        final Isil isil = Isil.get(mVars.get("isil"));
        final Isil catalogid = Isil.get(aSettings.get("catalogid", "DE-605"));
        final String almaDeletion = almaSettings.get("deletions", "DEL??.a=Y");

        // Organization originating the system control number
        mVars.put("catalogid", catalogid.getIsil());

        final String memberCode = isil.getMemberCode();
        final String networkCode = catalogid.getMemberCode();
        final String institutionCode = isil.getInstitutionCode();

        mVars.put("member", memberCode);
        mVars.put("network", networkCode);
        mVars.put("institution-code", institutionCode);
        mVars.put("id-suffix", almaSettings.get("id-suffix", ""));

        mMaps.put("institution-code-to-isil", INSTITUTION_CODE_TO_ISIL);

        final String rulesSuffix;

        final UnaryOperator<String> sourceSystemFilter = i -> "035  .a=~^\\(" + i + "\\)";

        // POR$$A=memberCode
        final LimetransFilter availableForFilter = LimetransFilter.all()
            .add("POR  .A=" + memberCode);

        // MBD$$M=memberCode OR POR$$M=memberCode
        final LimetransFilter memberFilter = LimetransFilter.any()
            .add("MBD  .M|POR  .M=" + memberCode);

        // MBD$$M=49HBZ_NETWORK AND ITM$$M=memberCode
        final LimetransFilter itemFilter = LimetransFilter.all()
            .add("MBD  .M=" + networkCode, "ITM  .M=" + memberCode);

        // DEL??.a=Y OR leader@05=d
        final LimetransFilter deletionFilter = LimetransFilter.any()
            .add(almaDeletion, "leader=~^.{5}d");

        final LimetransFilter noDeletionFilter = LimetransFilter.none()
            .add(deletionFilter);

        final Settings regexp = almaSettings.getAsSettings("regexp");
        Stream.of("description").forEach(k -> mVars.put("regexp." + k, regexp.get(k, ".*")));

        if (almaSettings.getAsBoolean("supplements", false)) {
            rulesSuffix = "-supplements";

            mFilter
                .add(LimetransFilter.any()
                        .add(availableForFilter)
                        .add(LimetransFilter.all()
                            .add(memberFilter)
                            .add(itemFilter)))
                .add(noDeletionFilter)
                .add(sourceSystemFilter.apply(catalogid.getIsil()));
        }
        else {
            final String deletionLiteral = almaSettings.get("deletion-literal",
                    mElasticsearchSettings != null ?  mElasticsearchSettings.get("deletionLiteral") : null);

            if (deletionLiteral != null) {
                final String[] deletion = almaDeletion.split("=");

                mVars.put("deletion-literal", deletionLiteral);
                mVars.put("deletion-source", deletion[0]);
                mVars.put("deletion-value", deletion[1]);

                memberFilter
                    .add(deletionFilter);
            }
            else {
                mVars.put("deletion-literal", "-");
                mVars.put("deletion-source", "-");
                mVars.put("deletion-value", "-");

                mFilter
                    .add(noDeletionFilter);
            }

            rulesSuffix = "";

            mFilter
                .add(memberFilter
                        .add(availableForFilter
                            .add(sourceSystemFilter.apply("EXLCZ"))))
                .add(LimetransFilter.any()
                        .add(sourceSystemFilter.apply("DE-600"))
                        .add(LimetransFilter.none()
                            .add(itemFilter)));
        }

        return rulesSuffix;
    }

    public void process() {
        process(null);
    }

    public void process(final StreamReceiver aReceiver) {
        final StreamPipe<StreamReceiver> pipe = getStreamPipe(mRulesPath, mVars,
                t -> LOGGER.info("Starting {} transformation: {}", t, mRulesPath));

        final StreamTee streamTee = new StreamTee();
        final Counter counter = new Counter();

        transformJson(streamTee);
        transformFormeta(streamTee);
        transformElasticsearch(streamTee);

        if (pipe instanceof final Maps maps) {
            mMaps.forEach(maps::putMap);
        }

        pipe
            .setReceiver(counter)
            .setReceiver(streamTee);

        if (aReceiver != null) {
            pipe.setReceiver(aReceiver);
        }

        final Filter filter = mFilter.isEmpty() ? null : mFilter.toFilter();
        mInputQueues.stream().map(i -> i.process(pipe, filter))
            .collect(Collectors.toList()).forEach(FileOpener::closeStream);

        LOGGER.info("Finished transformation ({})", counter);
    }

    public static StreamPipe<StreamReceiver> getStreamPipe(final String aRulesPath, final Map<String, String> aVars, final Consumer<Type> aConsumer) {
        if (aRulesPath == null) {
            return null;
        }

        final Map<String, String> vars = aVars != null ? aVars : Collections.emptyMap();
        final StreamPipe<StreamReceiver> pipe;
        final Type type;

        try {
            if (aRulesPath.endsWith(Type.METAFIX.getExtension()) || METAFIX_IS_DEFAULT && !aRulesPath.endsWith(Type.METAMORPH.getExtension())) {
                pipe = new Metafix(aRulesPath, vars);
                type = Type.METAFIX;
            }
            else {
                pipe = new Metamorph(aRulesPath, vars);
                type = Type.METAMORPH;
            }
        }
        catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        if (aConsumer != null) {
            aConsumer.accept(type);
        }

        return pipe;
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

    /*package-private*/ String pathForType(final String aPath) {
        return aPath != null ? mType != null ? String.format(aPath, mType.getExtension()) : aPath : null;
    }

    /*package-private*/ int getInputQueueSize() {
        return mInputQueues.stream().mapToInt(FileQueue::size).sum();
    }

}
