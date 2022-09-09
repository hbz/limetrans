package hbz.limetrans.function;

import hbz.limetrans.util.Helpers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.Value;
import org.metafacture.metafix.api.FixFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class VerifyLinks implements FixFunction {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String ISIL_PATH_FORMAT = "%s.%sid.bgzf";

    private static final String VERIFYING_PREFIX = "verifying";
    private static final String VERIFIED_PREFIX = "verified";

    private static final Map<String, List<String>> LINK_MAP = new HashMap<>();
    private static final Map<String, List<String>> SUPER_MAP = new HashMap<>();

    private static final String SUPER_SOURCE = "xbib[].uid";

    private static final Map<String, LongAdder> LINK_COUNTER = new HashMap<>();
    private static final Map<String, LongAdder> SUPER_COUNTER = new HashMap<>();

    private static Predicate<String> linkPredicate; // checkstyle-disable-line StaticVariableName
    private static Predicate<String> superPredicate; // checkstyle-disable-line StaticVariableName

    static {
        /*
        LINK_MAP.put("identifierForDescription", List.of(
                    "DescriptionOfContinuingEditionsOrVolumes[]",
                    "DescriptionOfFormerEditionsOrVolumes[]",
                    "DescriptionOfPeriodicSupplements[]", // TODO
                    "DescriptionOfRelatedEditions[]",
                    "DescriptionOfRelatedWorks[]", // TODO
                    "DescriptionOfTitleForms[]" // TODO
        ));
        */

        LINK_MAP.put("identifierForLinkingEntry", List.of(
                    "AdditionalPhysicalFormEntry[]",
                    "ConstituentUnitEntry[]",
                    "DataSourceEntry[]",
                    "HostItemEntry[]",
                    "IssuedWithEntry[]",
                    "MainSeriesEntry[]",
                    "NonspecificRelationshipEntry[]",
                    "OriginalLanguageEntry[]",
                    "OtherEditionEntry[]",
                    "PrecedingEntry[]",
                    "SubSeriesEntry[]",
                    "SucceedingEntry[]",
                    "SupplementParentEntry[]",
                    "SupplementSpecialIssueEntry[]",
                    "TranslationEntry[]"
        ));

        /*
        LINK_MAP.put("relationIdentifier", List.of(
                    "SecondaryEditionReproduction[]" // TODO
        ));
        */

        /*
        LINK_MAP.put("identifierOfTheSource", List.of(
                    "SourceIdentifier[]" // TODO
        ));
        */

        SUPER_MAP.put("superIdentifier", List.of(
                    "RecordIdentifier"
        ));
    }

    public VerifyLinks() {
    }

    public static void main(final String[] aArgs) {
        final int argc = aArgs.length;
        if (argc < 1) {
            throw new IllegalArgumentException("Usage: " + VerifyLinks.class + " <path> [<id>...]");
        }

        final Set<String> idSet = loadIdSet(aArgs[0], true);
        if (idSet != null) {
            for (int i = 1; i < argc; ++i) {
                System.out.println(aArgs[i] + "=" + idSet.contains(aArgs[i]));
            }
        }
    }

    public static void setup(final Map<String, String> aVars) {
        final String isilPath = Objects.requireNonNull(aVars.get("isil-path"));

        final Set<String> idSet = loadIdSet(ISIL_PATH_FORMAT.formatted(isilPath, ""), true);
        final Set<String> skipIdSet = loadIdSet(ISIL_PATH_FORMAT.formatted(isilPath, "skip"), false);
        final Set<String> superIdSet = loadIdSet(ISIL_PATH_FORMAT.formatted(isilPath, "super"), true);

        linkPredicate = idSet != null ? id -> idSet.contains(id) && !skipIdSet.contains(id) : null;
        superPredicate = superIdSet != null ? superIdSet::contains : null;

        LINK_COUNTER.clear();
        SUPER_COUNTER.clear();
    }

    public static void reset() {
        if (linkPredicate != null) {
            linkPredicate = null;
            LOGGER.info("Verified links: {}", LINK_COUNTER);
        }

        if (superPredicate != null) {
            superPredicate = null;
            LOGGER.info("Verified super identifiers: {}", SUPER_COUNTER);
        }
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        if (linkPredicate != null) {
            verifyLinks(aRecord, LINK_MAP, null, linkPredicate, LINK_COUNTER);
        }

        if (superPredicate != null) {
            verifyLinks(aRecord, SUPER_MAP, SUPER_SOURCE, superPredicate, SUPER_COUNTER);
        }
    }

    private void verifyLinks(final Record aRecord, final Map<String, List<String>> aMap, final String aSource, final Predicate<String> aPredicate, final Map<String, LongAdder> aCounter) {
        aMap.forEach((field, list) -> {
            final String suffix = field.substring(0, 1).toUpperCase() + field.substring(1);

            list.forEach(path -> eachValue(aRecord, path, value -> {
                final Value.Hash hash = value.asHash();

                eachValue(aSource != null ? aRecord : hash, aSource != null ? aSource : field, idValue -> {
                    if (aPredicate.test(idValue.asString())) {
                        hash.add(VERIFIED_PREFIX + suffix, idValue);
                        aCounter.computeIfAbsent(path, k -> new LongAdder()).increment();
                    }

                    hash.add(VERIFYING_PREFIX + suffix, idValue);
                });
            }));
        });
    }

    private void eachValue(final Value.Hash aHash, final String aPath, final Consumer<Value> aConsumer) {
        Value.asList(aHash.get(aPath), a -> a.forEach(aConsumer));
    }

    private static Set<String> loadIdSet(final String aPath, final boolean aRequired) {
        final Set<String> set = new HashSet<>();
        return Helpers.loadFile(aPath, aRequired, "ID set", set::add, set::size, LOGGER) ? set : null;
    }

}
