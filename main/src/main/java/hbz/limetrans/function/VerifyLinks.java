package hbz.limetrans.function;

import hbz.limetrans.util.Helpers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.metafix.FixCommand;
import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.Value;
import org.metafacture.metafix.api.FixFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;

@FixCommand("verify_links")
public class VerifyLinks implements FixFunction {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String ISIL_PATH_FORMAT = "%s.%s.bgzf";

    private static final String VERIFYING_PREFIX = "verifying";
    private static final String VERIFIED_PREFIX = "verified";

    private static final Map<String, List<String>> SUPER_MAP = new HashMap<>();
    private static final Map<String, LongAdder> SUPER_COUNTER = new HashMap<>();

    private static final String SUPER_SOURCE = "xbib[].uid";

    private static Predicate<String> superPredicate; // checkstyle-disable-line StaticVariableName

    static {
        /*
        Link.ID.getMap().put("identifierForDescription", List.of(
                    "DescriptionOfContinuingEditionsOrVolumes[]",
                    "DescriptionOfFormerEditionsOrVolumes[]",
                    "DescriptionOfPeriodicSupplements[]", // TODO
                    "DescriptionOfRelatedEditions[]",
                    "DescriptionOfRelatedWorks[]", // TODO
                    "DescriptionOfTitleForms[]" // TODO
        ));
        */

        Link.ID.getMap().put("identifierForLinkingEntry[]", List.of(
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
        Link.ID.getMap().put("identifierOfTheSource", List.of(
                    "SourceIdentifier[]" // TODO
        ));
        */

        /*
        Link.ID.getMap().put("relationIdentifier", List.of(
                    "SecondaryEditionReproduction[]" // TODO
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
            throw new IllegalArgumentException("Usage: " + VerifyLinks.class + " <path> [<value>...]");
        }

        final Set<String> set = loadSet(aArgs[0], "link", true);
        if (set != null) {
            for (int i = 1; i < argc; ++i) {
                System.out.println(aArgs[i] + "=" + set.contains(aArgs[i]));
            }
        }
    }

    public static void setup(final Map<String, String> aVars) {
        final String isilPath = Objects.requireNonNull(aVars.get("isil-path"));

        Link.forEach(l -> {
            final String name = l.name();
            final String key = name.toLowerCase();

            final Set<String> set = loadSet(ISIL_PATH_FORMAT.formatted(isilPath, key), name, l.getRequired());
            final Set<String> skipSet = loadSet(ISIL_PATH_FORMAT.formatted(isilPath, "skip" + key), name, false);

            l.setPredicate(set != null ? v -> set.contains(v) && !skipSet.contains(v) : null);

            l.getCounter().clear();
        });

        final Set<String> superIdSet = loadSet(ISIL_PATH_FORMAT.formatted(isilPath, "superid"), "super ID", true);
        superPredicate = superIdSet != null ? superIdSet::contains : null;
        SUPER_COUNTER.clear();
    }

    public static void reset() {
        Link.forEach(l -> {
            if (l.getPredicate() != null) {
                l.setPredicate(null);
                LOGGER.info("Verified {}s: {}", l.name(), l.getCounter());
            }
        });

        if (superPredicate != null) {
            superPredicate = null;
            LOGGER.info("Verified super IDs: {}", SUPER_COUNTER);
        }
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        Link.forEach(l -> {
            if (l.getPredicate() != null) {
                verifyLinks(aRecord, l.getMap(), null, l.getPredicate(), l.getCounter());
            }
        });

        if (superPredicate != null) {
            verifyLinks(aRecord, SUPER_MAP, SUPER_SOURCE, superPredicate, SUPER_COUNTER);
        }
    }

    private void verifyLinks(final Record aRecord, final Map<String, List<String>> aMap, final String aSource, final Predicate<String> aPredicate, final Map<String, LongAdder> aCounter) {
        final VerifyingConsumer consumer = (path, verifying, verified, value, string) -> {
            verifying.add(value);

            if (aPredicate.test(string)) {
                verified.add(value);
                aCounter.computeIfAbsent(path, k -> new LongAdder()).increment();
            }
        };

        aMap.forEach((field, list) -> {
            final String suffix = field.substring(0, 1).toUpperCase() + field.substring(1) + (field.endsWith(Metafix.ARRAY_MARKER) ? "" : Metafix.ARRAY_MARKER);

            list.forEach(path -> eachValue(aRecord, path, value -> {
                final Value.Hash hash = value.asHash();

                final List<Value> verifying = new ArrayList<>();
                final List<Value> verified = new ArrayList<>();

                eachValue(aSource != null ? aRecord : hash, aSource != null ? aSource : field, fieldValue -> {
                    fieldValue.matchType()
                        .ifArray(a -> a.forEach(v -> consumer.accept(path, verifying, verified, v, v.asString())))
                        .ifString(s -> consumer.accept(path, verifying, verified, new Value(s), s));
                });

                if (!verifying.isEmpty()) {
                    hash.add(VERIFYING_PREFIX + suffix, Value.newArray(a -> verifying.forEach(a::add)));
                }

                if (!verified.isEmpty()) {
                    hash.add(VERIFIED_PREFIX + suffix, Value.newArray(a -> verified.forEach(a::add)));
                }
            }));
        });
    }

    private void eachValue(final Value.Hash aHash, final String aPath, final Consumer<Value> aConsumer) {
        Value.asList(aHash.get(aPath), a -> a.forEach(aConsumer));
    }

    private static Set<String> loadSet(final String aPath, final String aType, final boolean aRequired) {
        final Set<String> set = new HashSet<>();
        return Helpers.loadFile(aPath, aRequired, aType + " set", set::add, set::size, LOGGER) ? set : null;
    }

    @FunctionalInterface
    private interface VerifyingConsumer {
        void accept(String aPath, List<Value> aVerifying, List<Value> aVerified, Value aValue, String aString);
    }

    private enum Link {

        ID(true);

        private final Map<String, List<String>> mMap = new HashMap<>();
        private final Map<String, LongAdder> mCounter = new HashMap<>();
        private final boolean mRequired;

        private Predicate<String> mPredicate;

        Link(final boolean aRequired) {
            mRequired = aRequired;
        }

        private static void forEach(final Consumer<Link> aConsumer) {
            Arrays.stream(values()).forEach(aConsumer);
        }

        private Map<String, LongAdder> getCounter() {
            return mCounter;
        }

        private Map<String, List<String>> getMap() {
            return mMap;
        }

        private Predicate<String> getPredicate() {
            return mPredicate;
        }

        private void setPredicate(final Predicate<String> aPredicate) {
            mPredicate = aPredicate;
        }

        private boolean getRequired() {
            return mRequired;
        }

    }

}
