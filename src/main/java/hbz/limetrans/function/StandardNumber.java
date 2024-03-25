package hbz.limetrans.function;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.Value;
import org.metafacture.metafix.api.FixFunction;
import org.metafacture.metamorph.functions.ISBN;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class StandardNumber implements FixFunction {

    private static final String IDENTIFIER_FORMAT = "identifier%s";
    private static final String PREFERRED_FORMAT = "preferred%s";
    private static final String VARIANT_FORMAT = "variant%s[]";

    private static final String HYPHEN = "-";

    private static final char LOWER = '0';
    private static final char UPPER = 'X';

    private static final int BASE = 10;

    public StandardNumber() {
    }

    private static boolean isbnIsValid(final String aNumber) {
        return ISBN.isValid(aNumber);
    }

    private static String isbn10to13(final String aNumber) {
        return ISBN.isbn10to13(aNumber);
    }

    private static String isbn13to10(final String aNumber) {
        return ISBN.isbn13to10(aNumber);
    }

    public static String normalizeZDB(final String aValue) {
        return Type.ZDB.normalize(aValue, null);
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final Type type = Type.valueOf(aParams.get(0));
        final Value.Hash value = aRecord.get(aParams.get(1)).asHash();
        final String source = aOptions.get("source");

        final Value number = source != null ? aRecord.get(source) : value.getField(type.identifierField());
        if (number != null) {
            type.normalize(number.asString(), value);
        }
    }

    private abstract static class AbstractType {

        private final Matcher mMatcher;

        private AbstractType(final String aPattern) {
            mMatcher = Pattern.compile(aPattern).matcher("");
        }

        public String normalize(final String aValue, final Value.Hash aHash, final Type aType) {
            mMatcher.reset(aValue);
            return mMatcher.find() ? normalize(mMatcher, aHash, aType, aValue) : null;
        }

        protected abstract String normalize(Matcher aMatcher, Value.Hash aHash, Type aType, String aValue);

        protected boolean isValid(final String aNumber, final String aCheckDigit) {
            final int base = aNumber.length() + 1;

            final int checksum = IntStream.range(0, base - 1)
                .map(i -> (aNumber.charAt(i) - '0') * (base - i)).sum();

            return isValid(checksum, aCheckDigit.charAt(0));
        }

        protected boolean isValid(final int aChecksum, final char aChr) {
            return false;
        }

        protected void put(final Value.Hash aHash, final Type aType, final String aValue, final String aPreferred, final String... aVariants) {
            if (aHash == null) {
                return;
            }

            if (aPreferred != null) {
                aHash.put(aType.preferredField(), new Value(aPreferred));
            }

            if (aVariants.length > 0) {
                aHash.put(aType.variantField(), Value.newArray(a -> Arrays.stream(aVariants)
                            .filter(n -> n != null && !n.equals(aValue)).distinct().forEach(n -> a.add(new Value(n)))));
            }
        }

    }

    /*package-private*/ enum Type {

        ISBN(new AbstractType("\\b(?:\\d-?){9}(?:(?:\\d-?){3}\\d|[\\dxX])\\b") { // checkstyle-disable-line AnonInnerLength

            private static final int ISBN10_SIZE = 10;
            private static final int ISBN13_SIZE = 13;

            private static final String PREFIX = "978";
            private static final int PREFIX_LENGTH = PREFIX.length() + HYPHEN.length();

            private static final TreeMap<String, Range> RANGE_MAP = new TreeMap<>(); // checkstyle-disable-line IllegalType

            static {
                try (
                    InputStream inputStream = StandardNumber.class.getResourceAsStream("/standardnumber/RangeMessage.csv");
                    Reader reader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(reader)
                ) {
                    bufferedReader.lines().forEach(l -> new Range(l.split(",")));
                }
                catch (final IOException e) {
                    System.err.println("Failed to load ISBN range messages: " + e);
                }
            }

            @Override
            protected String normalize(final Matcher aMatcher, final Value.Hash aHash, final Type aType, final String aValue) {
                final String normalizedNumber = aMatcher.group().replace(HYPHEN, "");
                if (!isbnIsValid(normalizedNumber)) {
                    return null;
                }

                switch (normalizedNumber.length()) {
                    case ISBN10_SIZE: {
                        final String alternateNumber = isbn10to13(normalizedNumber);
                        final String hyphenatedNumber = hyphenate13(alternateNumber);

                        put(aHash, aType, aValue, alternateNumber, // ISBN-13 normalized
                                hyphenatedNumber,                  // ISBN-13 hyphenated
                                normalizedNumber,                  // ISBN-10 normalized
                                hyphenate10(normalizedNumber)      // ISBN-10 hyphenated
                        );

                        break;
                    }
                    case ISBN13_SIZE: {
                        final String alternateNumber = normalizedNumber.startsWith(PREFIX) ? isbn13to10(normalizedNumber) : null;
                        final String hyphenatedNumber = hyphenate13(normalizedNumber);

                        put(aHash, aType, aValue, normalizedNumber, // ISBN-13 normalized
                                hyphenatedNumber,                   // ISBN-13 hyphenated
                                alternateNumber,                    // ISBN-10 normalized
                                hyphenate10(alternateNumber)        // ISBN-10 hyphenated
                        );

                        break;
                    }
                    default:
                        // ignore
                        break;
                }

                return normalizedNumber;
            }

            private String hyphenate10(final String aNumber) {
                final String hyphenatedNumber = aNumber != null ? hyphenate13(PREFIX + aNumber) : null;
                return hyphenatedNumber != null ? hyphenatedNumber.substring(PREFIX_LENGTH) : null;
            }

            private String hyphenate13(final String aNumber) {
                final Map.Entry<String, Range> entry = RANGE_MAP.lowerEntry(aNumber);
                return entry != null ? entry.getValue().hyphenate(aNumber) : null;
            }

            private static class Range {

                private final String mEnd;
                private final int mBeginOffset;
                private final int mEndLength;
                private final int mGroupOffset;
                private final int mPrefixOffset;

                private Range(final String[] aParts) {
                    final String prefix = aParts[0];
                    final String group = prefix + aParts[1];
                    final String begin = group + aParts[2];
                    mEnd = group + aParts[3]; // checkstyle-disable-line MagicNumber

                    mPrefixOffset = prefix.length();
                    mGroupOffset = group.length();
                    mBeginOffset = begin.length();
                    mEndLength = mEnd.length();

                    RANGE_MAP.put(begin, this);
                }

                private String hyphenate(final String aNumber) {
                    if (mEnd.compareTo(aNumber.substring(0, mEndLength)) < 0) {
                        return null;
                    }

                    final StringBuilder sb = new StringBuilder(aNumber);

                    sb.insert(ISBN13_SIZE - 1, HYPHEN); // 4
                    sb.insert(mBeginOffset,    HYPHEN); // 3
                    sb.insert(mGroupOffset,    HYPHEN); // 2
                    sb.insert(mPrefixOffset,   HYPHEN); // 1

                    return sb.toString();
                }

            }

        }),

        ISSN(new AbstractType("\\b(\\d{4})-?(\\d{3})([\\dxX])\\b") { // checkstyle-disable-line AnonInnerLength

            @Override
            protected String normalize(final Matcher aMatcher, final Value.Hash aHash, final Type aType, final String aValue) {
                final String number1 = aMatcher.group(1);
                final String number2 = aMatcher.group(2);
                final String number = number1 + number2;
                final String checkDigit = aMatcher.group(3).toUpperCase(); // checkstyle-disable-line MagicNumber

                if (!isValid(number, checkDigit)) {
                    return null;
                }

                final String normalizedNumber = number + checkDigit;
                final String variantNumber = number1 + HYPHEN + number2 + checkDigit;

                put(aHash, aType, aValue, normalizedNumber, variantNumber);

                return normalizedNumber;
            }

            @Override
            protected boolean isValid(final int aChecksum, final char aChr) {
                return (aChecksum + (aChr == UPPER ? BASE : aChr - LOWER)) % (BASE + 1) == 0;
            }

        }),

        ZDB(new AbstractType("\\b(\\d{2,10})-?([\\dxX])\\b") { // checkstyle-disable-line AnonInnerLength

            @Override
            protected String normalize(final Matcher aMatcher, final Value.Hash aHash, final Type aType, final String aValue) {
                final String number = aMatcher.group(1);
                final String checkDigit = aMatcher.group(2).toUpperCase();

                if (!isValid(number, checkDigit)) {
                    return null;
                }

                final String normalizedNumber = number + checkDigit;
                final String variantNumber = number + HYPHEN + checkDigit;

                put(aHash, aType, aValue, normalizedNumber, variantNumber);

                return normalizedNumber;
            }

            @Override
            protected boolean isValid(final int aChecksum, final char aChr) {
                return aChecksum % (BASE + 1) == (aChr == UPPER ? BASE : aChr - LOWER);
            }

        });

        private final AbstractType mType;

        Type(final AbstractType aType) {
            mType = aType;
        }

        public String normalize(final String aValue, final Value.Hash aHash) {
            return mType.normalize(aValue, aHash, this);
        }

        public String identifierField() {
            return fieldName(IDENTIFIER_FORMAT);
        }

        public String preferredField() {
            return fieldName(PREFERRED_FORMAT);
        }

        public String variantField() {
            return fieldName(VARIANT_FORMAT);
        }

        private String fieldName(final String aFormat) {
            return aFormat.formatted(name());
        }

    }

}
