package hbz.limetrans.function;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.Value;
import org.metafacture.metafix.api.FixFunction;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class StandardNumber implements FixFunction {

    private static final String IDENTIFIER_FORMAT = "identifier%s";
    private static final String PREFERRED_FORMAT = "preferred%s";

    private static final char LOWER = '0';
    private static final char UPPER = 'X';

    private static final int BASE = 10;

    public StandardNumber() {
    }

    public static String normalizeZDB(final String aValue) {
        return Type.ZDB.normalize(aValue, null);
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final Type type = Type.valueOf(aParams.get(0));
        final Value.Hash value = aRecord.get(aParams.get(1)).asHash();

        final Value number = value.getField(type.identifierField());
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

        protected void put(final Value.Hash aHash, final Type aType, final String aValue, final String aPreferred) {
            if (aHash == null) {
                return;
            }

            if (aPreferred != null) {
                aHash.put(aType.preferredField(), new Value(aPreferred));
            }
        }

    }

    /*package-private*/ enum Type {

        ISBN(new AbstractType("") {

            @Override
            protected String normalize(final Matcher aMatcher, final Value.Hash aHash, final Type aType, final String aValue) {
                return null;
            }

        }),

        ISSN(new AbstractType("\\b(\\d{4})-?(\\d{3})([\\dxX])\\b") { // checkstyle-disable-line AnonInnerLength

            @Override
            protected String normalize(final Matcher aMatcher, final Value.Hash aHash, final Type aType, final String aValue) {
                final String number = aMatcher.group(1) + aMatcher.group(2);
                final String checkDigit = aMatcher.group(3).toUpperCase(); // checkstyle-disable-line MagicNumber

                if (!isValid(number, checkDigit)) {
                    return null;
                }

                final String normalizedNumber = number + checkDigit;
                put(aHash, aType, aValue, normalizedNumber);
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
                put(aHash, aType, aValue, normalizedNumber);
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

        private String fieldName(final String aFormat) {
            return aFormat.formatted(name());
        }

    }

}
