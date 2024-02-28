package hbz.limetrans.function;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.api.FixFunction;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class StandardNumber implements FixFunction {

    private static final int BASE = 10;

    public StandardNumber() {
    }

    public static String normalizeZDB(final String aValue) {
        return Type.ZDB.normalize(aValue);
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final Type type = Type.valueOf(aParams.get(0));
        aRecord.transform(aParams.get(1), type::normalize);
    }

    private abstract static class AbstractType {

        private final Matcher mMatcher;

        private AbstractType(final String aPattern) {
            mMatcher = Pattern.compile(aPattern).matcher("");
        }

        public String normalize(final String aValue) {
            mMatcher.reset(aValue);
            return mMatcher.find() ? normalize(mMatcher) : null;
        }

        protected abstract String normalize(Matcher aMatcher);

        protected int checksum(final String aNumber) {
            final int base = aNumber.length() + 1;
            return IntStream.range(0, aNumber.length()).map(i -> (aNumber.charAt(i) - '0') * (base - i)).sum();
        }

    }

    private enum Type {

        ISBN(new AbstractType("") {

            @Override
            protected String normalize(final Matcher aMatcher) {
                return null;
            }

        }),

        ISSN(new AbstractType("\\b(\\d{4})-(\\d{3})([\\dxX])\\b") {

            @Override
            protected String normalize(final Matcher aMatcher) {
                final String number = aMatcher.group(1) + aMatcher.group(2);
                final String checkDigit = aMatcher.group(3).toUpperCase(); // checkstyle-disable-line MagicNumber

                final int checksum = checksum(number);
                final char chr = checkDigit.charAt(0);

                if ((checksum + (chr == 'X' ? BASE : chr - '0')) % (BASE + 1) != 0) {
                    return null;
                }

                return number + checkDigit;
            }

        }),

        ZDB(new AbstractType("^(\\d{2,10})-?([\\dxX])\\b") {

            @Override
            protected String normalize(final Matcher aMatcher) {
                final String number = aMatcher.group(1);
                final String checkDigit = aMatcher.group(2).toUpperCase();

                final int checksum = checksum(number);
                final char chr = checkDigit.charAt(0);

                if (checksum % (BASE + 1) != (chr == 'X' ? BASE : chr - '0')) {
                    return null;
                }

                return number + "-" + checkDigit;
            }

        });

        private final AbstractType mType;

        Type(final AbstractType aType) {
            mType = aType;
        }

        public String normalize(final String aValue) {
            return mType.normalize(aValue);
        }

    }

}
