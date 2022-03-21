package hbz.limetrans.function;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.api.FixFunction;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class ISSN implements FixFunction {

    private static final Pattern PATTERN = Pattern.compile("\\b(\\d{4})-(\\d{3})([\\dxX])\\b");

    public ISSN() {
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        aRecord.transform(aParams.get(0), s -> {
            final Matcher m = PATTERN.matcher(s);

            if (m.find()) {
                final String number = m.group(1) + m.group(2);
                final String checkDigit = m.group(3).toUpperCase(); // checkstyle-disable-line MagicNumber

                final int base = number.length() + 1;
                final int checksum = IntStream.range(0, number.length())
                    .map(i -> (number.charAt(i) - '0') * (base - i)).sum();

                final char chr = checkDigit.charAt(0);

                if ((checksum + (chr == 'X' ? 10 : chr - '0')) % 11 == 0) { // checkstyle-disable-line MagicNumber
                    return number + checkDigit;
                }
            }

            return null;
        });
    }

}
