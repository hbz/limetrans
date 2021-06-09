package hbz.limetrans.metamorph;

import org.metafacture.metamorph.api.helpers.AbstractSimpleStatelessFunction;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class ZDB extends AbstractSimpleStatelessFunction {

    private static final Pattern PATTERN = Pattern.compile("^(\\d{2,10})-?([\\dxX])\\b");

    private static final int BASE = 10;

    public ZDB() {
    }

    @Override
    public String process(final String aValue) {
        if (aValue != null && !aValue.isEmpty()) {
            final Matcher m = PATTERN.matcher(aValue);

            if (m.find()) {
                final String number = m.group(1);
                final String checkDigit = m.group(2).toUpperCase();

                final int base = number.length() + 1;
                final int checksum = IntStream.range(0, number.length())
                    .map(i -> (number.charAt(i) - '0') * (base - i)).sum();

                final char chr = checkDigit.charAt(0);

                if (checksum % (BASE + 1) == (chr == 'X' ? BASE : chr - '0')) {
                    return number + "-" + checkDigit;
                }
            }
        }

        return null;
    }

}
