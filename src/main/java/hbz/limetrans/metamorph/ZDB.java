package hbz.limetrans.metamorph;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.culturegraph.mf.metamorph.api.helpers.AbstractSimpleStatelessFunction;

public class ZDB extends AbstractSimpleStatelessFunction {

    private static final Pattern PATTERN = Pattern.compile("^(\\d{2,10})-?([\\dxX])\\b");

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

                if (checksum % 11 == (chr == 'X' ? 10 : chr - '0')) {
                    return number + checkDigit;
                }
            }
        }

        return null;
    }

}
