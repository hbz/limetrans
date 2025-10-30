/*
 * Copyright (C) 2014 Jörg Prante
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses
 * or write to the Free Software Foundation, Inc., 51 Franklin Street,
 * Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * The interactive user interfaces in modified source and object code
 * versions of this program must display Appropriate Legal Notices,
 * as required under Section 5 of the GNU Affero General Public License.
 *
 */
package org.xbib.standardnumber;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

public class ISBNTest {

    @Test
    public void testDehypenate() {
        Assert.assertEquals("000111333", new ISBN().set("000-111-333").normalize().normalizedValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testISBNTooShort() throws Exception {
        new ISBN().set("12-7").normalize().verify();
    }

    @Test
    public void testDirtyISBN() throws Exception {
        String value = "ISBN 3-9803350-5-4 kart. : DM 24.00";
        ISBN isbn = new ISBN().set(value).normalize().verify();
        Assert.assertEquals(isbn.normalizedValue(), "3980335054");
    }

    @Test(expected = NumberFormatException.class)
    public void testTruncatedISBN() throws Exception {
        String value = "ISBN";
        new ISBN().set(value).normalize().verify();
    }

    @Test
    public void fixChecksum() throws Exception {
        String value = "3616065810";
        ISBN isbn = new ISBN().set(value).createChecksum(true).normalize().verify();
        Assert.assertEquals("361606581X", isbn.normalizedValue());
    }

    @Test
    public void testEAN() throws Exception {
        String value = "978-3-551-75213-0";
        ISBN ean = new ISBN().set(value).ean(true).normalize().verify();
        Assert.assertEquals("9783551752130", ean.normalizedValue());
        Assert.assertEquals("978-3-551-75213-0", ean.format());
    }

    @Test
    public void testEAN2() throws Exception {
        String value = "978-3-551-75213-1";
        ISBN ean = new ISBN().set(value).ean(true).createChecksum(true).normalize().verify();
        Assert.assertEquals("9783551752130", ean.normalizedValue());
        Assert.assertEquals("978-3-551-75213-0", ean.format());
    }

    @Test
    public void testEAN3() throws Exception {
        String value = "9786612563034";
        ISBN ean = new ISBN().set(value).ean(true).createChecksum(true).normalize().verify();
        Assert.assertEquals("9786612563034", ean.normalizedValue());
        Assert.assertEquals("9786612563034", ean.format());
        Assert.assertEquals(null, ean.ean(false).format());
    }

    @Test
    public void testEAN4() throws Exception {
        String value = "9780192132475";
        ISBN ean = new ISBN().set(value).ean(true).createChecksum(true).normalize().verify();
        Assert.assertEquals("9780192132475", ean.normalizedValue());
        Assert.assertEquals("978-0-19-213247-5", ean.format());
        Assert.assertEquals(null, ean.ean(false).format());
    }

    @Test
    public void testEAN5() throws Exception {
        String value = "1933988312";
        ISBN ean = new ISBN().set(value).ean(true).createChecksum(true).normalize().verify();
        Assert.assertEquals("9781933988313", ean.normalizedValue());
        Assert.assertEquals("978-1-933988-31-3", ean.format());
        Assert.assertEquals("1-933988-31-2", ean.ean(false).format());
    }

    @Test
    public void testEAN6() throws Exception {
        String value = "3406548407";
        ISBN ean = new ISBN().set(value).ean(true).createChecksum(true).normalize().verify();
        Assert.assertEquals("9783406548406", ean.normalizedValue());
        Assert.assertEquals("978-3-406-54840-6", ean.format());
        Assert.assertEquals("3-406-54840-7", ean.ean(false).format());
    }

    @Test(expected = NumberFormatException.class)
    public void testWrongAndDirtyEAN() throws Exception {
        // correct ISBN-10 is 3-451-04112-X
        String value = "ISBN ISBN 3-451-4112-X kart. : DM 24.80";
        new ISBN().set(value).ean(false).createChecksum(true).normalize().verify();
    }

    @Test
    public void testVariants() throws Exception {
        String content = "1-9339-8817-7.";
        ISBN isbn = new ISBN().set(content).normalize();
        if (!isbn.isEAN()) {
            // create up to 4 variants: ISBN, ISBN normalized, ISBN-13, ISBN-13 normalized
            if (isbn.isValid()) {
                Assert.assertEquals("1-933988-17-7", isbn.ean(false).format());
                Assert.assertEquals("1933988177", isbn.ean(false).normalizedValue());
            }
            isbn = isbn.ean(true).set(content).normalize();
            if (isbn.isValid()) {
                Assert.assertEquals("978-1-933988-17-7", isbn.format());
                Assert.assertEquals("9781933988177", isbn.normalizedValue());
            }
        }
        else {
            // 2 variants, do not create ISBN-10 for an ISBN-13
            if (isbn.isValid()) {
                Assert.assertEquals(isbn.ean(true).format(), "978-1-933988-17-7");
                Assert.assertEquals(isbn.ean(true).normalizedValue(), "9781933988177");
            }
        }
    }

    public static class ISBN {

        private static final Pattern PATTERN = Pattern.compile("[\\p{Digit}xX\\p{Pd}]{10,17}");

        private static final List<String> ranges = new ISBNRangeMessageConfigurator().getRanges();

        private String eanvalue;
        private String value;
        private boolean createWithChecksum;
        private boolean eanPreferred;
        private boolean isEAN;
        private boolean valid;

        public ISBN() {
        }

        public ISBN set(CharSequence value) {
            this.value = value != null ? value.toString() : null;
            return this;
        }

        public ISBN createChecksum(boolean createWithChecksum) {
            this.createWithChecksum = createWithChecksum;
            return this;
        }

        public ISBN normalize() {
            Matcher m = PATTERN.matcher(value);
            this.value = m.find() ? dehyphenate(value.substring(m.start(), m.end())) : null;
            return this;
        }

        public boolean isValid() throws NumberFormatException {
            return value != null && !value.isEmpty() && check() && (eanPreferred ? eanvalue != null : value != null);
        }

        public ISBN verify() throws NumberFormatException {
            if (value == null || value.isEmpty()) {
                throw new NumberFormatException("must not be null");
            }
            check();
            this.valid = eanPreferred ? eanvalue != null : value != null;
            if (!valid) {
                throw new NumberFormatException("invalid number");
            }
            return this;
        }

        public String normalizedValue() {
            return eanPreferred ? eanvalue : value;
        }

        public String format() {
            if ((!eanPreferred && value == null) || eanvalue == null) {
                return null;
            }
            return eanPreferred ?
                fix(eanvalue) :
                fix("978" + value).substring(4);
        }

        public boolean isEAN() {
            return isEAN;
        }

        public ISBN ean(boolean preferEAN) {
            this.eanPreferred = preferEAN;
            return this;
        }

        private String hyphenate(String prefix, String isbn) {
            StringBuilder sb = new StringBuilder(prefix.substring(0, 4)); // '978-', '979-'
            prefix = prefix.substring(4);
            isbn = isbn.substring(3); // 978, 979
            int i = 0;
            int j = 0;
            while (i < prefix.length()) {
                char ch = prefix.charAt(i++);
                if (ch == '-') {
                    sb.append('-'); // set first hyphen
                }
                else {
                    sb.append(isbn.charAt(j++));
                }
            }
            sb.append('-'); // set second hyphen
            while (j < (isbn.length() - 1)) {
                sb.append(isbn.charAt(j++));
            }
            sb.append('-'); // set third hyphen
            sb.append(isbn.charAt(isbn.length() - 1));
            return sb.toString();
        }

        private boolean check() {
            this.eanvalue = null;
            this.isEAN = false;
            int i;
            int val;
            if (value.length() < 9) {
                return false;
            }
            if (value.length() == 10) {
                // ISBN-10
                int checksum = 0;
                int weight = 10;
                for (i = 0; weight > 0; i++) {
                    val = value.charAt(i) == 'X' || value.charAt(i) == 'x' ? 10
                        : value.charAt(i) - '0';
                    if (val >= 0) {
                        if (val == 10 && weight != 1) {
                            return false;
                        }
                        checksum += weight * val;
                        weight--;
                    }
                    else {
                        return false;
                    }
                }
                String s = value.substring(0, 9);
                if (checksum % 11 != 0) {
                    if (createWithChecksum) {
                        this.value = s + createCheckDigit10(s);
                    }
                    else {
                        return false;
                    }
                }
                this.eanvalue = "978" + s + createCheckDigit13("978" + s);
            }
            else if (value.length() == 13) {
                // ISBN-13 "book land"
                if (!value.startsWith("978") && !value.startsWith("979")) {
                    return false;
                }
                int checksum13 = 0;
                int weight13 = 1;
                for (i = 0; i < 13; i++) {
                    val = value.charAt(i) == 'X' || value.charAt(i) == 'x' ? 10 : value.charAt(i) - '0';
                    if (val >= 0) {
                        if (val == 10) {
                            return false;
                        }
                        checksum13 += (weight13 * val);
                        weight13 = (weight13 + 2) % 4;
                    }
                    else {
                        return false;
                    }
                }
                // set value
                if ((checksum13 % 10) != 0) {
                    if (eanPreferred && createWithChecksum) {
                        // with createChecksum
                        eanvalue = value.substring(0, 12) + createCheckDigit13(value.substring(0, 12));
                    }
                    else {
                        return false;
                    }
                }
                else {
                    eanvalue = value;
                }
                if (!eanPreferred && (eanvalue.startsWith("978") || eanvalue.startsWith("979"))) {
                    // create 10-digit from 13-digit
                    this.value = eanvalue.substring(3, 12) + createCheckDigit10(eanvalue.substring(3, 12));
                }
                else {
                    // 10 digit version not available - not an error
                    this.value = null;
                }
                this.isEAN = true;
            }
            else if (value.length() == 9) {
                String s = value.substring(0, 9);
                // repair ISBN-10 ?
                if (createWithChecksum) {
                    // create 978 from 10-digit without createChecksum
                    eanvalue = "978" + s + createCheckDigit13("978" + s);
                    value = s + createCheckDigit10(s);
                }
                else {
                    return false;
                }
            }
            else if (value.length() == 12) {
                // repair ISBN-13 ?
                if (!value.startsWith("978") && !value.startsWith("979")) {
                    return false;
                }
                if (createWithChecksum) {
                    String s = value.substring(0, 9);
                    String t = value.substring(3, 12);
                    // create 978 from 10-digit
                    this.eanvalue = "978" + s + createCheckDigit13("978" + s);
                    this.value = t + createCheckDigit10(t);
                }
                else {
                    return false;
                }
                this.isEAN = true;
            }
            else {
                return false;
            }
            return true;
        }

        private char createCheckDigit10(String value) throws NumberFormatException {
            int checksum = 0;
            int val;
            int l = value.length();
            for (int i = 0; i < l; i++) {
                val = value.charAt(i) - '0';
                if (val < 0 || val > 9) {
                    throw new NumberFormatException("not a digit in " + value);
                }
                checksum += val * (10-i);
            }
            int mod = checksum % 11;
            return mod == 0 ? '0' : mod == 1 ? 'X' : (char)((11-mod) + '0');
        }

        private char createCheckDigit13(String value) throws NumberFormatException {
            int checksum = 0;
            int weight;
            int val;
            int l = value.length();
            for (int i = 0; i < l; i++) {
                val = value.charAt(i) - '0';
                if (val < 0 || val > 9) {
                    throw new NumberFormatException("not a digit in " + value);
                }
                weight = i % 2 == 0 ? 1 : 3;
                checksum += weight * val;
            }
            int mod = 10 - checksum % 10;
            return mod == 10 ? '0' : (char)(mod + '0');
        }

        private String fix(String isbn) {
            if (isbn == null) {
                return null;
            }
            for (int i = 0; i < ranges.size(); i += 2) {
                if (isInRange(isbn, ranges.get(i), ranges.get(i + 1)) == 0) {
                    return hyphenate(ranges.get(i), isbn);
                }
            }
            return isbn;
        }

        private int isInRange(String isbn, String begin, String end) {
            String b = dehyphenate(begin);
            int blen = b.length();
            int c = blen <= isbn.length() ?
                isbn.substring(0, blen).compareTo(b) :
                isbn.compareTo(b);
            if (c < 0) {
                return -1;
            }
            String e = dehyphenate(end);
            int elen = e.length();
            c = e.compareTo(isbn.substring(0, elen));
            if (c < 0) {
                return 1;
            }
            return 0;
        }

        private String dehyphenate(String isbn) {
            StringBuilder sb = new StringBuilder(isbn);
            int i = sb.indexOf("-");
            while (i >= 0) {
                sb.deleteCharAt(i);
                i = sb.indexOf("-");
            }
            return sb.toString();
        }

        private final static class ISBNRangeMessageConfigurator {

            private final List<String> ranges;
            private final Stack<StringBuilder> content;

            private String prefix;
            private String rangeBegin;
            private String rangeEnd;
            private boolean valid;
            private int length;

            public ISBNRangeMessageConfigurator() {
                content = new Stack<StringBuilder>();
                ranges = new ArrayList<String>();
                length = 0;
                try {
                    InputStream in = getClass().getResourceAsStream("/standardnumber/RangeMessage.xml");
                    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
                    XMLEventReader xmlReader = xmlInputFactory.createXMLEventReader(in);
                    while (xmlReader.hasNext()) {
                        processEvent(xmlReader.peek());
                        xmlReader.nextEvent();
                    }
                }
                catch (XMLStreamException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }

            private void processEvent(XMLEvent e) {
                switch (e.getEventType()) {
                    case XMLEvent.START_ELEMENT: {
                        StartElement element = e.asStartElement();
                        String name = element.getName().getLocalPart();
                        if ("RegistrationGroups".equals(name)) {
                            valid = true;
                        }
                        content.push(new StringBuilder());
                        break;
                    }
                    case XMLEvent.END_ELEMENT: {
                         EndElement element = e.asEndElement();
                         String name = element.getName().getLocalPart();
                         String v = content.pop().toString();
                         if ("Prefix".equals(name)) {
                             prefix = v;
                         }
                         if ("Range".equals(name)) {
                             int pos = v.indexOf('-');
                             if (pos > 0) {
                                 rangeBegin = v.substring(0, pos);
                                 rangeEnd = v.substring(pos + 1);
                             }
                         }
                         if ("Length".equals(name)) {
                             length = Integer.parseInt(v);
                         }
                         if ("Rule".equals(name)) {
                             if (valid && rangeBegin != null && rangeEnd != null) {
                                 if (length > 0) {
                                     ranges.add(prefix + "-" + rangeBegin.substring(0, length));
                                     ranges.add(prefix + "-" + rangeEnd.substring(0, length));
                                 }
                             }
                         }
                         break;
                    }
                    case XMLEvent.CHARACTERS: {
                         Characters c = (Characters) e;
                         if (!c.isIgnorableWhiteSpace()) {
                             String text = c.getData().trim();
                             if (text.length() > 0 && !content.empty()) {
                                 content.peek().append(text);
                             }
                         }
                         break;
                    }
                }
            }

            public List<String> getRanges() {
                return ranges;
            }

        }
    }

}
