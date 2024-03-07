package hbz.limetrans.function;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.metafacture.metafix.Value;
import org.xbib.standardnumber.ISBNTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class StandardNumberTest {

    private final StandardNumber.Type mType;
    private final String mNormalized;
    private final String mPreferred;
    private final String mValue;
    private final String[] mVariants;
    private final boolean mCompatibility;

    public StandardNumberTest(final StandardNumber.Type aType, final String aValue, final String aNormalized, final String aPreferred, final String aVariants, final boolean aCompatibility) {
        mType = aType;
        mValue = aValue;
        mNormalized = aNormalized;
        mPreferred = aPreferred;
        mVariants = aVariants != null ? aVariants.isEmpty() ? new String[]{} : aVariants.split(" ") : null;
        mCompatibility = aCompatibility;
    }

    @Parameterized.Parameters(name="({index}) {0} \"{1}\"")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { StandardNumber.Type.ISBN, "978-3-551-75213-0", "9783551752130", "9783551752130", "" /* preferred == actual */,      true },
            { StandardNumber.Type.ISBN, "9781933988313",     "9781933988313", "9781933988313", "978-1-933988-31-3",               true },
            { StandardNumber.Type.ISBN, "9786612563034",     "9786612563034", "9786612563034", "" /* invalid group code */,       true },
            { StandardNumber.Type.ISBN, "9780192132475",     "9780192132475", "9780192132475", "978-0-19-213247-5",               true },
            { StandardNumber.Type.ISBN, "9784000000000",     "9784000000000", "9784000000000", "978-4-00-000000-0",               true },
            { StandardNumber.Type.ISBN, "9789999399999",     "9789999399999", "9789999399999", "978-99993-999-9-9",               true },
            { StandardNumber.Type.ISBN, "1933988312",        "1933988312",    "9781933988313", "978-1-933988-31-3 1-933988-31-2", true },
            { StandardNumber.Type.ISBN, "3406548407",        "3406548407",    "9783406548406", "978-3-406-54840-6 3-406-54840-7", true },
            { StandardNumber.Type.ISBN, "361606581X",        "361606581X",    "9783616065816", "978-3-616-06581-6 3-616-06581-X", true },

            // varying number of hyphens
            { StandardNumber.Type.ISBN, "9-7-83551752130",
                "9783551752130", "9783551752130", "978-3-551-75213-0",                          true },
            { StandardNumber.Type.ISBN, "9-7-8-3551752130",
                "9783551752130", "9783551752130", "978-3-551-75213-0",                          true },
            { StandardNumber.Type.ISBN, "9-7-8-3-551752130",
                "9783551752130", "9783551752130", "978-3-551-75213-0",                          true },
            { StandardNumber.Type.ISBN, "9-7-8-3-5-51752130",
                "9783551752130", "9783551752130", "978-3-551-75213-0",                          false /* compat: null */ },
            { StandardNumber.Type.ISBN, "9----783551752130",
                null,            null,            null,                                         false /* compat: 9783551752130 */ },
            { StandardNumber.Type.ISBN, "9-----783551752130",
                null,            null,            null,                                         true },
            { StandardNumber.Type.ISBN, "3-6-1606581X",
                "361606581X",    "9783616065816", "978-3-616-06581-6 361606581X 3-616-06581-X", true },
            { StandardNumber.Type.ISBN, "3-6-1-606581X",
                "361606581X",    "9783616065816", "978-3-616-06581-6 361606581X 3-616-06581-X", true },
            { StandardNumber.Type.ISBN, "3-6-1-6-0-6-5-81X",
                "361606581X",    "9783616065816", "978-3-616-06581-6 361606581X 3-616-06581-X", true },
            { StandardNumber.Type.ISBN, "3-6-1-6-0-6-5-8-1X",
                "361606581X",    "9783616065816", "978-3-616-06581-6 361606581X 3-616-06581-X", false /* compat: null */ },
            { StandardNumber.Type.ISBN, "3-------61606581X",
                null,            null,            null,                                         false /* compat: 9783616065816 */ },
            { StandardNumber.Type.ISBN, "3--------61606581X",
                null,            null,            null,                                         true },

            // dirty (correct pattern)
            { StandardNumber.Type.ISBN, "ISBN: 1-.93.3-988-31-2 EUro 17.70",
                null,            null,            null,                                         true },
            { StandardNumber.Type.ISBN, "ISBN 3-7691-3150-9 1. Aufl. 2006",
                "3769131509",    "9783769131505", "978-3-7691-3150-5 3769131509 3-7691-3150-9", true },
            { StandardNumber.Type.ISBN, "ISBN 978-3-608-91086-5 (Klett-Cotta) ab der 7. Aufl.",
                "9783608910865", "9783608910865", "978-3-608-91086-5",                          true },
            { StandardNumber.Type.ISBN, "ISBN 88-7336-210-9 35.00 EUR",
                "8873362109",    "9788873362104", "978-88-7336-210-4 8873362109 88-7336-210-9", true },
            { StandardNumber.Type.ISBN, "ISBN 3-9803350-5-4 kart. : DM 24.00",
                "3980335054",    "9783980335058", "978-3-9803350-5-8 3980335054 3-9803350-5-4", true },
            { StandardNumber.Type.ISBN, "1-9339-8817-7.",
                "1933988177",    "9781933988177", "978-1-933988-17-7 1933988177 1-933988-17-7", true },

            // dirty (incorrect pattern)
            { StandardNumber.Type.ISBN, "978-3-451-41123-6DE", null, null, null, false /* compat: 9783451411236 */ },
            { StandardNumber.Type.ISBN, "ISBN9783451411236",   null, null, null, false /* compat: 9783451411236 */ },
            { StandardNumber.Type.ISBN, "HT3451411237",        null, null, null, false /* compat: 9783451411236 */ },

            // accidental match (incorrect ISBN-13 check digit)
            { StandardNumber.Type.ISBN, "ISBN978-3-451-41123-7",
                "3451411237", "9783451411236", "978-3-451-41123-6 3451411237 3-451-41123-7", false /* compat: null */ },

            // incorrect length
            { StandardNumber.Type.ISBN, "97835517521300",                          null, null, null, true },
            { StandardNumber.Type.ISBN, "978355175213",                            null, null, null, true },
            { StandardNumber.Type.ISBN, "19339883120",                             null, null, null, true },
            { StandardNumber.Type.ISBN, "193398831",                               null, null, null, true },
            { StandardNumber.Type.ISBN, "ISBN ISBN 3-451-4112-X kart. : DM 24.80", null, null, null, true },

            // incorrect check digit
            { StandardNumber.Type.ISBN, "9781933988314",     null, null, null, true }, // fix: 9781933988313
            { StandardNumber.Type.ISBN, "1933988311",        null, null, null, true }, // fix: 1933988312
            { StandardNumber.Type.ISBN, "3616065810",        null, null, null, true }, // fix: 361606581X
            { StandardNumber.Type.ISBN, "978-3-551-75213-1", null, null, null, true }, // fix: 9783551752130
            { StandardNumber.Type.ISBN, "978-3-451-41123-X", null, null, null, true }, // fix: 9783451411236

            { StandardNumber.Type.ISSN, "0178-2509",      "01782509", "01782509", "",          false },
            { StandardNumber.Type.ISSN, "01782509",       "01782509", "01782509", "0178-2509", false },
            { StandardNumber.Type.ISSN, "ISSN 0178-2509", "01782509", "01782509", "0178-2509", false },
            { StandardNumber.Type.ISSN, "0178250",        null,       null,       null,        false },
            { StandardNumber.Type.ISSN, "0178250X",       null,       null,       null,        false },
            { StandardNumber.Type.ISSN, "HT01782509",     null,       null,       null,        false },

            { StandardNumber.Type.ZDB, "983446-1",    "9834461", "9834461", "",         false },
            { StandardNumber.Type.ZDB, "9834461",     "9834461", "9834461", "983446-1", false },
            { StandardNumber.Type.ZDB, "ZDB 9834461", "9834461", "9834461", "983446-1", false },
            { StandardNumber.Type.ZDB, "983446",      null,      null,      null,       false },
            { StandardNumber.Type.ZDB, "983446X",     null,      null,      null,       false },
            { StandardNumber.Type.ZDB, "HT9834461",   null,      null,      null,       false },
        });
    }

    @Test
    public void testNormalize() {
        final Value.Hash hash = Value.newHash().asHash();
        Assert.assertEquals("Normalized", mNormalized,
                mType.normalize(mValue, hash));

        final Value preferred = hash.getField(mType.preferredField());
        Assert.assertEquals("Preferred", mPreferred,
                preferred != null ? preferred.asString() : null);

        final Value variants = hash.getField(mType.variantField());
        Assert.assertArrayEquals("Variant", mVariants,
                variants != null ? variants.asArray().stream().map(Value::asString).toArray(String[]::new) : null);
    }

    @Test
    public void testCompatibility() {
        if (mType != StandardNumber.Type.ISBN) {
            return;
        }

        final ISBNTest.ISBN isbn = new ISBNTest.ISBN().set(mValue).normalize();
        final boolean isValid = isbn.isValid(); // => check()

        final String isbn13Preferred = isbn.ean(true).normalizedValue();
        final String isbn13Variant = isbn.ean(true).format();

        final String isbn10Preferred = isbn.ean(false).normalizedValue();
        final String isbn10Variant = isbn.ean(false).format();

        final List<String> variants = new ArrayList<>();

        if (!mValue.equals(isbn13Variant)) {
            variants.add(isbn13Variant);
        }

        if (isValid && !isbn.isEAN()) {
            if (!mValue.equals(isbn10Preferred)) {
                variants.add(isbn10Preferred);
            }

            variants.add(isbn10Variant);
        }

        try {
            Assert.assertEquals("Preferred", mPreferred, isbn13Preferred);
            Assert.assertArrayEquals("Variant", mVariants != null ? mVariants : new String[]{null}, variants.toArray(new String[variants.size()]));
        }
        catch (final AssertionError e) {
            if (mCompatibility) {
                throw e;
            }
            else {
                return;
            }
        }

        Assert.assertTrue("Expected incompatible, but was compatible", mCompatibility);
    }

}
