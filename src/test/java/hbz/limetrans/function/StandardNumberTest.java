package hbz.limetrans.function;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.metafacture.metafix.Value;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class StandardNumberTest {

    private final StandardNumber.Type mType;
    private final String mNormalized;
    private final String mPreferred;
    private final String mValue;

    public StandardNumberTest(final StandardNumber.Type aType, final String aValue, final String aNormalized, final String aPreferred) {
        mType = aType;
        mValue = aValue;
        mNormalized = aNormalized;
        mPreferred = aPreferred;
    }

    @Parameterized.Parameters(name="({index}) {0} \"{1}\"")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { StandardNumber.Type.ISSN, "0178-2509",      "01782509", "01782509" },
            { StandardNumber.Type.ISSN, "01782509",       "01782509", "01782509" },
            { StandardNumber.Type.ISSN, "ISSN 0178-2509", "01782509", "01782509" },
            { StandardNumber.Type.ISSN, "0178250",        null,       null },
            { StandardNumber.Type.ISSN, "0178250X",       null,       null },
            { StandardNumber.Type.ISSN, "HT01782509",     null,       null },

            { StandardNumber.Type.ZDB, "983446-1",    "9834461", "9834461" },
            { StandardNumber.Type.ZDB, "9834461",     "9834461", "9834461" },
            { StandardNumber.Type.ZDB, "ZDB 9834461", "9834461", "9834461" },
            { StandardNumber.Type.ZDB, "983446",      null,      null },
            { StandardNumber.Type.ZDB, "983446X",     null,      null },
            { StandardNumber.Type.ZDB, "HT9834461",   null,      null },
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
    }

}
