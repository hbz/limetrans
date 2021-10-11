package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.junit.Assume;

import java.io.FileNotFoundException;
import java.io.IOException;

public abstract class AbstractLimetransTest {

    protected abstract Limetrans.Type getType();

    protected Limetrans getLimetrans(final String aName) throws IOException {
        return getLimetrans(loadSettings(aName));
    }

    protected Limetrans getLimetrans(final Settings aSettings) throws IOException {
        try {
            return new Limetrans(aSettings, getType());
        }
        catch (final FileNotFoundException e) {
            Assume.assumeTrue(getType().getRequired());
            return null;
        }
    }

    protected Settings loadSettings(final String aName) throws IOException {
        return Helpers.loadSettings("src/conf/test/" + aName + ".json");
    }

}
