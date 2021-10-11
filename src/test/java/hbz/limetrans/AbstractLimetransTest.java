package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import java.io.IOException;

public abstract class AbstractLimetransTest {

    protected Limetrans getLimetrans(final String aName) throws IOException {
        return new Limetrans(loadSettings(aName));
    }

    protected Settings loadSettings(final String aName) throws IOException {
        return Helpers.loadSettings("src/conf/test/" + aName + ".json");
    }

}
