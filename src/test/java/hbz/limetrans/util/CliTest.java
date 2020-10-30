package hbz.limetrans.util;

import hbz.limetrans.util.Cli.CliException;

import org.xbib.common.settings.Settings;

import org.junit.Assert;
import org.junit.Test;

public class CliTest {

    @Test
    public void testInvalidOption() throws CliException {
        final Throwable ex = Assert.assertThrows(CliException.class,
                () -> new Cli("program", "").parse(new String[]{"-o", "value"}));

        Assert.assertEquals("Unrecognized option: -o\nusage: program\n\n", ex.getMessage());
    }

    @Test
    public void testValidOption() throws CliException {
        final Cli cli = new Cli("program", "");
        cli.addOption("o", "opt", true, "option");

        cli.parse(new String[]{"-o", "value"});

        final Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put("opt", "value");

        Assert.assertEquals(settingsBuilder.build(), cli.getAsSettings());
    }

    @Test
    public void testArgs() throws CliException {
        assertArgs("arg1", "arg2");
    }

    @Test
    public void testMissingArgs() throws CliException {
        assertArgs();
    }

    private void assertArgs(final String... aArgs) throws CliException {
        final Cli cli = new Cli("program", "args");

        cli.parse(aArgs);

        final Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.putArray("args", aArgs);

        Assert.assertEquals(settingsBuilder.build(), cli.getAsSettings("args"));
    }

}
