package hbz.limetrans.util;

import hbz.limetrans.util.Cli.CliException;

import org.xbib.common.settings.Settings;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CliTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testInvalidOption() throws CliException {
        thrown.expect(CliException.class);
        thrown.expectMessage("Unrecognized option: -o");

        new Cli("program", "").parse(new String[]{"-o", "value"});
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
