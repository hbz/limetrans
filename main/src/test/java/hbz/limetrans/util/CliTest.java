package hbz.limetrans.util;

import hbz.limetrans.util.Cli.CliException;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

public class CliTest {

    private static final String PROGRAM = "program";
    private static final String ARGS = "args";

    public CliTest() {
    }

    @Test
    public void testInvalidOption() throws CliException {
        final Throwable ex = Assert.assertThrows(CliException.class,
                () -> assertCli(c -> {}, s -> {}, "-o", "value"));

        Assert.assertEquals("Unrecognized option: -o\nusage: " + PROGRAM + " " + ARGS + "\n\n", ex.getMessage());
    }

    @Test
    public void testValidOption() throws CliException {
        assertCli(
                c -> c.addOption("o", "opt", "some option", false),
                s -> s.put("opt", "value"),
                "-o", "value");
    }

    @Test
    public void testMultiOption() throws CliException {
        assertCli(
                c -> c.addOption("o", "opt", "some option", true),
                s -> s.put(new String[]{"opt"}, new String[]{"v1", "v2"}),
                "-o", "v1", "v2");
    }

    @Test
    public void testMultiSeparatorOption() throws CliException {
        assertCli(
                c -> c.addOption("o", "opt", "some option", true),
                s -> s.put(new String[]{"opt"}, new String[]{"v1", "v2"}),
                "-o", "v1,v2");
    }

    @Test
    public void testFlag() throws CliException {
        assertCli(
                c -> c.addOption("f", "flag", "some flag"),
                s -> s.put(new String[]{"flag"}, true),
                "-f");
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
        assertCli(c -> {}, s -> s.put(new String[]{ARGS}, aArgs), aArgs);
    }

    private void assertCli(final Consumer<Cli> aCliConsumer, final Consumer<Settings.Builder> aSettingsBuilderConsumer, final String... aArgs) throws CliException {
        final Cli cli = new Cli(PROGRAM, ARGS);
        aCliConsumer.accept(cli);

        final Settings.Builder settingsBuilder = Settings.settingsBuilder();
        aSettingsBuilderConsumer.accept(settingsBuilder);

        cli.parse(aArgs);
        Assert.assertEquals(settingsBuilder.build().getAsFlatMap("."), cli.getAsSettings(ARGS).getAsFlatMap("."));
    }

}
