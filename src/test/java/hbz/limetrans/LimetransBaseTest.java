package hbz.limetrans;

import hbz.limetrans.util.Settings;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LimetransBaseTest extends AbstractLimetransTest {

    @Override
    protected Limetrans.Type getType() {
        return null;
    }

    @Test
    public void testInputQueueMissingFile() throws IOException {
        testNoInput("missing-file");
    }

    @Test
    public void testInputQueueMissingPattern() throws IOException {
        testNoInput("missing-pattern");
    }

    @Test
    public void testInputQueueMissingPathAndPattern() throws IOException {
        testNoInput("missing-path-and-pattern");
    }

    @Test
    public void testInputQueueMissingQueueSetting() throws IOException {
        testNoInput("missing-queue-setting");
    }

    @Test
    public void testInputQueueMissingInputSetting() throws IOException {
        testNoInput("missing-input-setting");
    }

    @Test
    public void testInputQueueFixedPattern() throws IOException {
        testInputQueueSize("fixed-pattern", 1);
    }

    @Test
    public void testInputQueueMissingPath() throws IOException {
        testInputQueueSize("missing-path", 1);
    }

    @Test
    public void testInputQueueWildcardPatternMax1() throws IOException {
        testInputQueueSize("wildcard-pattern-max-1", 1);
    }

    @Test
    public void testInputQueueWildcardPatternMax2() throws IOException {
        testInputQueueSize("wildcard-pattern-max-2", 2);
    }

    @Test
    public void testInputQueueWildcardPatternNoMax() throws IOException {
        testInputQueueSize("wildcard-pattern-no-max", 4);
    }

    @Test
    public void testInputQueueMultiplePatterns() throws IOException {
        testInputQueueSize("multiple-patterns", 4);
    }

    @Test
    public void testInputQueueMultiplePatternsMax() throws IOException {
        testInputQueueSize("multiple-patterns-max", 2);
    }

    @Test
    public void testInputQueueMultipleQueues() throws IOException {
        testInputQueueSize("multiple-queues", 4);
    }

    @Test
    public void testSettingsReplacePlaceholders() throws IOException {
        final Settings settings = loadSettings("settings-replace-placeholders");

        Assert.assertEquals("AB", settings.get("ab"));
        Assert.assertEquals("ABC", settings.get("abc"));
        Assert.assertEquals("ABC", settings.getAsSettings("x").get("y"));
    }

    private void testNoInput(final String aName) throws IOException {
        final Settings settings = loadSettings("input-queue-" + aName);
        final Throwable ex = Assert.assertThrows(IllegalArgumentException.class, () -> getLimetrans(settings));

        Assert.assertEquals("Could not process limetrans: no input specified.", ex.getMessage());
    }

    private void testInputQueueSize(final String aName, final int aSize) throws IOException {
        final Limetrans limetrans = getLimetrans("input-queue-" + aName);
        Assert.assertEquals("Input queue size mismatch: " + aName, aSize, limetrans.getInputQueueSize());
    }

}
