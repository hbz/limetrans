package hbz.limetrans;

import org.junit.rules.ExpectedException;
import org.junit.Rule;
import org.junit.Test;
import org.xbib.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

public class LibraryMetadataTransformationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testInputQueueMissingFile() throws IOException {
        testNoInput("missing-file");
    }

    @Test
    public void testInputQueueMissingPath() throws IOException {
        testNoInput("missing-path");
    }

    @Test
    public void testInputQueueMissingPattern() throws IOException {
        testNoInput("missing-pattern");
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

    private void testNoInput(final String aName) throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Could not process limetrans: no input specified.");

        getLimetrans("input-queue-" + aName);
    }

    private void testInputQueueSize(final String aName, final int aSize) throws IOException {
        final LibraryMetadataTransformation limetrans = getLimetrans("input-queue-" + aName);
        assertEquals("Input queue size mismatch: " + aName, aSize, limetrans.getInputQueueSize());
    }

    private LibraryMetadataTransformation getLimetrans(final String aName) throws IOException {
        final URL url = new File("src/conf/test/limetrans-" + aName + ".json").toURI().toURL();
        return new LibraryMetadataTransformation(Helpers.getSettingsFromUrl(url));
    }

}
