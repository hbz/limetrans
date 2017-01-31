package hbz.limetrans;

import hbz.limetrans.util.Helpers;

import org.junit.rules.ExpectedException;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class LibraryMetadataTransformationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    public void testMultiplePatterns() throws IOException {
        testInputQueueSize("multiple-patterns", 4);
    }

    @Test
    public void testMultiplePatternsMax() throws IOException {
        testInputQueueSize("multiple-patterns-max", 2);
    }

    @Test
    public void testUnicodeNormalizationComposed() throws IOException {
        testEqualsReference("unicode-normalization-composed");
    }

    @Test
    public void testUnicodeNormalizationDecomposed() throws IOException {
        testEqualsReference("unicode-normalization-decomposed");
    }

    @Test
    public void testFormeta() throws IOException {
        testEqualsReference("formeta", "formeta");
    }

    @Test
    public void testFormetaPretty() throws IOException {
        testEqualsReference("formeta-pretty", "formeta");
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

    private void testEqualsReference(final String aName) throws IOException {
        testEqualsReference(aName, "jsonl");
    }

    private void testEqualsReference(final String aName, final String aExt) throws IOException {
        final LibraryMetadataTransformation limetrans = getLimetrans(aName);

        limetrans.process();

        assertEqualsReference(aName, aExt);
    }

    private LibraryMetadataTransformation getLimetrans(final String aName) throws IOException {
        final File file = new File("src/conf/test/limetrans-" + aName + ".json");
        return new LibraryMetadataTransformation(Helpers.loadSettings(file));
    }

    private void assertEqualsReference(final String aName, final String aExt) throws IOException {
        assertEquals("Reference data mismatch: " + aName,
                slurpFile("classpath:", "reference", aName, aExt),
                slurpFile("src/test/resources", "output", aName, aExt));
    }

    private String slurpFile(final String aPrefix, final String aDir, final String aName, final String aExt) throws IOException {
        return Helpers.slurpFile(aPrefix + "/limetrans/" + aDir + "/" + aName + "." + aExt, getClass());
    }

}
