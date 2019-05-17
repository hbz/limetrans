package hbz.limetrans;

import hbz.limetrans.util.Helpers;

import org.apache.commons.io.FileUtils;
import org.xbib.common.settings.Settings;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
    public void testMarc21() throws IOException {
        testEqualsReference("marc21");
    }

    @Test
    public void testMarc21Records() throws IOException {
        testEqualsReference("marc21records");
    }

    @Test
    public void testFormeta() throws IOException {
        testEqualsReference("formeta", "formeta");
    }

    @Test
    public void testFormetaPretty() throws IOException {
        testEqualsReference("formeta-pretty", "formeta");
    }

    @Test
    public void testJson() throws IOException {
        testEqualsReference("json", "json");
    }

    @Test
    public void testJsonPretty() throws IOException {
        testEqualsReference("json-pretty", "json");
    }

    @Test
    public void testElasticsearch() throws IOException {
        testElasticsearchEqualsReference("elasticsearch", "");
    }

    @Test
    public void testElasticsearchIdKey() throws IOException {
        testElasticsearchEqualsReference("elasticsearch-id-key", "ocm42328784");
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
        return new LibraryMetadataTransformation(loadSettings(aName));
    }

    private void assertEqualsReference(final String aName, final String aExt) throws IOException {
        assertEquals("Reference data mismatch: " + aName,
                getReference(aName, aExt), slurpFile("src/test/resources", "output", aName, aExt));
    }

    private void testElasticsearchEqualsReference(final String aName, final String aId) throws IOException {
        final Settings settings = loadSettings(aName).getAsSettings("output.elasticsearch");
        final ElasticsearchClient client = new ElasticsearchClient(Helpers.convertSettings(settings));

        try {
            getLimetrans(aName).process();

            assertEquals(getReference(aName, "json"), client.getClient().prepareGet(
                        settings.get(ElasticsearchClient.INDEX_NAME_KEY),
                        settings.get(ElasticsearchClient.INDEX_TYPE_KEY),
                        aId).get().getSourceAsString());
        }
        finally {
            client.close();
            FileUtils.deleteDirectory(new File(settings.get("embeddedPath")));
        }
    }

    private String getReference(final String aName, final String aExt) throws IOException {
        return slurpFile("classpath:", "reference", aName, aExt);
    }

    private Settings loadSettings(final String aName) throws IOException {
        return Helpers.loadSettings(new File("src/conf/test/limetrans-" + aName + ".json"));
    }

    private String slurpFile(final String aPrefix, final String aDir, final String aName, final String aExt) throws IOException {
        return Helpers.slurpFile(aPrefix + "/limetrans/" + aDir + "/" + aName + "." + aExt, getClass());
    }

}
