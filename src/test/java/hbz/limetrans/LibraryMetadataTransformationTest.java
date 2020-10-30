package hbz.limetrans;

import hbz.limetrans.test.TransformationTestCase;
import hbz.limetrans.util.Helpers;

import org.apache.commons.io.FileUtils;
import org.xbib.common.settings.Settings;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class LibraryMetadataTransformationTest {

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
        final Throwable ex = Assert.assertThrows(IllegalArgumentException.class,
                () -> getLimetrans("input-queue-" + aName));

        Assert.assertEquals("Could not process limetrans: no input specified.", ex.getMessage());
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
        final String referenceFile = getReferenceFile(aName, aExt);

        TransformationTestCase.evaluateTransformation(referenceFile, l -> limetrans.process(l));

        testLimetransEqualsReference(limetrans, aName, referenceFile,
                () -> {
                    try {
                        return Helpers.slurpFile("src/test/resources/limetrans/output/" + aName + "." + aExt);
                    }
                    catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private void testElasticsearchEqualsReference(final String aName, final String aId) throws IOException {
        final Settings settings = loadSettings(aName).getAsSettings("output.elasticsearch");
        final ElasticsearchClient client = new ElasticsearchClient(Helpers.convertSettings(settings));

        try {
            testLimetransEqualsReference(getLimetrans(aName), aName, getReferenceFile(aName, "json"),
                    () -> client.getClient().prepareGet(client.getIndexName(), client.getIndexType(), aId).get().getSourceAsString());
        }
        finally {
            client.close();
            FileUtils.deleteDirectory(new File(settings.get("embeddedPath")));
        }
    }

    private void testLimetransEqualsReference(final LibraryMetadataTransformation aLimetrans, final String aName, final String aReferenceFile, final Supplier<String> aOutputSupplier) throws IOException {
        aLimetrans.process();
        assertEquals("Reference data mismatch: " + aName, Helpers.slurpFile(aReferenceFile), aOutputSupplier.get());
    }

    private LibraryMetadataTransformation getLimetrans(final String aName) throws IOException {
        return new LibraryMetadataTransformation(loadSettings(aName));
    }

    private String getReferenceFile(final String aName, final String aExt) throws IOException {
        return Helpers.getClasspathUrl(getClass(), "classpath:/limetrans/reference/" + aName + "." + aExt).getPath();
    }

    private Settings loadSettings(final String aName) throws IOException {
        return Helpers.loadSettings(new File("src/conf/test/limetrans-" + aName + ".json"));
    }

}
