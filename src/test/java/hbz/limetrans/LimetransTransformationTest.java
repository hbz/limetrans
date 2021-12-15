package hbz.limetrans;

import hbz.limetrans.test.TransformationTestCase;
import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class LimetransTransformationTest extends AbstractLimetransTest {

    @Parameterized.Parameters(name="{0}")
    public static Object[] data() {
        return Limetrans.Type.values();
    }

    @Parameterized.Parameter(0)
    public Limetrans.Type mType;

    @Override
    protected Limetrans.Type getType() {
        return mType;
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
    public void testMultipleInputQueues() throws IOException {
        testEqualsReference("multiple-input-queues");
    }

    @Test
    public void testAlmaHBZ() throws IOException {
        testAlmaEqualsReference("hbz");
    }

    @Test
    public void testAlmaA96() throws IOException {
        testAlmaEqualsReference("a96");
    }

    @Test
    public void testAlma468() throws IOException {
        testAlmaEqualsReference("468");
    }

    @Test
    public void testAlmaA96Supplements() throws IOException {
        testAlmaEqualsReference("a96-supplements");
    }

    @Test
    public void testAlma468Supplements() throws IOException {
        testAlmaEqualsReference("468-supplements");
    }

    @Test
    public void testElasticsearch() throws IOException {
        testElasticsearchEqualsReference("elasticsearch", "");
    }

    @Test
    public void testElasticsearchIdKey() throws IOException {
        testElasticsearchEqualsReference("elasticsearch-id-key", "ocm42328784");
    }

    private void testAlmaEqualsReference(final String aName) throws IOException {
        testEqualsReference("alma-" + aName, "json");
    }

    private void testEqualsReference(final String aName) throws IOException {
        testEqualsReference(aName, "jsonl");
    }

    private void testEqualsReference(final String aName, final String aExt) throws IOException {
        final Limetrans limetrans = getLimetrans(aName);

        final String outputFile = getResourcePath(limetrans, "output", aName, aExt);
        final String referenceFile = getReferenceFile(limetrans, aName, aExt);

        Helpers.updateTestFile(referenceFile, () -> {
            limetrans.process();
            return outputFile;
        });

        TransformationTestCase.evaluateTransformation(referenceFile, limetrans::process);

        testLimetransEqualsReference(limetrans, aName, referenceFile, () -> {
            try {
                return Helpers.slurpFile(outputFile);
            }
            catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void testElasticsearchEqualsReference(final String aName, final String aId) throws IOException {
        final Limetrans limetrans = getLimetrans(aName);

        final Settings settings = loadSettings(aName).getAsSettings("output").getAsSettings("elasticsearch");
        final ElasticsearchClient client = new ElasticsearchClient(settings);

        try {
            testLimetransEqualsReference(limetrans, aName, getReferenceFile(limetrans, aName, "json"), () ->
                    client.getClient().prepareGet(client.getIndexName(), client.getIndexType(), aId).get().getSourceAsString());
        }
        finally {
            client.close();
            FileUtils.deleteDirectory(new File(settings.get("embeddedPath")));
        }
    }

    private void testLimetransEqualsReference(final Limetrans aLimetrans, final String aName, final String aReferenceFile, final Supplier<String> aOutputSupplier) throws IOException {
        aLimetrans.process();
        Assert.assertEquals("Reference data mismatch: " + aName, Helpers.slurpFile(aReferenceFile), aOutputSupplier.get());
    }

    private String getReferenceFile(final Limetrans aLimetrans, final String aName, final String aExt) throws IOException {
        return getResourcePath(aLimetrans, "reference", aName, aExt);
    }

    private String getResourcePath(final Limetrans aLimetrans, final String aDir, final String aName, final String aExt) {
        return aLimetrans.pathForType("src/test/resources/limetrans/" + aDir + "%s/" + aName + "." + aExt);
    }

}
