package hbz.limetrans;

import hbz.limetrans.test.TransformationTestCase;
import hbz.limetrans.util.Helpers;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public abstract class AbstractTransformationTest extends AbstractLimetransTest {

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

    protected void testEqualsReference(final String aName) throws IOException {
        testEqualsReference(aName, "jsonl");
    }

    protected void testEqualsReference(final String aName, final String aExt) throws IOException {
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

    protected void testAlmaEqualsReference(final String aName) throws IOException {
        testEqualsReference("alma-" + aName, "json");
    }

    protected void testElasticsearchEqualsReference(final String aName, final String aId) throws IOException {
        final Limetrans limetrans = getLimetrans(aName);

        final ElasticsearchClient client = ElasticsearchClient.newClient(loadSettings(aName).getAsSettings("output").getAsSettings("elasticsearch"));
        client.setDeleteOnExit(true);

        try {
            testLimetransEqualsReference(limetrans, aName, getReferenceFile(limetrans, aName, "json"), () -> client.getDocument(aId));
        }
        finally {
            client.close();
        }
    }

    protected void testLimetransEqualsReference(final Limetrans aLimetrans, final String aName, final String aReferenceFile, final Supplier<String> aOutputSupplier) throws IOException {
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
