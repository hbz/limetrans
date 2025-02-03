package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class LimetransTransformationTest extends AbstractTransformationTest {

    public LimetransTransformationTest() {
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
    public void testElasticsearch() throws IOException {
        testElasticsearchEqualsReference("elasticsearch", "1");
    }

    @Test
    public void testElasticsearchIdKey() throws IOException {
        testElasticsearchEqualsReference("elasticsearch-id-key", "ocm42328784");
    }

}
