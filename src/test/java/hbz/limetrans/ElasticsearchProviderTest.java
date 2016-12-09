package hbz.limetrans;

import hbz.limetrans.util.Helpers;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xbib.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ElasticsearchProviderTest {

    private static ElasticsearchProvider mEsProvider;

    @BeforeClass
    public static void setup() throws IOException {
        final URL url = new File("./src/conf/test/elasticsearch-provider-update-test.json").toURI().toURL();
        Settings elasticsearchSettings = Helpers.getSettingsFromUrl(url);
        mEsProvider = new ElasticsearchProvider(elasticsearchSettings);
    }

    @AfterClass
    public static void teardown() {
        mEsProvider.close();
    }

    @Test
    public void testAllDocumentsAreIndexed() throws IOException {
        mEsProvider.bulkIndex("./src/test/resources/elasticsearch/update-test-new.jsonl");
        assertEquals("Document count mismatch", 4, mEsProvider.getCount());
    }

    @Test
    public void testUpdate() throws IOException {
        Map<String, Object> document;

        mEsProvider.bulkIndex("./src/test/resources/elasticsearch/update-test-new.jsonl");
        document = mEsProvider.getDocument("3");
        assertField("Error on bulking new data", "Trollope, Anthony", document, "Person", "personName");
        assertField("Error on bulking new data", "1925-", document, "PersonCreator", 0, "personBio");

        mEsProvider.bulkIndex("./src/test/resources/elasticsearch/update-test-update.jsonl");
        document = mEsProvider.getDocument("3");
        assertField("Error on bulking updated data", "Trollope, Anthony", document, "Person", "personName");
        assertField("Error on bulking updated data", "1926-", document, "PersonCreator", 0, "personBio");
    }

    private void assertField(final String aMsg, final String aExpected,
            final String aActual, final String... aPath) {
        assertEquals(aMsg + ": " + String.join(".", aPath), aExpected, aActual);
    }

    private void assertField(final String aMsg, final String aExpected,
            final Map<String, Object> document, final String aEntity, final String aField) {
        final String actual = ((Map<String, String>) document.get(aEntity)).get(aField);

        assertField(aMsg, aExpected, actual, aEntity, aField);
    }

    private void assertField(final String aMsg, final String aExpected,
            final Map<String, Object> document, final String aEntity, final int aIndex, final String aField) {
        final String actual = ((List<Map<String, String>>) document.get(aEntity)).get(aIndex).get(aField);

        assertField(aMsg, aExpected, actual, aEntity, aField);
    }

}
