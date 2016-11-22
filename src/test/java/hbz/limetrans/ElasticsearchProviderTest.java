package hbz.limetrans;

import org.junit.BeforeClass;
import org.junit.Test;

import org.xbib.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

/**
 * Created by boeselager on 22.11.16.
 */
public class ElasticsearchProviderTest {

    private static ElasticsearchProvider mEsProvider;

    @BeforeClass
    public static void setup() throws IOException {
        final URL url = new File("./src/conf/test/elasticsearch-provider-update-test.json").toURI().toURL();
        Settings elasticsearchSettings = Helpers.getSettingsFromUrl(url);
        mEsProvider = new ElasticsearchProvider(elasticsearchSettings);
    }

    @Test
    public void testUpdate() throws IOException, ExecutionException, InterruptedException {
        mEsProvider.bulkIndex("./src/test/resources/elasticsearch/update-test-new.jsonl");
        final Map<String, Object> documentNew = mEsProvider.getDocument("3");
        documentNew.get("PersonCreator").toString().contains("1925-");
        mEsProvider.bulkIndex("./src/test/resources/elasticsearch/update-test-update.jsonl");
        final Map<String, Object> documentUpdate = mEsProvider.getDocument("3");
        documentUpdate.get("PersonCreator").toString().contains("1926-");
    }

}
