package hbz.limetrans;

import hbz.limetrans.util.Helpers;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class ElasticsearchQueryTest {

    private static final boolean T = true;
    private static final boolean F = false;

    private static final boolean Y = ElasticsearchClient.isLegacy();
    private static final boolean N = !Y;

    private static final boolean DEFERRED = Helpers.getProperty("elasticsearchQueryTestDeferred", false);

    private static final String INDEX_NAME = "index1";
    private static final String INDEX_TYPE = "type1";

    private static final String ID = "id%d";

    private static final String MATCH_ALL = """
    {
        "match_all": {}
    }""";

    private static final String MATCH = """
    {
        "match": {
            "%s": "%s"
        }
    }""";

    private static final String SIMPLE_QUERY_STRING = """
    {
        "simple_query_string": {
            "query": "%2$s",
            "fields": ["%1$s"],
            "analyze_wildcard": true,
            "default_operator": "and",
            "flags": "OR|NOT|PREFIX|PHRASE|PRECEDENCE|ESCAPE|WHITESPACE|FUZZY|NEAR"
        }
    }""";

    private static final String CQL_ALL_INDEXES = SIMPLE_QUERY_STRING.formatted("cql.allIndexes", "%s");

    private static final String TITLE = "{\"TitleStatement\":{\"titleMain\":\"%s\"}}";

    private static final String[] HOLZBAU_ATLAS = new String[]{
        TITLE.formatted("holzbau-atlas"),
        TITLE.formatted("holzbauatlas"),
        TITLE.formatted("holzbau atlas"),
        TITLE.formatted("holzbau"),
        TITLE.formatted("atlas")
    };

    private static final String[] DREI_ZINNEN_GEBIET = new String[]{
        TITLE.formatted("drei-zinnen-gebiet"),
        TITLE.formatted("drei-zinnengebiet"),
        TITLE.formatted("drei-zinnen gebiet"),
        TITLE.formatted("dreizinnen-gebiet"),
        TITLE.formatted("dreizinnengebiet"),
        TITLE.formatted("dreizinnen gebiet"),
        TITLE.formatted("drei zinnen-gebiet"),
        TITLE.formatted("drei zinnengebiet"),
        TITLE.formatted("drei zinnen gebiet"),
        TITLE.formatted("drei-zinnen"),
        TITLE.formatted("drei-gebiet"),
        TITLE.formatted("dreizinnen"),
        TITLE.formatted("dreigebiet"),
        TITLE.formatted("drei zinnen"),
        TITLE.formatted("drei gebiet"),
        TITLE.formatted("zinnen-gebiet"),
        TITLE.formatted("zinnengebiet"),
        TITLE.formatted("zinnen gebiet"),
        TITLE.formatted("drei"),
        TITLE.formatted("zinnen"),
        TITLE.formatted("gebiet")
    };

    private final List<Document> mDocuments = new ArrayList<>();
    private final String mQuery;

    private ElasticsearchClient mClient;

    public ElasticsearchQueryTest(final String aQuery, final String[] aDocuments, final boolean[] aMatches) {
        mQuery = aQuery;

        if (aDocuments.length != aMatches.length) {
            throw new IllegalArgumentException("expected " + aDocuments.length + ", got " + aMatches.length);
        }

        for (int i = 0; i < aDocuments.length; ++i) {
            mDocuments.add(new Document(ID.formatted(i + 1), aDocuments[i], aMatches[i]));
        }
    }

    @Parameterized.Parameters(name = "({index}) {0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {MATCH_ALL, HOLZBAU_ATLAS, new boolean[]{T, T, T, T, T}},

            {MATCH.formatted("cql.allIndexes", "test"), new String[]{TITLE.formatted("test"), TITLE.formatted("notest")}, new boolean[]{Y, F}},

            {CQL_ALL_INDEXES.formatted("holzbau-atlas"), HOLZBAU_ATLAS, new boolean[]{Y, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("holzbauatlas"),  HOLZBAU_ATLAS, new boolean[]{Y, Y, F, F, F}},
            {CQL_ALL_INDEXES.formatted("holzbau atlas"), HOLZBAU_ATLAS, new boolean[]{Y, F, Y, F, F}},
            {CQL_ALL_INDEXES.formatted("holzbau"),       HOLZBAU_ATLAS, new boolean[]{Y, F, Y, Y, F}},
            {CQL_ALL_INDEXES.formatted("atlas"),         HOLZBAU_ATLAS, new boolean[]{Y, F, Y, F, Y}},

            {CQL_ALL_INDEXES.formatted("drei-zinnen-gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-zinnengebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{F, Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-zinnen gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnen-gebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{Y, F, F, Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnengebiet"),   DREI_ZINNEN_GEBIET, new boolean[]{Y, Y, F, Y, Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnen gebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{Y, F, Y, Y, F, Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnen-gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{Y, F, F, F, F, F, Y, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnengebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{F, Y, F, F, F, F, Y, Y, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnen gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, Y, F, Y, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-zinnen"),        DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, F, F, F, Y, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-gebiet"),        DREI_ZINNEN_GEBIET, new boolean[]{F, F, F, F, F, F, F, F, F, F, Y, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnen"),         DREI_ZINNEN_GEBIET, new boolean[]{Y, F, Y, Y, F, Y, F, F, F, Y, F, Y, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreigebiet"),         DREI_ZINNEN_GEBIET, new boolean[]{F, F, F, F, F, F, F, F, F, F, Y, F, Y, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnen"),        DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, Y, F, Y, Y, F, F, F, Y, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei gebiet"),        DREI_ZINNEN_GEBIET, new boolean[]{Y, F, Y, F, F, F, Y, F, Y, F, Y, F, F, F, Y, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnen-gebiet"),      DREI_ZINNEN_GEBIET, new boolean[]{Y, F, F, F, F, F, Y, F, F, F, F, F, F, F, F, Y, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnengebiet"),       DREI_ZINNEN_GEBIET, new boolean[]{F, Y, F, F, F, F, Y, Y, F, F, F, F, F, F, F, Y, Y, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnen gebiet"),      DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, Y, F, Y, F, F, F, F, F, F, Y, F, Y, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei"),               DREI_ZINNEN_GEBIET, new boolean[]{Y, Y, Y, F, F, F, Y, Y, Y, Y, Y, F, F, Y, Y, F, F, F, Y, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnen"),             DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, Y, F, Y, Y, F, F, F, Y, F, Y, F, Y, F, Y, F}},
            {CQL_ALL_INDEXES.formatted("gebiet"),             DREI_ZINNEN_GEBIET, new boolean[]{Y, F, Y, Y, F, Y, Y, F, Y, F, Y, F, F, F, Y, Y, F, Y, F, F, Y}}
        });
    }

    @Before
    public void setup() {
        mClient = ElasticsearchClient.newClient(INDEX_NAME, INDEX_TYPE, b -> b
                .put(new String[]{"index", "settings"}, "classpath:/elasticsearch/hbztitle-settings-%s.json")
                .put(new String[]{"index", "mapping"}, "classpath:/elasticsearch/hbztitle-mapping-%s.json")
        );

        Assert.assertEquals("version mismatch", Y, mClient instanceof ElasticsearchClientV2);
    }

    @After
    public void cleanup() {
        if (mClient != null) {
            mClient.close();
        }
    }

    @Test
    public void test() {
        mDocuments.forEach(d -> {
            Assert.assertNull(d.id, mClient.getDocument(d.id));
            mClient.indexDocument(d.id, d.document);
            Assert.assertEquals(d.id, d.document, mClient.getDocument(d.id));
        });

        mClient.refreshIndex();

        final Map<String, String> results = mClient.searchDocuments(mQuery);
        final List<AssertionError> errors = new ArrayList<>();

        mDocuments.forEach(d -> {
            try {
                Assert.assertEquals(d.id, d.matches ? d.document : null, results.remove(d.id));
            }
            catch (final AssertionError e) {
                if (DEFERRED) {
                    System.out.println(e);
                    errors.add(e);
                }
                else {
                    throw e;
                }
            }
        });

        if (!errors.isEmpty()) {
            throw errors.get(0);
        }

        // Query returned more documents than were indexed (should not happen)
        Assert.assertEquals(results.toString(), 0, results.size());
    }

    private record Document(String id, String document, boolean matches) {
    }

}
