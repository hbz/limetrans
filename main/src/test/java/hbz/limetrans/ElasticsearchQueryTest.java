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
    private static final boolean LOG_MATCHES = Helpers.getProperty("elasticsearchQueryTestLogMatches", false);

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

    private static final String QUOTED_CQL_ALL_INDEXES = CQL_ALL_INDEXES.formatted("\\\"%s\\\"");

    private static final String BIB_NAME_PERSONAL = SIMPLE_QUERY_STRING.formatted("bib.namePersonal", "%s");

    private static final String TITLE = "{\"TitleStatement\":{\"titleMain\":\"%s\"}}";

    private static final String PERSON = "{\"Person\":{\"personName\":\"%s\"}}";

    private static final String[] E_PAYMENT = new String[]{
        TITLE.formatted("e-payment"),
        TITLE.formatted("epayment"),
        TITLE.formatted("e payment")
    };

    private static final String[] HOLZBAU_ATLAS = new String[]{
        TITLE.formatted("holzbau-atlas"),
        TITLE.formatted("holzbauatlas"),
        TITLE.formatted("holzbau atlas"),
        TITLE.formatted("holzbau"),
        TITLE.formatted("atlas")
    };

    private static final String[] HOLZBAU_ATLAS_IN_CONTEXT = new String[]{
        TITLE.formatted("foo holzbau-atlas bar"),
        TITLE.formatted("foo holzbau atlas bar")
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

    private static final String[] DREI_ZINNEN_GEBIET_IN_CONTEXT = new String[]{
        TITLE.formatted("foo drei-zinnen-gebiet bar"),
        TITLE.formatted("foo drei-zinnengebiet bar"),
        TITLE.formatted("foo drei-zinnen gebiet bar"),
        TITLE.formatted("foo dreizinnen-gebiet bar"),
        TITLE.formatted("foo dreizinnen gebiet bar"),
        TITLE.formatted("foo drei zinnen-gebiet bar"),
        TITLE.formatted("foo drei zinnengebiet bar"),
        TITLE.formatted("foo drei zinnen gebiet bar"),
        TITLE.formatted("foo drei-zinnen bar"),
        TITLE.formatted("foo drei-gebiet bar"),
        TITLE.formatted("foo drei zinnen bar"),
        TITLE.formatted("foo drei gebiet bar"),
        TITLE.formatted("foo zinnen-gebiet bar"),
        TITLE.formatted("foo zinnen gebiet bar")
    };

    private static final String[] DREI_ZINNEN_GEBIET_PERSON = new String[]{
        PERSON.formatted("drei-zinnen-gebiet"),
        PERSON.formatted("drei-zinnengebiet"),
        PERSON.formatted("drei-zinnen gebiet"),
        PERSON.formatted("dreizinnen-gebiet"),
        PERSON.formatted("dreizinnengebiet"),
        PERSON.formatted("dreizinnen gebiet"),
        PERSON.formatted("drei zinnen-gebiet"),
        PERSON.formatted("drei zinnengebiet"),
        PERSON.formatted("drei zinnen gebiet"),
        PERSON.formatted("drei-zinnen"),
        PERSON.formatted("drei-gebiet"),
        PERSON.formatted("dreizinnen"),
        PERSON.formatted("dreigebiet"),
        PERSON.formatted("drei zinnen"),
        PERSON.formatted("drei gebiet"),
        PERSON.formatted("zinnen-gebiet"),
        PERSON.formatted("zinnengebiet"),
        PERSON.formatted("zinnen gebiet"),
        PERSON.formatted("drei"),
        PERSON.formatted("zinnen"),
        PERSON.formatted("gebiet")
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

            {MATCH.formatted("cql.allIndexes", "test"), new String[]{TITLE.formatted("test"), TITLE.formatted("notest")}, new boolean[]{T, F}},

            {CQL_ALL_INDEXES.formatted("e-payment"), E_PAYMENT, new boolean[]{T, N, N}},
            {CQL_ALL_INDEXES.formatted("epayment"),  E_PAYMENT, new boolean[]{T, T, F}},
            {CQL_ALL_INDEXES.formatted("e payment"), E_PAYMENT, new boolean[]{N, F, T}},

            {QUOTED_CQL_ALL_INDEXES.formatted("e-payment"), E_PAYMENT, new boolean[]{T, N, N}},
            {QUOTED_CQL_ALL_INDEXES.formatted("e payment"), E_PAYMENT, new boolean[]{N, F, T}},

            {CQL_ALL_INDEXES.formatted("holzbau-atlas"), HOLZBAU_ATLAS, new boolean[]{T, N, N, F, F}},
            {CQL_ALL_INDEXES.formatted("holzbauatlas"),  HOLZBAU_ATLAS, new boolean[]{T, T, F, F, F}},
            {CQL_ALL_INDEXES.formatted("holzbau atlas"), HOLZBAU_ATLAS, new boolean[]{T, F, T, F, F}},
            {CQL_ALL_INDEXES.formatted("holzbau"),       HOLZBAU_ATLAS, new boolean[]{T, F, T, T, F}},
            {CQL_ALL_INDEXES.formatted("atlas"),         HOLZBAU_ATLAS, new boolean[]{T, F, T, F, T}},

            {QUOTED_CQL_ALL_INDEXES.formatted("holzbau-atlas"), HOLZBAU_ATLAS, new boolean[]{T, N, N, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("holzbau atlas"), HOLZBAU_ATLAS, new boolean[]{N, F, T, F, F}},

            {QUOTED_CQL_ALL_INDEXES.formatted("foo holzbau-atlas"), HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{T, N}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo holzbau atlas"), HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{N, T}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo holzbau"),       HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{T, T}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo atlas"),         HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{Y, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("holzbau-atlas bar"), HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{T, N}},
            {QUOTED_CQL_ALL_INDEXES.formatted("holzbau atlas bar"), HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{N, T}},
            {QUOTED_CQL_ALL_INDEXES.formatted("holzbau bar"),       HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{Y, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("atlas bar"),         HOLZBAU_ATLAS_IN_CONTEXT, new boolean[]{T, T}},

            {CQL_ALL_INDEXES.formatted("drei-zinnen-gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{T, N, N, N, N, F, N, F, N, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-zinnengebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{N, T, F, N, N, F, N, N, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-zinnen gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, N, F, N, N, F, N, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnen-gebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{T, N, F, T, N, N, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnengebiet"),   DREI_ZINNEN_GEBIET, new boolean[]{T, T, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnen gebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{Y, F, T, T, F, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnen-gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{T, N, N, F, F, F, T, N, N, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnengebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{F, T, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnen gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, F, F, F, T, F, T, F, F, F, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-zinnen"),        DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, N, F, N, N, F, N, T, F, N, F, N, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei-gebiet"),        DREI_ZINNEN_GEBIET, new boolean[]{F, F, F, F, F, F, F, F, F, F, T, F, N, F, N, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreizinnen"),         DREI_ZINNEN_GEBIET, new boolean[]{Y, F, T, T, F, T, F, F, F, T, F, T, F, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("dreigebiet"),         DREI_ZINNEN_GEBIET, new boolean[]{F, F, F, F, F, F, F, F, F, F, T, F, T, F, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei zinnen"),        DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, F, F, F, T, F, T, T, F, F, F, T, F, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei gebiet"),        DREI_ZINNEN_GEBIET, new boolean[]{T, F, T, F, F, F, T, F, T, F, T, F, F, F, T, F, F, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnen-gebiet"),      DREI_ZINNEN_GEBIET, new boolean[]{T, N, N, F, F, F, T, N, N, F, F, F, F, F, F, T, N, N, F, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnengebiet"),       DREI_ZINNEN_GEBIET, new boolean[]{F, T, F, F, F, F, T, T, F, F, F, F, F, F, F, T, T, F, F, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnen gebiet"),      DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, F, F, F, T, F, T, F, F, F, F, F, F, T, F, T, F, F, F}},
            {CQL_ALL_INDEXES.formatted("drei"),               DREI_ZINNEN_GEBIET, new boolean[]{T, T, T, F, F, F, T, T, T, T, T, F, F, T, T, F, F, F, T, F, F}},
            {CQL_ALL_INDEXES.formatted("zinnen"),             DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, F, F, F, T, F, T, T, F, F, F, T, F, T, F, T, F, T, F}},
            {CQL_ALL_INDEXES.formatted("gebiet"),             DREI_ZINNEN_GEBIET, new boolean[]{T, F, T, T, F, T, T, F, T, F, T, F, F, F, T, T, F, T, F, F, T}},

            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnen-gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{T, N, N, N, N, F, N, F, N, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnengebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{N, T, F, N, N, F, N, N, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnen gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, N, F, N, N, F, N, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("dreizinnen-gebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{T, N, F, T, N, N, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("dreizinnen gebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, N, F, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnen-gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{N, N, N, F, F, F, T, N, N, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnengebiet"),  DREI_ZINNEN_GEBIET, new boolean[]{F, N, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnen gebiet"), DREI_ZINNEN_GEBIET, new boolean[]{N, F, N, F, F, F, N, F, T, F, F, F, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnen"),        DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, N, F, N, N, F, N, T, F, N, F, N, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-gebiet"),        DREI_ZINNEN_GEBIET, new boolean[]{F, F, F, F, F, F, F, F, F, F, T, F, N, F, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnen"),        DREI_ZINNEN_GEBIET, new boolean[]{N, F, N, F, F, F, T, F, T, N, F, F, F, T, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei gebiet"),        DREI_ZINNEN_GEBIET, new boolean[]{F, F, Y, F, F, F, Y, F, F, F, N, F, F, F, T, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("zinnen-gebiet"),      DREI_ZINNEN_GEBIET, new boolean[]{T, N, N, F, F, F, T, N, N, F, F, F, F, F, F, T, N, N, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("zinnen gebiet"),      DREI_ZINNEN_GEBIET, new boolean[]{N, F, T, F, F, F, N, F, T, F, F, F, F, F, F, N, F, T, F, F, F}},

            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei-zinnen-gebiet"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{T, N, N, N, F, N, F, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei-zinnengebiet"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, T, F, N, F, N, N, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei-zinnen gebiet"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, Y, N, N, N, F, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei-zinnen"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, T, N, N, N, F, N, T, F, N, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo gebiet"),             DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{Y, F, F, Y, F, F, F, F, F, Y, F, F, Y, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo dreizinnen-gebiet"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{T, N, F, T, N, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo dreizinnen gebiet"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, Y, N, T, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo dreizinnen"),         DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{Y, F, T, T, T, F, F, F, T, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei zinnen-gebiet"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, N, N, F, F, T, N, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei"),               DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{T, T, T, F, F, T, T, T, T, T, T, T, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo zinnen-gebiet"),      DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{Y, F, F, F, F, F, F, F, F, F, F, F, T, N}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei zinnengebiet"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, N, F, F, F, T, T, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo zinnengebiet"),       DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, Y, F, F, F, F, F, F, F, F, F, F, T, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei zinnen gebiet"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, N, F, F, N, F, T, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo zinnen"),             DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, Y, F, F, F, F, F, Y, F, F, F, T, T}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei-gebiet"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, F, F, F, F, F, F, F, T, F, N, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei zinnen"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, N, F, F, T, F, T, N, F, T, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo drei gebiet"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, Y, F, F, Y, F, F, F, N, F, T, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("foo zinnen gebiet"),      DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, Y, F, F, F, F, F, F, F, F, F, N, T}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnen-gebiet bar"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{T, F, N, F, F, N, F, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnengebiet bar"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, T, F, F, F, F, N, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnen gebiet bar"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, T, N, N, N, F, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-zinnen bar"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, F, F, F, F, F, F, T, F, N, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("gebiet bar"),             DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{T, F, T, T, T, T, F, T, F, T, F, T, T, T}},
            {QUOTED_CQL_ALL_INDEXES.formatted("dreizinnen-gebiet bar"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{Y, F, F, T, N, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("dreizinnen gebiet bar"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, Y, N, T, F, F, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("dreizinnen bar"),         DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{Y, F, F, Y, F, F, F, F, Y, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnen-gebiet bar"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, N, N, F, F, Y, N, N, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei bar"),               DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{Y, Y, F, F, F, F, F, F, Y, Y, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("zinnen-gebiet bar"),      DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{T, N, N, F, F, T, N, N, F, F, F, F, T, N}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnengebiet bar"),  DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, N, F, F, F, Y, T, F, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("zinnengebiet bar"),       DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, T, F, F, F, Y, T, F, F, F, F, F, Y, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnen gebiet bar"), DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, N, F, F, N, F, T, F, F, F, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("zinnen bar"),             DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, F, F, F, Y, F, F, T, F, T, F, Y, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei-gebiet bar"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, F, F, F, F, F, F, F, T, F, N, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei zinnen bar"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, F, F, F, Y, F, F, N, F, T, F, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("drei gebiet bar"),        DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{F, F, Y, F, F, Y, F, F, F, N, F, T, F, F}},
            {QUOTED_CQL_ALL_INDEXES.formatted("zinnen gebiet bar"),      DREI_ZINNEN_GEBIET_IN_CONTEXT, new boolean[]{N, F, T, F, F, N, F, T, F, F, F, F, N, T}},

            {BIB_NAME_PERSONAL.formatted("drei-zinnen-gebiet"), DREI_ZINNEN_GEBIET_PERSON, new boolean[]{T, T, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei-zinnengebiet"),  DREI_ZINNEN_GEBIET_PERSON, new boolean[]{T, T, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei-zinnen gebiet"), DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, T, F, F, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("dreizinnen-gebiet"),  DREI_ZINNEN_GEBIET_PERSON, new boolean[]{T, T, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("dreizinnengebiet"),   DREI_ZINNEN_GEBIET_PERSON, new boolean[]{T, T, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("dreizinnen gebiet"),  DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, T, F, F, T, F, F, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei zinnen-gebiet"), DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei zinnengebiet"),  DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei zinnen gebiet"), DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, T, F, F, F, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei-zinnen"),        DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, T, F, F, T, F, F, F, T, F, T, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei-gebiet"),        DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, F, F, T, F, T, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("dreizinnen"),         DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, T, F, F, T, F, F, F, T, F, T, F, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("dreigebiet"),         DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, F, F, T, F, T, F, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei zinnen"),        DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, T, F, F, F, F, T, F, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei gebiet"),        DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, T, F, F, F, F, F, T, F, F, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("zinnen-gebiet"),      DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, T, T, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("zinnengebiet"),       DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, T, T, F, F, F, F, F, F, F, T, T, F, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("zinnen gebiet"),      DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, T, F, F, F, F, F, F, F, F, T, F, F, F}},
            {BIB_NAME_PERSONAL.formatted("drei"),               DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, T, T, T, F, F, F, F, T, T, F, F, F, T, F, F}},
            {BIB_NAME_PERSONAL.formatted("zinnen"),             DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, F, F, F, F, F, F, T, F, F, F, F, T, F, F, F, T, F, T, F}},
            {BIB_NAME_PERSONAL.formatted("gebiet"),             DREI_ZINNEN_GEBIET_PERSON, new boolean[]{F, F, T, F, F, T, F, F, T, F, F, F, F, F, T, F, F, T, F, F, T}}
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
                    return;
                }
                else {
                    throw e;
                }
            }

            if (LOG_MATCHES && d.matches) {
                System.out.println(d.id + "=" + d.document);
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
