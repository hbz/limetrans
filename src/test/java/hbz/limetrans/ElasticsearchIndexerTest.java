package hbz.limetrans;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ElasticsearchIndexerTest {

    private static final String ID1 = "ID1";
    private static final String ID2 = "ID2";

    private static final String LITERAL1 = "L1";
    private static final String LITERAL2 = "L2";
    private static final String LITERAL3 = "L3";
    private static final String LITERAL4 = "L4";

    private static final String VALUE1 = "V1";
    private static final String VALUE2 = "V2";
    private static final String VALUE3 = "V3";
    private static final String VALUE4 = "V4";

    private static final String ENTITY1 = "En1";
    private static final String ENTITY2 = "En2";

    private static final String LIST1 = "Li1[]";
    private static final String LIST2 = "Li2[]";
    private static final String LIST3 = "Li3[]";

    private static final String INDEX_NAME = "index1";
    private static final String INDEX_TYPE = "type1";

    private ElasticsearchClient mClient;
    private ElasticsearchIndexer mIndexer;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @After
    public void cleanup() {
        mIndexer.closeStream();
    }

    @Test
    public void testShouldIndexEmptyRecord() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{}");
    }

    @Test
    public void testShouldIndexLiterals() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'L1':'V1','L2':'V2','L3':'V3'}");
    }

    @Test
    public void testShouldIndexEntities() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.startEntity(ENTITY1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.endEntity();
        mIndexer.startEntity(ENTITY2);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.endEntity();
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'En1':{'L1':'V1','L2':'V2'},'En2':{'L1':'V1','L2':'V2'}}");
    }

    @Test
    public void testShouldIndexNestedEntities() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.startEntity(ENTITY1);
        mIndexer.startEntity(ENTITY2);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.endEntity();
        mIndexer.endEntity();
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'En1':{'En2':{'L1':'V1'}}}");
    }

    @Test
    public void testShouldIndexMarkedEntitiesAsList() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.startEntity(LIST1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endEntity();
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'Li1':['V1','V2','V3']}");
    }

    @Test
    public void testShouldIndexEntitiesInLists() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.startEntity(LIST1);
        mIndexer.startEntity(ENTITY1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.endEntity();
        mIndexer.startEntity(ENTITY2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.literal(LITERAL4, VALUE4);
        mIndexer.endEntity();
        mIndexer.endEntity();
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'Li1':[{'L1':'V1','L2':'V2'},{'L3':'V3','L4':'V4'}]}");
    }

    @Test
    public void testShouldIndexNestedLists() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.startEntity(LIST1);
        mIndexer.startEntity(LIST2);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.endEntity();
        mIndexer.startEntity(LIST3);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.literal(LITERAL4, VALUE4);
        mIndexer.endEntity();
        mIndexer.endEntity();
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'Li1':[['V1','V2'],['V3','V4']]}");
    }

    @Test
    public void testShouldIndexDuplicateNames() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'L1':'V1','L1':'V2'}");
    }

    @Test
    public void testShouldFailToIndexFieldWithSeparator() {
        expectBulkFailure(); // MapperParsingException[Field name [L1.L4] cannot contain '.']
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1 + "." + LITERAL4, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();
    }

    @Test
    public void testShouldIndexMultipleDocuments() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endRecord();

        mIndexer.startRecord(ID2);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL2, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'L1':'V1','L2':'V2','L3':'V3'}");
        assertDocument(ID2, "{'L1':'V2','L2':'V3'}");
    }

    @Test
    public void testShouldIndexMultipleBulks() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        mIndexer.startRecord(ID2);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL2, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'L1':'V1','L2':'V2','L3':'V3'}");
        assertDocument(ID2, "{'L1':'V2','L2':'V3'}");
    }

    @Test
    public void testShouldReplaceDocument() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endRecord();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL2, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'L1':'V2','L2':'V3'}");
    }

    @Test
    public void testShouldReplaceDocumentInMultipleBulks() {
        setIndexer();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE1);
        mIndexer.literal(LITERAL2, VALUE2);
        mIndexer.literal(LITERAL3, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL2, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, "{'L1':'V2','L2':'V3'}");
    }

    @Test
    public void testShouldUpdateDocument() {
        final String doc1 = "{'L1':'V1','L2':'V2','L3':'V3'}";
        final String doc2 = "{'L1':'V2','L2':'V2','L3':'V3','L4':'V4'}";

        setIndexer("update");

        assertMissing(ID1);
        insertDocument(ID1, doc1);
        assertDocument(ID1, doc1);

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL4, VALUE4);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, doc2);
    }

    @Test
    public void testShouldFailToUpdateDocumentWithChangedId() {
        final String doc = "{'L1':'V1','L2':'V2','L3':'V3'}";

        expectBulkFailure(); // DocumentMissingException[[type1][ID2]: document missing]
        setIndexer("update");

        assertMissing(ID1);
        insertDocument(ID1, doc);
        assertDocument(ID1, doc);

        mIndexer.startRecord(ID2);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL2, VALUE4);
        mIndexer.endRecord();

        mIndexer.flush();
    }

    @Test
    public void testShouldFailToUpdateMissingDocument() {
        expectBulkFailure(); // DocumentMissingException[[type1][ID1]: document missing]
        setIndexer("update");

        assertMissing(ID1);

        mIndexer.startRecord(ID1);
        mIndexer.literal(LITERAL1, VALUE2);
        mIndexer.literal(LITERAL2, VALUE3);
        mIndexer.endRecord();

        mIndexer.flush();
    }

    @Test
    public void testShouldDeleteDocument() {
        final String doc = "{'L1':'V1','L2':'V2','L3':'V3'}";

        setIndexer("delete");

        assertMissing(ID1);
        insertDocument(ID1, doc);
        assertDocument(ID1, doc);

        mIndexer.startRecord(ID1);
        mIndexer.endRecord();

        mIndexer.flush();

        assertMissing(ID1);
    }

    @Test
    public void testShouldSilentlyDeleteMissingDocument() {
        setIndexer("delete");

        assertMissing(ID1);

        mIndexer.startRecord(ID1);
        mIndexer.endRecord();

        mIndexer.flush();

        assertMissing(ID1);
    }

    @Test
    public void testShouldNotDeleteDocumentWithDifferentId() {
        final String doc = "{'L1':'V1','L2':'V2','L3':'V3'}";

        setIndexer("delete");

        assertMissing(ID1);
        insertDocument(ID1, doc);
        assertDocument(ID1, doc);

        mIndexer.startRecord(ID2);
        mIndexer.endRecord();

        mIndexer.flush();

        assertDocument(ID1, doc);
    }

    private void setIndexer() {
        setIndexer(null);
    }

    private void setIndexer(final String aBulkAction) {
        mClient = new ElasticsearchClient(INDEX_NAME, INDEX_TYPE);
        mIndexer = new ElasticsearchIndexer(mClient, aBulkAction);
    }

    private void assertDocument(final String aId, final String aExpected) {
        assertEquals(fixQuotes(aExpected), getDocument(aId));
    }

    private void assertMissing(final String aId) {
        assertNull(getDocument(aId));
    }

    private void expectBulkFailure() {
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("failure in bulk execution");
        // TODO: specify expected bulk failures
    }

    private void insertDocument(final String aId, final String aDocument) {
        mClient.getClient().prepareIndex(INDEX_NAME, INDEX_TYPE, aId)
            .setSource(fixQuotes(aDocument))
            .get();
    }

    private String getDocument(final String aId) {
        return mClient.getClient().prepareGet(INDEX_NAME, INDEX_TYPE, aId)
            .get().getSourceAsString();
    }

    /*
     * Utility method which replaces all single quotes in a string with double quotes.
     * This allows to specify the JSON output in the test cases without having to wrap
     * each bit of text in escaped double quotes.
     */
    private String fixQuotes(final String str) {
        return str.replace('\'', '"');
    }

}
