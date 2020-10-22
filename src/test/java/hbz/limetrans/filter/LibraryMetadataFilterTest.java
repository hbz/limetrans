package hbz.limetrans.filter;

import hbz.limetrans.util.Helpers;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class LibraryMetadataFilterTest {

    private static final String BASE_PATH = "src/test/resources/filter";
    private static final String INPUT_PATH = BASE_PATH + "/input/test.xml";
    private static final String OUTPUT_PATH = BASE_PATH + "/output/test.json";

    @Test
    public void testMissingFile() throws IOException {
        final Throwable ex = Assert.assertThrows(IllegalArgumentException.class,
                () -> getFilter("missing-file", null, null));

        Assert.assertEquals("Could not process limetrans filter: no input specified.", ex.getMessage());
    }

    @Test
    public void testMissingFilter() throws IOException {
        testEqualsReference("missing-filter");
    }

    @Test
    public void testEmptyFilter() throws IOException {
        testEqualsReference("missing-filter", "any", new String[][]{{}});
    }

    @Test
    public void testEmptyFirstFilter() throws IOException {
        testEqualsReference("missing-filter", "any", new String[][]{{}, {"Mathematics"}, {"Children's literature."}});
    }

    @Test
    public void testEmptySubsequentFilter() throws IOException {
        testEqualsReference("empty-filter", "any", new String[][]{{"Mathematics"}, {}, {"Children's literature."}});
    }

    @Test
    public void testPresence() throws IOException {
        testEqualsReference("presence", "@7102 .a");
    }

    @Test
    public void testPresenceEquals() throws IOException {
        testEqualsReference("presence-equals", "@7102 .a=Mathematisches Forschungsinstitut Oberwolfach.");
    }

    @Test
    public void testPresenceRegexp() throws IOException {
        testEqualsReference("presence-equals", "@7102 .a=~Oberwolfach");
    }

    @Test
    public void testAbsence() throws IOException {
        testEqualsReference("absence", "!7102 .a");
    }

    @Test
    public void testAbsenceEquals() throws IOException {
        testEqualsReference("absence-equals", "!7102 .a=Mathematisches Forschungsinstitut Oberwolfach.");
    }

    @Test
    public void testAbsenceRegexp() throws IOException {
        testEqualsReference("absence-equals", "!7102 .a=~Oberwolfach");
    }

    @Test
    public void testRecordId() throws IOException {
        testEqualsReference("record-id", "001=ocm44959477");
    }

    @Test
    public void testFieldEquals() throws IOException {
        testEqualsReference("field-equals", "650??.a=Mathematics");
    }

    @Test
    public void testFieldRegexp() throws IOException {
        testEqualsReference("field-regexp", "856??.u=~book");
    }

    @Test
    public void testAnyEquals() throws IOException {
        testEqualsReference("any-equals", "Print version:");
    }

    @Test
    public void testAnyRegexp() throws IOException {
        testEqualsReference("any-regexp", "~Science");
    }

    @Test
    public void testNestedAny() throws IOException {
        testEqualsReference("nested-any", "any", new String[][]{{"650??.a=Mathematics", "@042??.a"}, {"856??.u=~book", "!001=ocm44954079", "!001=ocm47011858"}});
    }

    @Test
    public void testNestedAll() throws IOException {
        testEqualsReference("nested-all", "all", new String[][]{{"650??.a=Mathematics", "@042??.a"}, {"856??.u=~book", "!001=ocm44954079", "!001=ocm47011858"}});
    }

    @Test
    public void testNestedNone() throws IOException {
        testEqualsReference("nested-none", "none", new String[][]{{"650??.a=Mathematics", "@042??.a"}, {"856??.u=~book"}});
    }

    private void testEqualsReference(final String aName, final String... aFilters) throws IOException {
        testEqualsReference(aName, "any", new String[][]{aFilters});
    }

    private void testEqualsReference(final String aName, final String aOperator, final String[][] aFilters) throws IOException {
        getFilter(INPUT_PATH, aOperator, aFilters).process();

        Assert.assertEquals("Reference data mismatch: " + aName,
                Helpers.slurpFile(BASE_PATH + "/reference/" + aName + ".json"),
                Helpers.slurpFile(OUTPUT_PATH));
    }

    private LibraryMetadataFilter getFilter(final String aInput, final String aOperator, final String[][] aFilters) throws IOException {
        return new LibraryMetadataFilter("MARCXML", new String[]{aInput}, LibraryMetadataFilter.DEFAULT_KEY, aOperator, aFilters, OUTPUT_PATH, true);
    }

}
