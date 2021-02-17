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
                () -> process(LibraryMetadataFilter.any(), "missing-file"));

        Assert.assertEquals("Could not process limetrans filter: no input specified.", ex.getMessage());
    }

    @Test
    public void testMissingFilter() throws IOException {
        testEqualsReference("missing-filter");
    }

    @Test
    public void testEmptyFilter() throws IOException {
        testEqualsReference("missing-filter", LibraryMetadataFilter.any());
    }

    @Test
    public void testEmptyFirstFilter() throws IOException {
        testEqualsReference("empty-filter", LibraryMetadataFilter.any()
                .add(LibraryMetadataFilter.all())
                .add("Mathematics")
                .add("Children's literature."));
    }

    @Test
    public void testEmptySubsequentFilter() throws IOException {
        testEqualsReference("empty-filter", LibraryMetadataFilter.any()
                .add("Mathematics")
                .add(LibraryMetadataFilter.all())
                .add("Children's literature."));
    }

    @Test
    public void testNullFilter() throws IOException {
        testEqualsReference("empty-filter", LibraryMetadataFilter.any()
                .add("Mathematics")
                .add((String) null)
                .add("Children's literature."));
    }

    @Test
    public void testNullFirstFilter() throws IOException {
        testEqualsReference("empty-filter", LibraryMetadataFilter.any()
                .add(LibraryMetadataFilter.all().add(null, "Mathematics"))
                .add("Children's literature."));
    }

    @Test
    public void testNullSubsequentFilter() throws IOException {
        testEqualsReference("empty-filter", LibraryMetadataFilter.any()
                .add(LibraryMetadataFilter.all().add("Mathematics", null))
                .add("Children's literature."));
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
        testEqualsReference("nested-any", LibraryMetadataFilter.any()
                .add(LibraryMetadataFilter.all().add("650??.a=Mathematics", "@042??.a"))
                .add(LibraryMetadataFilter.all().add("856??.u=~book", "!001=ocm44954079", "!001=ocm47011858")));
    }

    @Test
    public void testNestedAll() throws IOException {
        testEqualsReference("nested-all", LibraryMetadataFilter.all()
                .add(LibraryMetadataFilter.any().add("650??.a=Mathematics", "@042??.a"))
                .add(LibraryMetadataFilter.any().add("856??.u=~book", "!001=ocm44954079", "!001=ocm47011858")));
    }

    @Test
    public void testNestedNone() throws IOException {
        testEqualsReference("nested-none", LibraryMetadataFilter.none()
                .add(LibraryMetadataFilter.any().add("650??.a=Mathematics", "@042??.a"))
                .add("856??.u=~book"));
    }

    private void testEqualsReference(final String aName, final String... aFilters) throws IOException {
        testEqualsReference(aName, LibraryMetadataFilter.any().add(aFilters));
    }

    private void testEqualsReference(final String aName, final LibraryMetadataFilter aFilter) throws IOException {
        process(aFilter, INPUT_PATH);

        Assert.assertEquals("Reference data mismatch: " + aName,
                Helpers.slurpFile(BASE_PATH + "/reference/" + aName + ".json"),
                Helpers.slurpFile(OUTPUT_PATH));
    }

    private void process(final LibraryMetadataFilter aFilter, final String aInput) throws IOException {
        aFilter.process(new String[]{aInput}, OUTPUT_PATH, "MARCXML", true);
    }

}
