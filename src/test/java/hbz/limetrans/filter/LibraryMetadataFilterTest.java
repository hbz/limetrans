package hbz.limetrans.filter;

import hbz.limetrans.util.Helpers;

import org.xbib.common.settings.Settings;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class LibraryMetadataFilterTest {

    private static final String BASE_PATH = "src/test/resources/filter";
    private static final String INPUT_PATH = BASE_PATH + "/input/test.xml";
    private static final String OUTPUT_PATH = BASE_PATH + "/output/test.json";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMissingFile() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Could not process limetrans filter: no input specified.");

        getFilter("missing-file");
    }

    @Test
    public void testMissingFilter() throws IOException {
        testEqualsReference("missing-filter");
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

    @Ignore("org.metafacture.metamorph.functions.Regexp can't handle null values")
    @Test
    public void testAnyRegexp() throws IOException {
        testEqualsReference("any-regexp", "~Science");
    }

    private void testEqualsReference(final String aName, final String... aFilters) throws IOException {
        getFilter(INPUT_PATH, aFilters).process();

        Assert.assertEquals("Reference data mismatch: " + aName,
                Helpers.slurpFile(BASE_PATH + "/reference/" + aName + ".json"),
                Helpers.slurpFile(OUTPUT_PATH));
    }

    private LibraryMetadataFilter getFilter(final String aInput, final String... aFilters) throws IOException {
        final Settings.Builder settingsBuilder = Settings.settingsBuilder();

        settingsBuilder.putArray("input", new String[]{aInput});
        settingsBuilder.putArray("filter", aFilters);
        settingsBuilder.put("output", OUTPUT_PATH);

        return new LibraryMetadataFilter(settingsBuilder.build());
    }

}
