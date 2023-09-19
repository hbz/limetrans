package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class AlmaTransformationTest extends LimetransTransformationTest {

    @Test
    public void testAlmaHBZ() throws IOException {
        testAlmaEqualsReference("hbz");
    }

    @Test
    public void testAlmaA96() throws IOException {
        testAlmaEqualsReference("a96");
    }

    @Test
    public void testAlma107() throws IOException {
        testAlmaEqualsReference("107");
    }

    @Test
    public void testAlma468() throws IOException {
        testAlmaEqualsReference("468");
    }

    @Test
    public void testAlma832() throws IOException {
        testAlmaEqualsReference("832");
    }

    @Test
    public void testAlma836() throws IOException {
        testAlmaEqualsReference("836");
    }

    @Test
    public void testAlmaA96Postprocess() throws IOException {
        testAlmaEqualsReference("a96-postprocess");
    }

    private void testAlmaEqualsReference(final String aName) throws IOException {
        testEqualsReference("alma-" + aName, "json");
    }

}
