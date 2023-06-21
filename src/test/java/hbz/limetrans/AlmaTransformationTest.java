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
    public void testAlmaA96Supplements() throws IOException {
        testAlmaEqualsReference("a96-supplements");
    }

    @Test
    public void testAlma468Supplements() throws IOException {
        testAlmaEqualsReference("468-supplements");
    }

    @Test
    public void testAlma832Supplements() throws IOException {
        testAlmaEqualsReference("832-supplements");
    }

    @Test
    public void testAlma836Supplements() throws IOException {
        testAlmaEqualsReference("836-supplements");
    }

    @Test
    public void testAlmaA96Postprocess() throws IOException {
        testAlmaEqualsReference("a96-postprocess");
    }

    private void testAlmaEqualsReference(final String aName) throws IOException {
        testEqualsReference("alma-" + aName, "json");
    }

}
