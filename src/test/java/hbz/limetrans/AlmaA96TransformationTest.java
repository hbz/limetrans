package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class AlmaA96TransformationTest extends AbstractTransformationTest {

    public AlmaA96TransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("a96");
    }

    @Test
    public void testAlmaPostprocess() throws IOException {
        testAlmaEqualsReference("a96-postprocess");
    }

}
