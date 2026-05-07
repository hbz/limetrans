package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class Alma836TransformationTest extends AbstractTransformationTest {

    public Alma836TransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("836");
    }

}
