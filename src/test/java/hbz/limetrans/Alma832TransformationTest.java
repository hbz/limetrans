package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class Alma832TransformationTest extends AbstractTransformationTest {

    public Alma832TransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("832");
    }

}
