package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class Alma468TransformationTest extends AbstractTransformationTest {

    public Alma468TransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("468");
    }

}
