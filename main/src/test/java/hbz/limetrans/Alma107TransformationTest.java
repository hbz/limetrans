package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class Alma107TransformationTest extends AbstractTransformationTest {

    public Alma107TransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("107");
    }

}
