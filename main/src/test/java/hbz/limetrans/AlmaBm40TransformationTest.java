package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class AlmaBm40TransformationTest extends AbstractTransformationTest {

    public AlmaBm40TransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("bm40");
    }

}
