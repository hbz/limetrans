package hbz.limetrans;

import org.junit.Test;

import java.io.IOException;

public class AlmaHbzTransformationTest extends AbstractTransformationTest {

    public AlmaHbzTransformationTest() {
    }

    @Test
    public void testAlma() throws IOException {
        testAlmaEqualsReference("hbz");
    }

}
