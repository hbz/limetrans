package transformationquality;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PersonPersonBioReportTest extends AbstractFieldReportTest{

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList("/Person/personBio");
    }

    public PersonPersonBioReportTest(String aField) {
        mField = aField;
    }

    @Test
    public void reportField() throws IOException, InterruptedException {
        super.reportField();
    }
}
