package transformationquality;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import transformationquality.AbstractFieldReportTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PersonContributorPersonRoleReportTest extends AbstractFieldReportTest{

    final private static Logger mLogger = LogManager.getLogger();

    @Parameterized.Parameters
    public static Collection<Object> data() {
        return Arrays.asList("/PersonContributor/personRole");
    }

    public PersonContributorPersonRoleReportTest(String aField) {
        mField = aField;
    }

    @Test
    public void reportField() throws IOException, InterruptedException {
        super.reportField(mLogger);
    }
}
