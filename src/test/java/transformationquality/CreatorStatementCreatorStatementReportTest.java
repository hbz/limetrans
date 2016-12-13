package transformationquality;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class CreatorStatementCreatorStatementReportTest extends AbstractFieldReportTest{

    final private static Logger mLogger = LogManager.getLogger();

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList("/CreatorStatement/creatorStatement");
    }

    public CreatorStatementCreatorStatementReportTest(String aField) {
        mField = aField;
    }

    @Test
    public void reportField() throws IOException, InterruptedException {
        super.reportField(mLogger);
    }
}
