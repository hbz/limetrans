package transformationquality;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SeriesAddedEntryUniformTitleVolume extends AbstractFieldReportTest{

    final private static Logger mLogger = LogManager.getLogger();

    @Parameterized.Parameters
    public static Collection<Object> data() {
        return Arrays.asList("/SeriesAddedEntryUniformTitle/0/volume");
    }
    // TODO: values: SeriesAddedEntryUniformTitle[] vs. reference: SeriesAddedEntryUniformTitle

    public SeriesAddedEntryUniformTitleVolume(String aField) {
        mField = aField;
    }

    @Test
    public void reportField() throws IOException, InterruptedException {
        super.reportField(mLogger);
    }
}
