package hbz.limetrans.function;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.RecordTransformer;
import org.metafacture.metafix.api.FixContext;

import java.util.List;
import java.util.Map;

public class Once implements FixContext {

    private boolean mExecuted;

    public Once() {
    }

    @Override
    public void execute(final Metafix metafix, final Record record, final List<String> params, final Map<String, String> options, final RecordTransformer recordTransformer) {
        if (!mExecuted) {
            mExecuted = true;
            recordTransformer.transform(record);
        }
    }

}
