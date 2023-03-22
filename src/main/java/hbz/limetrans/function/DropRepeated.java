package hbz.limetrans.function;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.Value;
import org.metafacture.metafix.api.FixFunction;

import java.util.List;
import java.util.Map;

public class DropRepeated implements FixFunction {

    public DropRepeated() {
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final int index = Integer.parseInt(aOptions.getOrDefault("index", "1")) - 1;

        aParams.forEach(field -> {
            final Value value = aRecord.get(field);

            if (value != null) {
                value.matchType().ifArray(a -> aRecord.set(field, a.get(index)));
            }
        });
    }

}
