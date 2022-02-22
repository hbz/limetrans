package hbz.limetrans.function;

import org.metafacture.framework.StandardEventNames;
import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.Value;
import org.metafacture.metafix.api.FixFunction;

import java.util.List;
import java.util.Map;

public class DebugRecord implements FixFunction {

    public DebugRecord() {
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final Value id = aRecord.get(aOptions.getOrDefault("id", StandardEventNames.ID));
        final String prefix = (Value.isNull(id) || id.toString().isEmpty() ? "" : "[" + id + "] ") + (aParams.size() > 0 ? aParams.get(0) + ": " : "");

        if (getBoolean(aOptions, "pretty")) {
            aRecord.forEach((f, v) -> System.out.println(prefix + f + "=" + v));
        }
        else {
            System.out.println(prefix + aRecord);
        }
    }

}
