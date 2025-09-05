package hbz.limetrans.function;

import org.metafacture.metafix.FixCommand;
import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.api.FixFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@FixCommand("dedup")
public class Dedup implements FixFunction {

    public Dedup() {
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final Set<String> set = new HashSet<>();
        aRecord.transform(aParams.get(0), s -> set.add(s) ? s : null);
    }

}
