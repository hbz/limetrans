package hbz.limetrans.function;

import hbz.limetrans.util.LMDB;

import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.api.FixFunction;

import java.util.List;
import java.util.Map;

public class PutLmdbMap implements FixFunction {

    public PutLmdbMap() {
    }

    @Override
    public void apply(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final String fileName = aParams.get(0);
        aMetafix.putMap(aParams.size() > 1 ? aParams.get(1) : fileName, new LMDB(aMetafix.resolvePath(fileName)));
    }

}
