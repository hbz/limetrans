package hbz.limetrans.function;

import org.metafacture.metafix.FixConditional;
import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.api.FixPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemberLocal implements FixPredicate {

    private static final String SUBFIELD = "M";

    public MemberLocal() {
    }

    @Override
    @SuppressWarnings("removal")
    public boolean test(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final List<String> params = new ArrayList<>();
        params.add(aParams.get(0) + "." + SUBFIELD);

        if (!FixConditional.exists.test(aMetafix, aRecord, params, null)) {
            return true;
        }
        else {
            params.add(aMetafix.getVars().get("member"));
            return FixConditional.any_equal.test(aMetafix, aRecord, params, null);
        }
    }

}
