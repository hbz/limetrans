package hbz.limetrans.function;

import org.metafacture.metafix.FixCommand;
import org.metafacture.metafix.Metafix;
import org.metafacture.metafix.Record;
import org.metafacture.metafix.api.FixPredicate;
import org.metafacture.metafix.conditional.AnyEqual;
import org.metafacture.metafix.conditional.Exists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@FixCommand("member_local")
public class MemberLocal implements FixPredicate {

    private static final String SUBFIELD = "M";

    public MemberLocal() {
    }

    @Override
    public boolean test(final Metafix aMetafix, final Record aRecord, final List<String> aParams, final Map<String, String> aOptions) {
        final List<String> params = new ArrayList<>();
        params.add(aParams.get(0) + "." + SUBFIELD);

        if (!new Exists().test(aMetafix, aRecord, params, null)) {
            return true;
        }
        else {
            params.add(aMetafix.getVars().get("member"));
            return new AnyEqual().test(aMetafix, aRecord, params, null);
        }
    }

}
