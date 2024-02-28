package hbz.limetrans.function;

import org.metafacture.metamorph.api.helpers.AbstractSimpleStatelessFunction;

public class ZDB extends AbstractSimpleStatelessFunction {

    public ZDB() {
    }

    @Override
    public String process(final String aValue) {
        return aValue != null && !aValue.isEmpty() ? StandardNumber.normalizeZDB(aValue) : null;
    }

}
