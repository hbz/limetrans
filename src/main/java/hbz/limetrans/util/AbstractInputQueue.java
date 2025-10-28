package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.framework.LifeCycle;
import org.metafacture.framework.ObjectReceiver;
import org.metafacture.framework.Sender;
import org.metafacture.framework.StreamReceiver;

public abstract class AbstractInputQueue {

    private static final Logger LOGGER = LogManager.getLogger();

    private int mOrder;

    protected void init(final Settings aSettings) {
        mOrder = aSettings.getAsInt("_order", 0);
    }

    protected static Logger getLogger() {
        return LOGGER;
    }

    public int getOrder() {
        return mOrder;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public abstract int size();

    public LifeCycle process(final StreamReceiver aReceiver) {
        return process(aReceiver, null);
    }

    public abstract <T extends StreamReceiver & Sender<StreamReceiver>> LifeCycle process(StreamReceiver aReceiver, T aSender);

    protected <T> void process(final String aMsg, final ObjectReceiver<T> aOpener, final T aObj) {
        LOGGER.info("Processing " + aMsg);

        try {
            aOpener.process(aObj);
        }
        catch (final Exception e) { // checkstyle-disable-line IllegalCatch
            LOGGER.error("Processing failed:", e);
        }
    }

}
