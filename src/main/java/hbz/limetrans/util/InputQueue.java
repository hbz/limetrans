package hbz.limetrans.util;

import org.metafacture.framework.LifeCycle;
import org.metafacture.framework.Sender;
import org.metafacture.framework.StreamReceiver;

public interface InputQueue {

    boolean isEmpty();

    int size();

    <T extends StreamReceiver & Sender<StreamReceiver>> LifeCycle process(StreamReceiver aReceiver, T aSender);

    default LifeCycle process(final StreamReceiver aReceiver) {
        return process(aReceiver, null);
    }

}
