package hbz.limetrans.test;

import org.metafacture.javaintegration.EventList.Event;
import org.metafacture.javaintegration.EventList;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public class EventStack {

    private final Iterator<Event> mEvents;
    private final Deque<EventStackEntry> mStack = new ArrayDeque<>();

    public EventStack(final EventList aEventList) {
        mEvents = aEventList.getEvents().iterator();
        mStack.push(new EventStackEntry(null, null));
    }

    public boolean hasNext() {
        return mEvents.hasNext();
    }

    public EventStackEntry next() {
        final Event event = mEvents.next();
        final EventStackEntry entry = new EventStackEntry(event, mStack.peek());

        switch (event.getType()) {
            case START_RECORD:
            case START_ENTITY:
                mStack.push(entry);
                break;
            case END_ENTITY:
            case END_RECORD:
                mStack.pop();
                break;
            default:
                // nothing to do
                break;
        }

        return entry;
    }

}
