package hbz.limetrans.test;

import org.culturegraph.mf.javaintegration.EventList.Event;
import org.culturegraph.mf.javaintegration.EventList;

import java.util.Iterator;
import java.util.Stack;

public class EventStack {

    private final Iterator<Event> mEvents;
    private final Stack<EventStackEntry> mStack = new Stack<>();

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
