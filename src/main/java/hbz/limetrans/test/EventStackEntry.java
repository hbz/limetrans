package hbz.limetrans.test;

import org.culturegraph.mf.javaintegration.EventList.Event;

import static org.culturegraph.mf.json.JsonEncoder.ARRAY_MARKER;

public class EventStackEntry {

    public static final String SEPARATOR = "/";

    public enum Mismatch {

        TYPE("type"),
        NAME("name"),
        VALUE("value");

        public static final String FORMAT = "Event %s mismatch";

        private final String mText;

        private Mismatch(final String aText) {
            mText = aText;
        }

        @Override
        public String toString() {
            return String.format(FORMAT, mText.toUpperCase());
        }

    }

    private final EventStackEntry mParent;
    private final Event mEvent;

    private int mPosition = 0;

    public EventStackEntry(final Event aEvent, final EventStackEntry aParent) {
        mEvent = aEvent;
        mParent = aParent;

        if (aParent != null) {
            aParent.incrementPosition();
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        if (mParent != null) {
            builder
                .append(mParent)
                .append(SEPARATOR)
                .append(mEvent);

            mParent.appendPosition(builder);
        }

        return builder.toString();
    }

    public Event getEvent() {
        return mEvent;
    }

    public Mismatch getMismatch(final EventStackEntry aEntry) {
        final Event event = aEntry.getEvent();

        final String name = mEvent.getName();
        final String value = mEvent.getValue();

        return mEvent.getType() != event.getType() ? Mismatch.TYPE :
            name != null && !name.equals(event.getName()) ? Mismatch.NAME :
            value != null && !value.equals(event.getValue()) ? Mismatch.VALUE : null;
    }

    private void incrementPosition() {
        if (mParent == null || mEvent.getName().endsWith(ARRAY_MARKER)) {
            ++mPosition;
        }
    }

    private void appendPosition(final StringBuilder aBuilder) {
        if (mPosition > 0) {
            aBuilder
                .append("<")
                .append(mPosition)
                .append(">");
        }
    }

}
