package hbz.limetrans.filter;

import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Helpers;

import org.metafacture.io.ObjectStdoutWriter;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.InlineMorph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/*
 * Filter examples:
 *
 * - "@001": Record with an ID.
 * - "!001": Record with no ID.
 * - "001=ocn958002247": Record with ID "ocn958002247"
 * - "85642.3=Inhaltstext": Record(s) with field "85642.3" equal to "Inhaltstext"
 * - "85642.3=~Inhaltstext": Record(s) with field "85642.3" matching "Inhaltstext"
 * - "Inhaltstext": Record(s) with any field equal to "Inhaltstext"
 * - "~Inhaltstext": Record(s) with any field matching "Inhaltstext"
 */

public class LimetransFilter {

    private static final String DEFAULT_KEY = "001";

    private final List<LimetransFilter> mChildren = new ArrayList<>();
    private final List<String> mValues = new ArrayList<>();
    private final Operator mOperator;
    private final String mKey;

    private LimetransFilter(final Operator aOperator, final String aKey) {
        mOperator = aOperator;
        mKey = aKey;
    }

    public LimetransFilter(final String aOperator, final String aKey, final String[] aValues) {
        this(Operator.valueOf(aOperator), aKey);
        add(aValues);
    }

    public static LimetransFilter all(final String aKey) {
        return new LimetransFilter(Operator.all, aKey);
    }

    public static LimetransFilter all() {
        return all(null);
    }

    public static LimetransFilter any() {
        return new LimetransFilter(Operator.any, null);
    }

    public static LimetransFilter none() {
        return new LimetransFilter(Operator.none, null);
    }

    public LimetransFilter add(final LimetransFilter... aFilters) {
        Arrays.stream(aFilters).filter(f -> f != null).forEach(mChildren::add);
        return this;
    }

    public LimetransFilter add(final String... aValues) {
        Arrays.stream(aValues).filter(v -> v != null).forEach(mValues::add);
        return this;
    }

    public boolean isEmpty() {
        return mChildren.isEmpty() && mValues.isEmpty();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        build(sb::append);
        return Helpers.prettyXml(sb.toString());
    }

    public Filter toFilter() {
        final InlineMorph metamorph = InlineMorph.in(LimetransFilter.class);
        build(metamorph::with);
        return new Filter(metamorph.create());
    }

    private void build(final Consumer<String> aConsumer) {
        aConsumer.accept("<rules>");
        aConsumer.accept("<entity name=\"\" flushWith=\"record\">");

        if (!isEmpty()) {
            aConsumer.accept("<if>");
            buildClause(aConsumer);
            aConsumer.accept("</if>");
        }

        aConsumer.accept("<data source=\"" + (mKey != null ? mKey : DEFAULT_KEY) + "\" />");
        aConsumer.accept("</entity>");
        aConsumer.accept("</rules>");
    }

    private void buildClause(final Consumer<String> aConsumer) {
        if (isEmpty()) {
            return;
        }

        aConsumer.accept("<" + mOperator + ">");

        mChildren.forEach(f -> f.buildClause(aConsumer));
        mValues.forEach(v -> {
            if (v.startsWith("@")) {
                aConsumer.accept(buildData(v.substring(1), true));
            }
            else if (v.startsWith("!")) {
                aConsumer.accept("<none>");
                aConsumer.accept(buildData(v.substring(1), true));
                aConsumer.accept("</none>");
            }
            else {
                aConsumer.accept(buildData(v, false));
            }
        });

        aConsumer.accept("</" + mOperator + ">");
    }

    private String buildData(final String aFilter, final boolean aFilterSource) {
        final String source;
        final String filter;

        final int index = aFilter.indexOf('=');
        if (index != -1) {
            source = aFilter.substring(0, index);
            filter = aFilter.substring(index + 1);
        }
        else if (aFilterSource) {
            source = aFilter;
            filter = null;
        }
        else {
            source = "*";
            filter = aFilter;
        }

        return "<data source=\"" + source + "\">" + (
                filter == null ? "" : filter.startsWith("~") ?
                    "<regexp match=\"" + filter.substring(1) + "\" />" :
                    "<equals string=\"" + filter + "\" />"
                ) + "</data>";
    }

    public void process(final String[] aInput, final String aOutput, final String aProcessor, final boolean aPretty) throws IOException {
        final FileQueue inputQueue = new FileQueue(aProcessor, true, aInput);

        if (inputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans filter: no input specified.");
        }

        final JsonEncoder encoder = new JsonEncoder();
        encoder.setPrettyPrinting(aPretty);

        final Filter filter = toFilter();
        filter.setReceiver(encoder).setReceiver(aOutput == null || "-".equals(aOutput) ?
                new ObjectStdoutWriter<String>() : new ObjectWriter<String>(aOutput));

        inputQueue.process(filter).closeStream();
    }

    private enum Operator {
        all, any, none
    }

}
