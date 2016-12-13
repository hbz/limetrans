package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.*;

@RunWith(Parameterized.class)
public class FieldReportTest extends AbstractTransformationTest{

    final private static Logger mLogger = LogManager.getLogger();
    final private String mField;
    final private static Set<String> mWorkingDocs = new HashSet<>();
    final private static Set<String> mMissingFields = new HashSet<>();
    final private static Set<String> mMissingInRef = new HashSet<>();
    final private static Set<String> mMissingReferences = new HashSet<>();
    final private static Map<String, ErrorFieldPair> mErrors = new HashMap<>();


    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList("CreatorStatement");
    }

    public FieldReportTest(String aField) {
        mField = aField;
    }

    @Test
    public void reportField() throws IOException, InterruptedException {
        String line = mReader.readLine();
        while (line != null){
            JsonNode document = mMapper.readTree(line);
            String ocm = document.get("RecordIdentifier").get("identifierForTheRecord").asText().substring(8);
            JsonNode reference = mReference.get(mReferenceMap.get(ocm));
            compare(ocm, document, reference);
            line = mReader.readLine();
        }
        if (!mMissingFields.isEmpty()){
            mLogger.error("MISSING FIELDS IN TRANSFORMED DATA:");
            mMissingFields.forEach(x -> mLogger.error("\t" + x));
        }
        if (!mMissingInRef.isEmpty()){
            mLogger.error("MISSING FIELDS IN REFERENCE DOCUMENT:");
            mMissingInRef.forEach(x -> mLogger.error("\t" + x));
        }
        if (!mMissingReferences.isEmpty()){
            mLogger.error("MISSING REFERENCE DOCUMENTS:");
            mMissingReferences.forEach(x -> mLogger.error("\t" + x));
        }
        if (!mErrors.isEmpty()){
            mLogger.error("DIVERGENT TRANSFORMATION:");
            mErrors.forEach((x, y) -> mLogger.error("\t".concat(x).concat("\n").concat(y.toString())));
        }

    }

    public void compare(final String aId, final JsonNode aDocument, final JsonNode aReference){
        if (aReference == null){
            mMissingReferences.add(aId);
            return;
        }
        JsonNode field = aDocument.get(mField);
        JsonNode ref = aReference.get(mField);
        if (field == null){
            if (ref != null) {
                mMissingFields.add(aId);
            }
            return;
        }
        if (ref == null){
            mMissingInRef.add(aId);
            return;
        }
        if (field.equals(ref)){
            mWorkingDocs.add(aId);
            return;
        }
        // else
        mErrors.put(aId, new ErrorFieldPair(ref.toString(), field.toString()));
    }

    class ErrorFieldPair {

        final String mReference;
        final String mValue;

        ErrorFieldPair(final String aReference, final String aValue){
            mReference = aReference;
            mValue = aValue;
        }

        public String toString(){
            return "\t\tRef: ".concat(mReference).concat("\n\t\tVal: ").concat(mValue);
        }
    }
}
