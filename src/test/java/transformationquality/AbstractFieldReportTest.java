package transformationquality;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class AbstractFieldReportTest extends AbstractTransformationTest{

    protected String mField;
    final private static Set<String> mWorkingDocs = new HashSet<>();
    final private static Set<String> mMissingFields = new HashSet<>();
    final private static Set<String> mMissingInRef = new HashSet<>();
    final private static Set<String> mMissingReferences = new HashSet<>();
    final private static Map<String, ErrorFieldPair> mErrors = new HashMap<>();


    public void reportField(final Logger aLogger) throws IOException, InterruptedException {
        String line = mReader.readLine();
        while (line != null){
            JsonNode document = mMapper.readTree(line);
            String ocm = document.get("RecordIdentifier").get("identifierForTheRecord").asText().substring(8);
            JsonNode reference = mReference.get(mReferenceMap.get(ocm));
            compare(ocm, document, reference);
            line = mReader.readLine();
        }
        if (!mMissingFields.isEmpty()){
            aLogger.error("MISSING FIELDS IN TRANSFORMED DATA (" + mMissingFields.size() + "):");
            mMissingFields.forEach(x -> aLogger.error("\t" + x));
        }
        if (!mMissingInRef.isEmpty()){
            aLogger.error("MISSING FIELDS IN REFERENCE DOCUMENT (" + mMissingInRef.size() + "):");
            mMissingInRef.forEach(x -> aLogger.error("\t" + x));
        }
        if (!mMissingReferences.isEmpty()){
            aLogger.error("MISSING REFERENCE DOCUMENTS (" + mMissingReferences.size() + "):");
            mMissingReferences.forEach(x -> aLogger.error("\t" + x));
        }
        if (!mErrors.isEmpty()){
            aLogger.error("DIVERGENT TRANSFORMATION (" + mErrors.size() + "):");
            mErrors.forEach((x, y) -> aLogger.error("\t".concat(x).concat("\n").concat(y.toString())));
        }

    }

    public void compare(final String aId, final JsonNode aDocument, final JsonNode aReference){
        if (aReference == null){
            mMissingReferences.add(aId);
            return;
        }
        JsonNode field = aDocument.at(mField);
        JsonNode ref = aReference.at(mField);
        if (field instanceof MissingNode){
            if (!(ref instanceof MissingNode)) {
                mMissingFields.add(aId.concat(" (").concat(ref.toString()).concat(")"));
            }
            return;
        }
        if (ref instanceof MissingNode){
            mMissingInRef.add(aId.concat(" (").concat(field.toString()).concat(")"));
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
