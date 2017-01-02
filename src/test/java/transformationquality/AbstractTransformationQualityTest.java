package transformationquality;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class AbstractTransformationQualityTest extends AbstractTransformationTest{

    final protected static Class[] mRelevantJsonClasses = new Class[]{ArrayNode.class, ObjectNode.class, TextNode.class};
    final protected static List<String> mMissingDocs = new ArrayList<>();
    final protected static Map<String, Set<String>> mMissingInRefFields = new HashMap<>();
    final protected static Map<String, Set<String>> mMisConfiguredFields = new HashMap<>();
    final protected static Map<String, Set<String>> mErrorFields = new HashMap<>();
    final protected static Map<String, Integer> mErroneousFields = new HashMap<>();
    final protected static Set<String> mWorkingFields = new HashSet<>();
    final protected static Set<String> mErrorKeys = new HashSet<>();
    final protected static Map<String, Integer> mAccumulatedErrorFields = new HashMap<>();

    protected static void checkDocument(String aId, JsonNode aReference, JsonNode aDocument, String aParentNode) {
        Set<String> missingInRef = new HashSet<>();
        Set<String> misconfigured = new HashSet<>();
        Set<String> error = new HashSet<>();

        final Iterator<Map.Entry<String, JsonNode>> fields = aDocument.fields();
        while (fields.hasNext()){
            final Map.Entry<String, JsonNode> field = fields.next();
            JsonNode ref = aReference.get(field.getKey());

            String qualifiedFieldName = (aParentNode == null ? field.getKey() : aParentNode.concat(".").concat(field.getKey()));

            if (field == null){
                continue;
            }
            if (ref == null){
                missingInRef.add(qualifiedFieldName);
                continue;
            }
            if (!areSameInstanceOf(ref, field.getValue())){
                misconfigured.add(qualifiedFieldName);
                continue;
            }

            if ((field.getValue() instanceof TextNode)){
                if (ref.equals(field)) {
                    mWorkingFields.add(qualifiedFieldName);
                }
                else{
                    mWorkingFields.remove(qualifiedFieldName);
                    mErrorKeys.add(qualifiedFieldName);
                    error.add(qualifiedFieldName);
                }
            }
            else if (field.getValue() instanceof ObjectNode){
                checkDocument(aId, ref, field.getValue(), qualifiedFieldName);
            }
        }

        for (String s : missingInRef){
            addValue(aId, s, mMissingInRefFields);
        }
        for (String s : misconfigured){
            addValue(aId, s, mMisConfiguredFields);
        }
        for (String s : error){
            addValue(aId, s, mErrorFields);
        }
    }

    private static void addValue(final String aKey, final String aValue, final Map<String, Set<String>> aMap){
        Set<String> set;
        if (aMap.containsKey(aKey)){
            set = aMap.get(aKey);
        }
        else{
            set = new HashSet<>();
        }
        set.add(aValue);
        aMap.put(aKey, set);
    }

    private static boolean areSameInstanceOf(JsonNode aJsonNode1, JsonNode aJsonNode2) {
        for (Class clazz : mRelevantJsonClasses){
            if (clazz.isInstance(aJsonNode1)){
                if (clazz.isInstance(aJsonNode2)){
                    return true;
                }
                return false;
            }
        }
        return false;
    }



    protected static void postProcessAndReport(final Logger aLogger, final Integer aErroneousDocsAcceptedPerField,
                                               final Map<String, Integer> aExpectedFieldsWorking){
        if (!mMissingDocs.isEmpty()){
            aLogger.error("MISSING DOCUMENTS IN TRANSFORMATION:");
            mMissingDocs.forEach(x -> aLogger.error("\t" + x));
        }
        if (!mErrorKeys.isEmpty()){
            aLogger.error("OVERALL ERRONEOUS FIELDS:");
            mErrorKeys.forEach(x -> aLogger.error("\t" + x));
        }

        final Map<String, Set<String>> missingInRefFieldsInverted = invert(mMissingInRefFields);
        final Map<String, Set<String>> errorFieldsInverted = invert(mErrorFields);
        countAndAccumulateErrors(missingInRefFieldsInverted);
        countAndAccumulateErrors(errorFieldsInverted);

        if (!missingInRefFieldsInverted.isEmpty()){
            aLogger.error("MISSING FIELDS IN REFERENCE:");
            missingInRefFieldsInverted.forEach((x, y) -> aLogger.error("\t" + x + " (" + y.size() + "): " + y));
        }
        if (!errorFieldsInverted.isEmpty()){
            aLogger.error("ERRONEOUS FIELDS IN DOCUMENTS:");
            errorFieldsInverted.forEach((x, y) -> aLogger.error("\t" + x + " (" + y.size() + "): " + y));
        }
        mAccumulatedErrorFields.forEach((k, v) -> {
            if (v > aErroneousDocsAcceptedPerField &&
                    aExpectedFieldsWorking.get(k) != null &&
                    v > aExpectedFieldsWorking.get(k)){
                mErroneousFields.put(k, v);
            }
            else{
                mWorkingFields.add(k);
            }
        });
        if (!mWorkingFields.isEmpty()){
            aLogger.error("WORKING FIELDS IN TRANSFORMATION:");
            mWorkingFields.forEach(x -> aLogger.error("\t" + x));
        }
    }

    private static void countAndAccumulateErrors(final Map<String, Set<String>> aErrorMap) {
        for (Map.Entry<String, Set<String>> entry : aErrorMap.entrySet()){
            if (mAccumulatedErrorFields.get(entry.getKey()) == null){
                mAccumulatedErrorFields.put(entry.getKey(), entry.getValue().size());
            }
            else{
                int count = mAccumulatedErrorFields.get(entry.getKey());
                count += entry.getValue().size();
                mAccumulatedErrorFields.put(entry.getKey(), count);
            }
        }
    }

    private static Map<String, Set<String>> invert(Map<String, Set<String>> aMap){
        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : aMap.entrySet()){
            for (String s : entry.getValue()){
                if (result.get(s) == null){
                    result.put(s, new HashSet<>());
                }
                Set<String> invVal = result.get(s);
                invVal.add(entry.getKey());
            }
        }
        return result;
    }


    protected void testMissingDocs(final Integer aMissingDocsAccepted){
        assertTrue("Too many documents missing in transformation.", mMissingDocs.size() <= aMissingDocsAccepted);
    }

    protected void testErroneousFields(){
        StringBuffer sb = new StringBuffer("Too many errors in fields:");
        mErroneousFields.forEach((k, v) -> {
            sb.append("\n\t" + k + " (" + v + ")");
        });
        assertTrue(sb.toString(), mErroneousFields.isEmpty());
    }

}
