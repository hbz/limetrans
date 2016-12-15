package transformationquality;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
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
        if (!mWorkingDocs.isEmpty()){
            aLogger.error("WORKING DOCUMENTS (" + mWorkingDocs.size() + "):");
            mWorkingDocs.forEach(x -> aLogger.error("\t" + x));
        }

    }

    public void compare(final String aId, final JsonNode aDocument, final JsonNode aReference) {
        if (aReference == null) {
            mMissingReferences.add(aId);
            return;
        }
        final ArrayList<JsonNode> docList = new ArrayList<>();
        docList.add(aDocument);
        final ArrayList<JsonNode> refList = new ArrayList<>();
        refList.add(aReference);

        final List<JsonNode> allDocNodes = getAllSpecifiedNodes(docList, mField);
        final List<JsonNode> allRefNodes = getAllSpecifiedNodes(refList, mField);

        if (!allDocNodes.isEmpty()) {
            if (allRefNodes.isEmpty()){
                mMissingInRef.add(aId.concat(" (").concat(allDocNodes.toString()).concat(")"));
            }
            else{
                if (areEqual(allRefNodes, allDocNodes)){
                    mWorkingDocs.add(aId);
                }
                else{
                    mErrors.put(aId, new ErrorFieldPair(allRefNodes.toString(), allDocNodes.toString()));
                }
            }
        }
        else if (!allRefNodes.isEmpty()) {
            mMissingFields.add(aId.concat(" (").concat(allRefNodes.toString()).concat(")"));
        }
    }

    private List<JsonNode> getAllSpecifiedNodes(final List<JsonNode> aList, final String aField) {
        final String[] split = separateFirstLevel(aField);
        final List<JsonNode> newNodes = new ArrayList<>();
        final Iterator<JsonNode> iterator = aList.iterator();
        while (iterator.hasNext()){
            JsonNode node = iterator.next();
            final JsonNode subnode = node.get(split[0]);
            if (subnode == null){
                iterator.remove();
            }
            else if (subnode instanceof ArrayNode){
                final ArrayNode arrayNode = (ArrayNode) subnode;
                arrayNode.forEach(x -> newNodes.add(x));
                iterator.remove();
            }
            else if (subnode instanceof ObjectNode){
                newNodes.add(subnode);
                iterator.remove();
            }
            else if (subnode instanceof TextNode){
                newNodes.add(subnode);
                iterator.remove();
            }
        }
        aList.addAll(newNodes);
        if (split.length == 2) {
            getAllSpecifiedNodes(aList, split[1]);
        }
        return aList;
    }

    private String[] separateFirstLevel(String aDeepField){
        final String[] result;
        if (aDeepField.startsWith("/")){
            aDeepField = aDeepField.substring(1);
        }
        result = aDeepField.split("/", 2);
        return result;
    }

    private boolean areEqual(List<JsonNode> aList1, List<JsonNode> aList2) {
        if (aList1.size() != aList2.size()){
            return false;
        }
        final Iterator<JsonNode> iterator = aList1.iterator();
        while (iterator.hasNext()){
            JsonNode next = iterator.next();
            if (!aList2.contains(next)){
                return false;
            }
        }
        return true;
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
