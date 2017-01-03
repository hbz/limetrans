package transformationquality;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FieldReportTest extends AbstractTransformationTest{

    protected String mField;
    final private static Logger mLogger = LogManager.getLogger();
    private static boolean mFullLogging;
    final private static Set<String> mWorkingDocs = new HashSet<>();
    final private static Set<String> mMissingFields = new HashSet<>();
    final private static Set<String> mMissingInRef = new HashSet<>();
    final private static Set<String> mMissingReferences = new HashSet<>();
    final private static Map<String, ErrorFieldPair> mErrors = new HashMap<>();

    public FieldReportTest(String aField) {
        mField = aField;
    }

    @Parameterized.Parameters
    public static Collection primeNumbers() {
        return Arrays.asList(new String[] {
                "/CreatorStatement/creatorStatement",
                "/DateProper/date",
                "/Description/description",
                "/Edition/edition",
                "/Extent/extent",
                "/IdentifierISBN/identifierISBN",
                "/IdentifierISBNParallel/identifierISBN",
                "/Language/language",
                "/Language/languageSource",
                "/OnlineAccess/nonpublicnote",
                "/OnlineAccess/uri",
                "/PersonContributor/personBio",
                "/PersonContributor/personName",
                "/PersonContributor/personRole",
                "/PersonContributor/personTitle",
                "/PersonCreator/personBio",
                "/PersonCreator/personName",
                "/PersonCreator/personRole",
                "/PersonCreator/personTitle",
                "/Person/personBio",
                "/Person/personName",
                "/Person/personRole",
                "/Person/personTitle",
                "/PublicationPlace/printingPlace",
                "/PublisherName/name",
                "/RSWK/identifierGND",
                "/RSWK/subjectIdentifier",
                "/RSWK/subjectTopicName",
                "/SeriesAddedEntryUniformTitle/title",
                "/SeriesAddedEntryUniformTitle/volume",
                "/TitleAddendum/title",
                "/TitleStatement/titleMain",
                "/VolumeDesignation/volumeDesignation"
        });
    }

    @BeforeClass
    public static void setup() throws IOException {
        mFullLogging = !(Boolean.valueOf(System.getenv("CI")));
        mReader.mark(10000000);
    }

    @Test
    public void reportField() throws IOException, InterruptedException {
        reset();
        String line = mReader.readLine();
        while (line != null){
            JsonNode document = mMapper.readTree(line);
            String ocm = document.get("RecordIdentifier").get("identifierForTheRecord").asText().substring(8);
            JsonNode reference = mReference.get(mReferenceMap.get(ocm));
            compare(ocm, document, reference);
            line = mReader.readLine();
        }
        if (!mMissingFields.isEmpty()){
            mLogger.error(mField + ": MISSING FIELDS IN TRANSFORMED DATA (" + mMissingFields.size() + ")");
        }
        if (!mMissingInRef.isEmpty()){
            mLogger.error(mField + ": MISSING FIELDS IN REFERENCE DOCUMENT (" + mMissingInRef.size() + ")");
        }
        if (!mMissingReferences.isEmpty()){
            mLogger.error(mField + ": MISSING REFERENCE DOCUMENTS (" + mMissingReferences.size() + ")");
        }
        if (!mErrors.isEmpty()){
            mLogger.error(mField + ": DIVERGENT TRANSFORMATION (" + mErrors.size() + ")");
        }
        if (!mWorkingDocs.isEmpty()){
            mLogger.error(mField + ": WORKING DOCUMENTS (" + mWorkingDocs.size() + ")");
        }
        if (mFullLogging){
            mMissingFields.forEach(
                    x -> mLogger.error("\tMISSING FIELD IN TRANSFORMED DATA FOR ".concat(mField).concat(" : ").concat(x)));
            mMissingInRef.forEach(
                    x -> mLogger.error("\tMISSING FIELD IN REFERENCE DOCUMENT FOR ".concat(mField).concat(" : ").concat(x)));
            mMissingReferences.forEach(
                    x -> mLogger.error("\tMISSING REFERENCE DOCUMENT FOR ".concat(mField).concat(" : ").concat(x)));
            mErrors.forEach(
                    (x, y) -> mLogger.error("\tDIVERGENT TRANSFORMATION FOR ".concat(mField).concat(" : ").concat(x).concat("\n").concat(y.toString())));
            mWorkingDocs.forEach(
                    x -> mLogger.error("\tWORKING DOCUMENT FOR ".concat(mField).concat(" : ").concat(x)));
        }
        mLogger.error("\n\n");
    }

    private void reset() throws IOException {
        mMissingFields.clear();
        mMissingInRef.clear();
        mMissingReferences.clear();
        mErrors.clear();
        mWorkingDocs.clear();
        mReader.reset();
    }

    private void compare(final String aId, final JsonNode aDocument, final JsonNode aReference) {
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
