package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.junit.Assert.assertTrue;

// TODO: make re-usable: amend to DE836TransformationQualityTest extends TransformationQualityTest

public class TransformationQualityTest{

    final private static Class[] mRelevantJsonClasses = new Class[]{ArrayNode.class, ObjectNode.class, TextNode.class};
    final private static Integer MISSING_DOCS_ACCEPTED = 10;
    final private static Integer ERRONEOUS_DOCS_ACCEPTED_PER_FIELD = 20;
    final private static List<String> EXPECTED_FIELDS_WORKING = Arrays.asList(new String[]{
            "RecordIdentifier.identifierForTheRecord",
            "Language.languageSource",
            "Language.language",
            "IdentifierISBN.identifierISBN",
            "Person.personName",
            "Person.personTitle",
            "Person.personBio",
            "Person.personRole",
            "PersonCreator.personName",
            "PersonCreator.personTitle",
            "PersonCreator.personBio",
            "PersonCreator.personRole",
            "PersonContributor.personName",
            "PersonContributor.personTitle",
            "PersonContributor.personBio",
            "PersonContributor.personRole",
            "TitleStatement.titleMain",
            "TitleAddendum.title",
            "VolumeDesignation.volumeDesignation",
            "CreatorStatement.creatorStatement",
            "Edition.edition",
            "PublicationPlace.printingPlace",
            "PublisherName.name",
            "DateProper.date",
            "Extent.extent",
            "SeriesAddedEntryUniformTitle.title",
            "SeriesAddedEntryUniformTitle.volume",
            "Description.description",
            "RSWK.subjectTopicName",
            "RSWK.subjectIdentifier",
            "RSWK.identifierGND",
            "OnlineAccess.uri",
            "OnlineAccess.nonpublicnote"});
    final private static Logger mLogger = LogManager.getLogger();
    final private static List<String> mMissingDocs = new ArrayList<>();
    final private static Map<String, Set<String>> mMissingInRefFields = new HashMap<>();
    final private static Map<String, Set<String>> mMisConfiguredFields = new HashMap<>();
    final private static Map<String, Set<String>> mErrorFields = new HashMap<>();
    final private static Map<String, Integer> mErroneousFields = new HashMap<>();
    final private static Set<String> mWorkingFields = new HashSet<>();
    final private static Set<String> mErrorKeys = new HashSet<>();
    final private static Map<String, Integer> mAccumulatedErrorFields = new HashMap<>();

    private static JsonNode mReference;
    private static ObjectMapper mMapper = new ObjectMapper();

    @BeforeClass
    public static void runTransformation() throws IOException, InterruptedException {

        final URL url = new File("src/conf/test/transformation-quality.json").toURI().toURL();
        final LibraryMetadataTransformation limetrans = new LibraryMetadataTransformation(Helpers.getSettingsFromUrl(url));
        limetrans.transform();

        final File referenceFile = new File("src/test/resources/integration/reference/transformation-quality.json");
        mReference = mMapper.readTree(referenceFile);

        final File outputFile = new File("src/test/resources/integration/output/transformation-quality.jsonl");
        BufferedReader reader = new BufferedReader(new FileReader(outputFile));
        Map<String, Integer> referenceMap = createReferencesMap();

        String line = reader.readLine();
        while (line != null){
            JsonNode document = mMapper.readTree(line);
            String ocm = document.get("RecordIdentifier").get("identifierForTheRecord").asText().substring(8);
            JsonNode reference = mReference.get(referenceMap.remove(ocm));
            checkDocument(ocm, reference, document, null);
            line = reader.readLine();
        }
        mMissingDocs.addAll(referenceMap.keySet());

        postProcessAndReport();
    }

    private static void checkDocument(String aId, JsonNode aReference, JsonNode aDocument, String aParentNode) {
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

    private static Map<String,Integer> createReferencesMap() {
        final Map<String, Integer> result = new HashMap<>();
        final Iterator<JsonNode> elements = mReference.elements();
        for (int i=0; elements.hasNext(); i++){
            JsonNode doc = elements.next();
            result.put(doc.get("Identifier").get("identifierGeneric").asText(), i);
        }
        return result;
    }

    public static void postProcessAndReport(){
        if (!mMissingDocs.isEmpty()){
            mLogger.error("MISSING DOCUMENTS IN TRANSFORMATION:");
            mMissingDocs.forEach(x -> mLogger.error("\t" + x));
        }
        if (!mErrorKeys.isEmpty()){
            mLogger.error("OVERALL ERRONEOUS FIELDS:");
            mErrorKeys.forEach(x -> mLogger.error("\t" + x));
        }

        final Map<String, Set<String>> missingInRefFieldsInverted = invert(mMissingInRefFields);
        final Map<String, Set<String>> errorFieldsInverted = invert(mErrorFields);
        countAndAccumulateErrors(missingInRefFieldsInverted);
        countAndAccumulateErrors(errorFieldsInverted);

        if (!missingInRefFieldsInverted.isEmpty()){
            mLogger.error("MISSING FIELDS IN REFERENCE:");
            missingInRefFieldsInverted.forEach((x, y) -> mLogger.error("\t" + x + " (" + y.size() + "): " + y));
        }
        if (!errorFieldsInverted.isEmpty()){
            mLogger.error("ERRONEOUS FIELDS IN DOCUMENTS:");
            errorFieldsInverted.forEach((x, y) -> mLogger.error("\t" + x + " (" + y.size() + "): " + y));
        }
        mAccumulatedErrorFields.forEach((k, v) -> {
            if (v > ERRONEOUS_DOCS_ACCEPTED_PER_FIELD &&
                    EXPECTED_FIELDS_WORKING.contains(k)){
                mErroneousFields.put(k, v);
            }
            else{
                mWorkingFields.add(k);
            }
        });
        if (!mWorkingFields.isEmpty()){
            mLogger.error("WORKING FIELDS IN TRANSFORMATION:");
            mWorkingFields.forEach(x -> mLogger.error("\t" + x));
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

    @Test
    public void testMissingDocs(){
        assertTrue("Too many documents missing in transformation.", mMissingDocs.size() <= MISSING_DOCS_ACCEPTED);
    }

    @Test
    public void testErroneousFields(){
        StringBuffer sb = new StringBuffer("Too many errors in fields:");
        mErroneousFields.forEach((k, v) -> {
            sb.append("\n\t" + k + " (" + v + ")");
        });
        assertTrue(sb.toString(), mErroneousFields.isEmpty());
    }

}
