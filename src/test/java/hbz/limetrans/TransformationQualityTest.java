package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    final private static Integer MISSING_DOCS_ACCEPTED = 10;
    final private static Integer ERRONEOUS_DOCS_ACCEPTED_PER_FIELD = 20;
    final private static List<String> EXPECTED_FIELDS_WORKING = Arrays.asList(new String[]{
            "CreatorStatement",
            "DateProper",
            "Description",
            "Edition",
            "Extent",
            "IdentifierISBNParallel",
            "OnlineAccess",
            "Person",
            "PersonContributor",
            "PersonCreator",
            "PublicationPlace",
            "PublisherName",
            "RSWK",
            "SeriesAddedEntryUniformTitle",
            "TitleStatement",
            "TitleAddendum",
            "VolumeDesignation"});
    final private static Logger mLogger = LogManager.getLogger();
    final private static List<String> mMissingDocs = new ArrayList<>();
    final private static Map<String, Set<String>> mMissingFields = new HashMap<>();
    final private static Map<String, Set<String>> mErrorFields = new HashMap<>();
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
            checkDocument(ocm, reference, document);
            line = reader.readLine();
        }
        mMissingDocs.addAll(referenceMap.keySet());

        postProcessAndReport();
    }

    private static void checkDocument(String aId, JsonNode aReference, JsonNode aDocument) {
        Map<String, String> missing = new HashMap<>();
        Map<String, String> error = new HashMap<>();

        // iterate over all document fields
        // TODO: iterate over subfields and store in member variables as such entries
        final Iterator<Map.Entry<String, JsonNode>> fields = aDocument.fields();
        while (fields.hasNext()){
            final Map.Entry<String, JsonNode> d = fields.next();
            JsonNode r = aReference.get(d.getKey());
            if (r == null){
                missing.put(d.getKey(), d.getValue().asText());
            }
            else if (r.equals(d)) {
                mWorkingFields.add(d.getKey());
            }
            else{
                mWorkingFields.remove(d.getKey());
                mErrorKeys.add(d.getKey());
                error.put(d.getKey(), d.getValue().asText());
            }
        }
        if (!missing.isEmpty()){
            mMissingFields.put(aId, missing.keySet());
        }
        if (!error.isEmpty()){
            mErrorFields.put(aId, error.keySet());
        }
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
        if (!mWorkingFields.isEmpty()){
            mLogger.info("WORKING FIELDS IN TRANSFORMATION:");
            mWorkingFields.forEach(x -> mLogger.info("\t" + x));
        }
        if (!mMissingDocs.isEmpty()){
            mLogger.error("MISSING DOCUMENTS IN TRANSFORMATION:");
            mMissingDocs.forEach(x -> mLogger.error("\t" + x));
        }
        if (!mErrorKeys.isEmpty()){
            mLogger.error("OVERALL ERRONEOUS FIELDS:");
            mErrorKeys.forEach(x -> mLogger.error("\t" + x));
        }

        final Map<String, Set<String>> missingFieldsInverted = invert(mMissingFields);
        final Map<String, Set<String>> errorFieldsInverted = invert(mErrorFields);
        countAndAccumulateErrors(missingFieldsInverted);
        countAndAccumulateErrors(errorFieldsInverted);

        if (!mMissingFields.isEmpty()){
            mLogger.error("MISSING FIELDS IN DOCUMENTS:");
            missingFieldsInverted.forEach((x, y) -> mLogger.error("\t" + x + " (" + y.size() + "): " + y));
        }
        if (!mErrorFields.isEmpty()){
            mLogger.error("ERRONEOUS FIELDS IN DOCUMENTS:");
            errorFieldsInverted.forEach((x, y) -> mLogger.error("\t" + x + " (" + y.size() + "): " + y));
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
        Map<String, Integer> erroneousFieldNames = new HashMap<>();
        mAccumulatedErrorFields.forEach((k, v) -> {
            if (v > ERRONEOUS_DOCS_ACCEPTED_PER_FIELD &&
                    EXPECTED_FIELDS_WORKING.contains(k)){
                erroneousFieldNames.put(k, v);
            }
        });
        StringBuffer sb = new StringBuffer("Too many arrors in fields:");
        erroneousFieldNames.forEach((k, v) -> {
            sb.append("\n\t" + k + " (" + v + ")");
        });
        assertTrue(sb.toString(), erroneousFieldNames.isEmpty());
    }

}
