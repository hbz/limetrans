package transformationquality;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class DE836TransformationQualityTest extends AbstractTransformationQualityTest{

    final private static Integer MISSING_DOCS_ACCEPTED = 10;
    final private static Integer ERRONEOUS_DOCS_ACCEPTED_PER_FIELD = 20;
    final private static Map<String, Integer> EXPECTED_FIELDS_WORKING = new HashMap<>();
    static {
        EXPECTED_FIELDS_WORKING.put("RecordIdentifier.identifierForTheRecord", 987);
        EXPECTED_FIELDS_WORKING.put("Language.languageSource", 0);
        EXPECTED_FIELDS_WORKING.put("Language.language", 0);
        EXPECTED_FIELDS_WORKING.put("IdentifierISBN.identifierISBN", 0);
        EXPECTED_FIELDS_WORKING.put("Person.personName", 927);
        EXPECTED_FIELDS_WORKING.put("Person.personTitle", 41);
        EXPECTED_FIELDS_WORKING.put("Person.personBio", 846);
        EXPECTED_FIELDS_WORKING.put("Person.personRole", 0);
        EXPECTED_FIELDS_WORKING.put("PersonCreator.personName", 0);
        EXPECTED_FIELDS_WORKING.put("PersonCreator.personTitle", 0);
        EXPECTED_FIELDS_WORKING.put("PersonCreator.personBio", 0);
        EXPECTED_FIELDS_WORKING.put("PersonCreator.personRole", 0);
        EXPECTED_FIELDS_WORKING.put("PersonContributor.personName", 0);
        EXPECTED_FIELDS_WORKING.put("PersonContributor.personTitle", 0);
        EXPECTED_FIELDS_WORKING.put("PersonContributor.personBio", 0);
        EXPECTED_FIELDS_WORKING.put("PersonContributor.personRole", 0);
        EXPECTED_FIELDS_WORKING.put("TitleStatement.titleMain", 0);
        EXPECTED_FIELDS_WORKING.put("TitleAddendum.title", 104);
        EXPECTED_FIELDS_WORKING.put("VolumeDesignation.volumeDesignation", 0);
        EXPECTED_FIELDS_WORKING.put("CreatorStatement.creatorStatement", 963);
        EXPECTED_FIELDS_WORKING.put("Edition.edition", 0);
        EXPECTED_FIELDS_WORKING.put("PublicationPlace.printingPlace", 0);
        EXPECTED_FIELDS_WORKING.put("PublisherName.name", 0);
        EXPECTED_FIELDS_WORKING.put("DateProper.date", 0);
        EXPECTED_FIELDS_WORKING.put("Extent.extent", 0);
        EXPECTED_FIELDS_WORKING.put("SeriesAddedEntryUniformTitle.title", 0);
        EXPECTED_FIELDS_WORKING.put("SeriesAddedEntryUniformTitle.volume", 0);
        EXPECTED_FIELDS_WORKING.put("Description.description", 0);
        EXPECTED_FIELDS_WORKING.put("RSWK.subjectTopicName", 0);
        EXPECTED_FIELDS_WORKING.put("RSWK.subjectIdentifier", 0);
        EXPECTED_FIELDS_WORKING.put("RSWK.identifierGND", 0);
        EXPECTED_FIELDS_WORKING.put("OnlineAccess.uri", 0);
        EXPECTED_FIELDS_WORKING.put("OnlineAccess.nonpublicnote", 0);
    }
    final private static Logger mLogger = LogManager.getLogger();

    @BeforeClass
    public static void runTransformation() throws IOException, InterruptedException {
        String line = mReader.readLine();
        Set<String> existentDocs = new HashSet<>();
        while (line != null){
            JsonNode document = mMapper.readTree(line);
            String ocm = document.get("RecordIdentifier").get("identifierForTheRecord").asText().substring(8);
            existentDocs.add(ocm);
            JsonNode reference = mReference.get(mReferenceMap.get(ocm));
            checkDocument(ocm, reference, document, null);
            line = mReader.readLine();
        }
        mMissingDocs.addAll(mReferenceMap.keySet());
        mMissingDocs.removeAll(existentDocs);

        postProcessAndReport(mLogger, ERRONEOUS_DOCS_ACCEPTED_PER_FIELD, EXPECTED_FIELDS_WORKING);
    }

    @Test
    public void testMissingDocs(){
        super.testMissingDocs(MISSING_DOCS_ACCEPTED);
    }

    @Test
    public void testErroneousFields(){
        super.testErroneousFields();
    }

}
