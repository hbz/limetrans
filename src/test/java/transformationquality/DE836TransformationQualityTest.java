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
    final private static Map<String, Integer> EXPECTED_MAXIMUM_ERRORS_PER_FIELD = new HashMap<>();
    static {
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("RecordIdentifier.identifierForTheRecord", 987);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Language.languageSource", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Language.language", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("IdentifierISBN.identifierISBN", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Person.personName", 927);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Person.personTitle", 41);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Person.personBio", 846);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Person.personRole", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonCreator.personName", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonCreator.personTitle", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonCreator.personBio", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonCreator.personRole", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonContributor.personName", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonContributor.personTitle", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonContributor.personBio", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PersonContributor.personRole", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("TitleStatement.titleMain", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("TitleAddendum.title", 104);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("VolumeDesignation.volumeDesignation", 18);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("CreatorStatement.creatorStatement", 963);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Edition.edition", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PublicationPlace.printingPlace", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("PublisherName.name", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("DateProper.date", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Extent.extent", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("SeriesAddedEntryUniformTitle.title", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("SeriesAddedEntryUniformTitle.volume", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("Description.description", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("RSWK.subjectTopicName", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("RSWK.subjectIdentifier", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("RSWK.identifierGND", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("OnlineAccess.uri", 0);
        EXPECTED_MAXIMUM_ERRORS_PER_FIELD.put("OnlineAccess.nonpublicnote", 0);
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

        postProcessAndReport(mLogger, EXPECTED_MAXIMUM_ERRORS_PER_FIELD);
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
