package transformationquality;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class DE836TransformationQualityTest extends AbstractTransformationQualityTest{

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
