package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractTransformationTest {

    protected static JsonNode mReference;
    protected static ObjectMapper mMapper = new ObjectMapper();
    protected static BufferedReader mReader;
    protected static Map<String, Integer> mReferenceMap;

    @BeforeClass
    public static void prepare() throws IOException, InterruptedException {

        final URL url = new File("src/conf/test/transformation-quality.json").toURI().toURL();
        final LibraryMetadataTransformation limetrans = new LibraryMetadataTransformation(Helpers.getSettingsFromUrl(url));
        limetrans.transform();

        final File referenceFile = new File("src/test/resources/integration/reference/transformation-quality.json");
        mReference = mMapper.readTree(referenceFile);

        final File outputFile = new File("src/test/resources/integration/output/transformation-quality.jsonl");
        mReader = new BufferedReader(new FileReader(outputFile));
        mReferenceMap = createReferencesMap();
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
}
