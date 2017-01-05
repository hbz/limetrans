package transformationquality;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TransformationQualityReportHandler {

    final private static ObjectMapper mMapper = new ObjectMapper();

    public static TransformationQualityReportComparision compare(final TransformationQualityReport aNewReport,
                        final TransformationQualityReport aReferenceReport){
        return new TransformationQualityReportComparision(aNewReport, aReferenceReport);
    }

    public static TransformationQualityReport getNewestReport(final String aDirectory) throws IOException {
        Optional<Path> lastFilePath = Files.list(Paths.get(aDirectory))
                .filter(f -> Files.isDirectory(f) == false)
                .max((f1, f2) -> (int)(f1.toFile().lastModified() - f2.toFile().lastModified()));

        if (lastFilePath.isPresent()){
            return fromJsonFile(lastFilePath.get().toFile());
        }
        return null;
    }

    public static TransformationQualityReport fromJsonFile(final File aPath) throws IOException {
        return mMapper.readValue(aPath, TransformationQualityReport.class);
    }

    public static void toJsonFile(final String aPath, final TransformationQualityReport aReport) throws IOException {
        mMapper.writeValue(new File(aPath), aReport);
    }
}
