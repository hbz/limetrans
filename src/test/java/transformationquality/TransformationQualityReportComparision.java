package transformationquality;

public class TransformationQualityReportComparision {

    private TransformationQualityReportComparisionResult mResult;
    private double mResultScore;

    // TODO: implement comparision details information as String

    public TransformationQualityReportComparision(final TransformationQualityReport aNewReport,
                                                  final TransformationQualityReport aReferenceReport){
        mResult = TransformationQualityReportComparisionResult.GREEN;

        mResultScore = 0;
        // For now, all partial results are treated as of equal weight.
        mResultScore += (aNewReport.getWorkingFields().size() - aReferenceReport.getWorkingFields().size());
        mResultScore -= (aNewReport.getErrorFields().size() - aReferenceReport.getErrorFields().size());
        mResultScore -= (aNewReport.getErroneousFields().size() - aReferenceReport.getErroneousFields().size());
        mResultScore -= (aNewReport.getErrorKeys().size() - aReferenceReport.getErrorKeys().size());
        mResultScore -= (aNewReport.getAccumulatedErrorFields().size() - aReferenceReport.getAccumulatedErrorFields().size());
        mResultScore -= (aNewReport.getMissingDocs().size() - aReferenceReport.getMissingDocs().size());
        mResultScore -= (aNewReport.getMisConfiguredFields().size() - aReferenceReport.getMisConfiguredFields().size());
        mResultScore -= (aNewReport.getMissingInRefFields().size() - aReferenceReport.getMissingInRefFields().size());


        if ((aNewReport.getWorkingFields().size() < aReferenceReport.getWorkingFields().size()) ||
            (aNewReport.getErrorFields().size() > aReferenceReport.getErrorFields().size()) ||
            (aNewReport.getErroneousFields().size() > aReferenceReport.getErroneousFields().size()) ||
            (aNewReport.getErrorKeys().size() > aReferenceReport.getErrorKeys().size()) ||
            (aNewReport.getAccumulatedErrorFields().size() > aReferenceReport.getAccumulatedErrorFields().size()) ||
            (aNewReport.getMissingDocs().size() > aReferenceReport.getMissingDocs().size()) ||
            (aNewReport.getMisConfiguredFields().size() > aReferenceReport.getMisConfiguredFields().size()) ||
            (aNewReport.getMissingInRefFields().size() > aReferenceReport.getMissingInRefFields().size())){
            mResult = TransformationQualityReportComparisionResult.YELLOW;
        }

        if (mResultScore < 0.0){
            mResult = TransformationQualityReportComparisionResult.RED;
        }
    }

    /**
     * To be used as an overall indicator of the comparision result.
     * <li>{@link #GREEN}</li> indicates that all comparision details are either "unchanged" or "better" in new vs.
     * reference report.
     * <li>{@link #YELLOW}</li> indicates that some comparision details are either "unchanged" or "better" in new vs.
     * reference report, but some are "worse". The sum of all details is either "unchanged" or "better".
     * <li>{@link #RED}</li> indicates that at least some comparision details are "worse" in new vs. reference report.
     * The sum of all details is "worse".
     */
    public enum TransformationQualityReportComparisionResult{
        GREEN, YELLOW, RED;
    }

}
