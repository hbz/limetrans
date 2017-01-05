package transformationquality;

import java.util.*;

public class TransformationQualityReport {

    final private List<String> mMissingDocs;
    final private Map<String, Set<String>> mMissingInRefFields;
    final private Map<String, Set<String>> mMisConfiguredFields;
    final private Map<String, Set<String>> mErrorFields;
    final private Map<String, Integer> mErroneousFields;
    final private Set<String> mWorkingFields;
    final private Set<String> mErrorKeys;
    final private Map<String, Integer> mAccumulatedErrorFields;

    public TransformationQualityReport(final List<String> aMissingDocs,
                                       final Map<String, Set<String>> aMissingInRefFields,
                                       final Map<String, Set<String>> aMisConfiguredFields,
                                       final Map<String, Set<String>> aErrorFields,
                                       final Map<String, Integer> aErroneousFields,
                                       final Set<String> aWorkingFields,
                                       final Set<String> aErrorKeys,
                                       final Map<String, Integer> aAccumulatedErrorFields
                                       ){
        mMissingDocs = aMissingDocs;
        mMissingInRefFields = aMissingInRefFields;
        mMisConfiguredFields = aMisConfiguredFields;
        mErrorFields = aErrorFields;
        mErroneousFields = aErroneousFields;
        mWorkingFields = aWorkingFields;
        mErrorKeys = aErrorKeys;
        mAccumulatedErrorFields = aAccumulatedErrorFields;
    }

    public List<String> getMissingDocs() {
        return mMissingDocs;
    }

    public Map<String, Set<String>> getMissingInRefFields() {
        return mMissingInRefFields;
    }

    public Map<String, Set<String>> getMisConfiguredFields() {
        return mMisConfiguredFields;
    }

    public Map<String, Set<String>> getErrorFields() {
        return mErrorFields;
    }

    public Map<String, Integer> getErroneousFields() {
        return mErroneousFields;
    }

    public Set<String> getWorkingFields() {
        return mWorkingFields;
    }

    public Set<String> getErrorKeys() {
        return mErrorKeys;
    }

    public Map<String, Integer> getAccumulatedErrorFields() {
        return mAccumulatedErrorFields;
    }

}
