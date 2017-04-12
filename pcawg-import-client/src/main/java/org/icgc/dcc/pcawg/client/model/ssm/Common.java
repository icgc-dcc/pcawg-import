package org.icgc.dcc.pcawg.client.model.ssm;

public interface Common  {

  String getAnalysisId();
  String getAnalyzedSampleId();
  boolean getPcawgFlag();

  default String getString(){
    return String.format("[AnalysisId=%s, AnalyzedSampleId=%s]", getAnalysisId(), getAnalyzedSampleId());
  }

}
