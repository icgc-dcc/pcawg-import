package org.icgc.dcc.pcawg.client.model.ssm.metadata;

import org.icgc.dcc.pcawg.client.model.ssm.SSMCommon;

public interface SSMMetadata extends SSMCommon {

  String getMatchedSampleId();
  String getAssemblyVersion();
  String getPlatform();
  String getExperimentalProtocol();
  String getBaseCallingAlgorithm();
  String getAlignmentAlgorithm();
  String getVariationCallingAlgorithm();
  String getOtherAnalysisAlgorithm();
  String getSequencingStrategy();
  String getSeqCoverage();
  String getRawDataRepository();
  String getRawDataAccession();

}
