package org.icgc.dcc.pcawg.client.model.ssm;

import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;

public interface SSMCommon {

  String getAnalyzedSampleId();
  WorkflowTypes getWorkflowType();
  DataTypes getDataType();
  String getDccProjectCode();
  boolean getPcawgFlag();

  default String getString(){
    return String.format("[AnalysisId=%s, AnalyzedSampleId=%s]", getAnalysisId(), getAnalyzedSampleId());
  }

  default String getAnalysisId(){
    return UNDERSCORE.join(getDccProjectCode(), getWorkflowType(), getDataType());
  }

}
