package org.icgc.dcc.pcawg.client.core.model.ssm;

import org.icgc.dcc.pcawg.client.core.types.DataTypes;
import org.icgc.dcc.pcawg.client.core.types.WorkflowTypes;

import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;

public interface SSMCommon {

  String getAnalyzedSampleId();
  WorkflowTypes getWorkflowType();
  DataTypes getDataType();
  String getDccProjectCode();
  String getStudy();

  default String getString(){
    return String.format("[AnalysisId=%s, AnalyzedSampleId=%s]", getAnalysisId(), getAnalyzedSampleId());
  }

  default String getAnalysisId(){
    return UNDERSCORE.join(getDccProjectCode(), getWorkflowType(), getDataType());
  }

}
