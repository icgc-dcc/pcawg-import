package org.icgc.dcc.pcawg.client.model.metadata.project;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;

@Value
@Builder
public class SampleMetadata {

  @NonNull
  private final String aliquotId;

  @NonNull
  private final WorkflowTypes workflowType;

  @NonNull
  private final DataTypes dataType;

  private final boolean isUsProject;

  @NonNull
  private final String analyzedSampleId;

  @NonNull
  private final String dccProjectCode;

  @NonNull
  private final String matchedSampleId;

  public String getAnalysisId(){
    return UNDERSCORE.join(dccProjectCode, workflowType,dataType);
  }

}
