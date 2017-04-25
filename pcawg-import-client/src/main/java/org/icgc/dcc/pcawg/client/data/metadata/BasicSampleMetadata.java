package org.icgc.dcc.pcawg.client.data.metadata;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.core.types.WorkflowTypes;

import java.io.Serializable;

@Value
@Builder
public class BasicSampleMetadata implements SampleMetadata, Serializable{

  @NonNull private final String aliquotId;
  @NonNull private final WorkflowTypes workflowType;
  private final boolean isUsProject;
  @NonNull private final String analyzedSampleId;
  @NonNull private final String dccProjectCode;
  @NonNull private final String matchedSampleId;
  @NonNull private final String analyzedFileId;
  @NonNull private final String matchedFileId;

  public static BasicSampleMetadata.BasicSampleMetadataBuilder builderWith(BasicSampleMetadata s){
    return BasicSampleMetadata.builder()
        .aliquotId             (s.getAliquotId())
        .workflowType          (s.getWorkflowType())
        .isUsProject           (s.isUsProject())
        .analyzedSampleId      (s.getAnalyzedSampleId())
        .dccProjectCode        (s.getDccProjectCode())
        .matchedSampleId       (s.getMatchedSampleId())
        .analyzedFileId        (s.getAnalyzedFileId())
        .matchedFileId         (s.getMatchedFileId());
  }


}
