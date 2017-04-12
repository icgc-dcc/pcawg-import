package org.icgc.dcc.pcawg.client.data.metadata;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

@Value
@Builder
public class SampleMetadata {

  public static SampleMetadataBuilder builderWith(SampleMetadata s){
    return builder()
        .aliquotId             (s.getAliquotId())
        .workflowType          (s.getWorkflowType())
        .isUsProject           (s.isUsProject())
        .analyzedSampleId      (s.getAnalyzedSampleId())
        .dccProjectCode        (s.getDccProjectCode())
        .matchedSampleId       (s.getMatchedSampleId())
        .analyzedFileId        (s.getAnalyzedFileId())
        .matchedFileId         (s.getMatchedFileId());
  }

  @NonNull private final String aliquotId;
  @NonNull private final WorkflowTypes workflowType;
  private final boolean isUsProject;
  @NonNull private final String analyzedSampleId;
  @NonNull private final String dccProjectCode;
  @NonNull private final String matchedSampleId;
  @NonNull private final String analyzedFileId;
  @NonNull private final String matchedFileId;


}
