package org.icgc.dcc.pcawg.client.data.metadata;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.types.WorkflowTypes;

import java.io.Serializable;

import static org.icgc.dcc.pcawg.client.model.types.WorkflowTypes.CONSENSUS;

@Value
@Builder
public class ConsensusSampleMetadata implements Serializable, SampleMetadata {

  public static ConsensusSampleMetadata.ConsensusSampleMetadataBuilder builderWith(ConsensusSampleMetadata s){
    return ConsensusSampleMetadata.builder()
        .aliquotId             (s.getAliquotId())
        .isUsProject           (s.isUsProject())
        .analyzedSampleId      (s.getAnalyzedSampleId())
        .dccProjectCode        (s.getDccProjectCode())
        .matchedSampleId       (s.getMatchedSampleId())
        .analyzedFileId        (s.getAnalyzedFileId())
        .matchedFileId         (s.getMatchedFileId());
  }

  @NonNull private final String aliquotId;
  private final boolean isUsProject;
  @NonNull private final String analyzedSampleId;
  @NonNull private final String dccProjectCode;
  @NonNull private final String matchedSampleId;
  @NonNull private final String analyzedFileId;
  @NonNull private final String matchedFileId;

  @Override public WorkflowTypes getWorkflowType() {
    return CONSENSUS;
  }
}
