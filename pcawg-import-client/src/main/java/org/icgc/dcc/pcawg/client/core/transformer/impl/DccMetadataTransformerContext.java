package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;

@Value
@RequiredArgsConstructor(staticName = "newDccMetadataTransformerContext")
public class DccMetadataTransformerContext {

  @NonNull private final SSMMetadataClassification classification;
  @NonNull private final SSMMetadata ssmMetadata;

}
