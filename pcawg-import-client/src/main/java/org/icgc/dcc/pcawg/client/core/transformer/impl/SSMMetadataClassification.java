package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

@Value
@RequiredArgsConstructor(staticName = "newSSMMetadataClassification")
public class SSMMetadataClassification {

  @NonNull private final WorkflowTypes workflowType;
  @NonNull private final DataTypes dataType;

}
