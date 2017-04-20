package org.icgc.dcc.pcawg.client.model.ssm.classification.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.ssm.classification.SSMClassification;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

@Value
@RequiredArgsConstructor(staticName = "newSSMPrimaryClassification")
public class SSMPrimaryClassification implements SSMClassification {

  public static SSMPrimaryClassification newSSMPrimaryClassification(WorkflowTypes workflowType, MutationTypes mutationType) {
    return newSSMPrimaryClassification(workflowType, mutationType, SSMClassification.convertToDataType(mutationType));
  }

  @NonNull private final WorkflowTypes workflowType;
  @NonNull private final MutationTypes mutationType;
  @NonNull private final DataTypes dataType;

}
