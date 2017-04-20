package org.icgc.dcc.pcawg.client.model.ssm.classification.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.ssm.classification.SSMClassification;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.pcawg.client.model.ssm.classification.SSMClassification.convertToDataType;

@RequiredArgsConstructor(staticName = "newSSMMetadataClassification")
@Value
public class SSMMetadataClassification implements SSMClassification {

  public static SSMMetadataClassification newSSMMetadataClassification(WorkflowTypes workflowType, MutationTypes mutationType){
    return newSSMMetadataClassification(workflowType, convertToDataType(mutationType));
  }

  @NonNull private final WorkflowTypes workflowType;
  @NonNull private final DataTypes dataType;

}
