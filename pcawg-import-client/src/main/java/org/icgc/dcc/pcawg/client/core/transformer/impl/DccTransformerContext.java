package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.ssm.classification.SSMClassification;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.pcawg.client.model.ssm.classification.impl.SSMMetadataClassification.newSSMMetadataClassification;

@Value
@RequiredArgsConstructor(staticName = "newDccTransformerContext")
public class DccTransformerContext<T> {

  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowType, MutationTypes mutationType, T object){
    return newDccTransformerContext(newSSMMetadataClassification(workflowType, mutationType ), object);
  }

  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowType, DataTypes dataType, T object){
    return newDccTransformerContext(newSSMMetadataClassification(workflowType, dataType), object);
  }

  @NonNull private final SSMClassification SSMClassification;

  @NonNull private final T object;

}
