package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.val;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.SSMClassification;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.pcawg.client.vcf.SSMClassification.newSSMClassification;

@Builder
@Value
public class DccTransformerContext<T> {

  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowType, MutationTypes mutationType, T object){
    return newDccTransformerContext(newSSMClassification(workflowType, mutationType), object);
  }

  @Deprecated
  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowType, DataTypes dataTypes, T object){
    val dummyClassification = new SSMClassification(workflowType, MutationTypes.UNKNOWN, dataTypes);
    return newDccTransformerContext(dummyClassification, object);
  }

  public static <T> DccTransformerContext<T> newDccTransformerContext(SSMClassification SSMClassification, T object){
    return new DccTransformerContext<T>(SSMClassification, object);
  }

  @NonNull private final SSMClassification SSMClassification;

  @NonNull private final T object;

}
