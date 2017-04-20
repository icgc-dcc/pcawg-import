package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.val;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.SSMPrimaryClassification;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.pcawg.client.vcf.SSMPrimaryClassification.newSSMPrimaryClassification;

@Builder
@Value
public class DccTransformerContext<T> {

  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowType, MutationTypes mutationType, T object){
    return newDccTransformerContext(newSSMPrimaryClassification(workflowType, mutationType), object);
  }

  @Deprecated
  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowType, DataTypes dataTypes, T object){
    val dummyClassification = new SSMPrimaryClassification(workflowType, MutationTypes.UNKNOWN, dataTypes);
    return newDccTransformerContext(dummyClassification, object);
  }

  public static <T> DccTransformerContext<T> newDccTransformerContext(SSMPrimaryClassification SSMPrimaryClassification, T object){
    return new DccTransformerContext<T>(SSMPrimaryClassification, object);
  }

  @NonNull private final SSMPrimaryClassification SSMPrimaryClassification;

  @NonNull private final T object;

}
