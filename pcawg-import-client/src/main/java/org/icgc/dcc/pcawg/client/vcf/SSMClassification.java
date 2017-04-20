package org.icgc.dcc.pcawg.client.vcf;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.DELETION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.INSERTION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.SINGLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.UNKNOWN;

@Value
@RequiredArgsConstructor
public class SSMClassification {

  //Forces use of converting MutationType to DataType
  public static SSMClassification newSSMClassification(WorkflowTypes workflowType, MutationTypes mutationType) {
    return new SSMClassification(workflowType, mutationType, convertToDataType(mutationType));
  }

  public static SSMClassification newCustomSSMClassification(WorkflowTypes workflowType, MutationTypes mutationType, DataTypes dataType) {
    return new SSMClassification(workflowType, mutationType, dataType);
  }

  @NonNull private final WorkflowTypes workflowType;
  @NonNull private final MutationTypes mutationType;
  @NonNull private final DataTypes dataType;

  public static DataTypes convertToDataType(MutationTypes mutationType){
    if (mutationType == SINGLE_BASE_SUBSTITUTION || mutationType == MULTIPLE_BASE_SUBSTITUTION){
      return SNV_MNV;
    } else if (mutationType == DELETION_LTE_200BP || mutationType == INSERTION_LTE_200BP){
      return INDEL;
    } else if (mutationType == UNKNOWN) {
      return DataTypes.UNKNOWN;
    } else {
      throw new DataTypeConversionException(String.format("No implementation defined for converting the MutationType [%s] to a DataType", mutationType.name()));
    }
  }

}
