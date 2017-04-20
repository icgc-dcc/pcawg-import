package org.icgc.dcc.pcawg.client.model.ssm.classification;

import org.icgc.dcc.pcawg.client.vcf.DataTypeConversionException;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.DELETION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.INSERTION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.SINGLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.UNKNOWN;

public interface SSMClassification {

  static DataTypes convertToDataType(MutationTypes mutationType){
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

  WorkflowTypes getWorkflowType();

  DataTypes getDataType();

}
