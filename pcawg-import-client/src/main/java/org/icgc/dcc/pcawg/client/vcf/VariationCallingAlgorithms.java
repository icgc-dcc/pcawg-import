package org.icgc.dcc.pcawg.client.vcf;

import lombok.NonNull;
import lombok.val;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newEnumMap;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.BROAD;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.DKFZ_EMBL;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.MUSE;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.SANGER;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.SMUFIN;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.UNKNOWN;

/**
 * Rules for resolving the variantion_calling_algorithm text for a given workflowType and dataType
 */
public enum VariationCallingAlgorithms {

  CONSENSUS_PCAWG_SNV_MNV("PCAWG Consensus SNV-MNV caller", CONSENSUS, set(SNV_MNV)),
  CONSENSUS_PCAWG_INDEL("PCAWG Consensus INDEL caller", CONSENSUS, set(INDEL)),
  SANGER_PIPELINE("Sanger variant call pipeline", SANGER, set(SNV_MNV, INDEL)),
  DKFZ_EMBL_PIPELINE("DKFS/EMBL variant call pipeline", DKFZ_EMBL, set(SNV_MNV, INDEL)),
  MUSE_PIPELINE("Muse variant call pipeline", MUSE, set(SNV_MNV, INDEL)),
  BROAD_PIPELINE("Broad variant call pipeline", BROAD, set(SNV_MNV, INDEL)),
  SMUFIN_PIPELINE("SMUFIN variant call pipeline", SMUFIN, set(SNV_MNV, INDEL)),
  UNKNOWN_PIPELINE("Unknown variant call pipeline", UNKNOWN, set(SNV_MNV, INDEL));

  private static final String CLASS_NAME = VariationCallingAlgorithms.class.getName();

  private static Map<WorkflowTypes, Map<DataTypes, String>> workflowMap;

  private static Set<DataTypes> set(DataTypes ... dataTypes){
    return stream(dataTypes).collect(toImmutableSet());
  }

  private static void add(WorkflowTypes workflowType, DataTypes dataType, String text){
    // Lazy initialization
    if (workflowMap == null){
      workflowMap = newEnumMap(WorkflowTypes.class);
    }
    if (!workflowMap.containsKey(workflowType)){
      workflowMap.put(workflowType, newEnumMap(DataTypes.class));
    }
    val dataTypeMap = workflowMap.get(workflowType);
    checkArgument(!dataTypeMap.containsKey(dataType),
        "The dataType [%s] is already defined for the workflowType [%s] in %s", dataType,workflowType, CLASS_NAME);
    dataTypeMap.put(dataType, text);
  }

  public static String get(WorkflowTypes workflowType, DataTypes dataType){
    checkArgument(workflowMap.containsKey(workflowType),
        "The workflowType [%s] is not defined in %s",workflowType, CLASS_NAME);
    val dataTypeMap = workflowMap.get(workflowType);
    checkArgument(dataTypeMap.containsKey(dataType),
        "The dataType [%s] is not defined for workflowType [%s] in %s", dataType, workflowType, CLASS_NAME);
    return dataTypeMap.get(dataType);
  }

  VariationCallingAlgorithms(@NonNull String text, final WorkflowTypes workflowType, @NonNull Set<DataTypes> dataTypes) {
    dataTypes.forEach(x -> add(workflowType, x, text));
  }

}
