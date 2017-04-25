package org.icgc.dcc.pcawg.client.model.ssm.metadata.impl;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.model.NACodes;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Joiners.COLON;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.DEFAULT_STUDY;

@RequiredArgsConstructor(access = PRIVATE)
@EqualsAndHashCode
public class PcawgSSMMetadata implements SSMMetadata {
  private static final String DEFAULT_ASSEMBLY_VERSION = "GRCh37";
  private static final String DEFAULT_PLATFORM = "Illumina HiSeq";
  private static final String DEFAULT_SEQUENCING_STRATEGY = "WGS";
  private static final String TCGA = "TCGA";
  private static final String EGA = "EGA";

  @NonNull private final String variationCallingAlgorithm;
  @NonNull @Getter private final String matchedSampleId;
  @NonNull @Getter private final String analyzedSampleId;
  private final boolean isUsProject;
  @NonNull private final String aliquotId;
  @NonNull @Getter private final String analyzedFileId;
  @NonNull @Getter private final String matchedFileId;
  @NonNull @Getter private final String dccProjectCode;
  @NonNull @Getter private final WorkflowTypes workflowType;
  @NonNull @Getter private final DataTypes dataType;

  public static PcawgSSMMetadata newPcawgSSMMetadata(String variationCallingAlgorithm, String matchedSampleId,
      String analyzedSampleId,
      boolean isUsProject, String aliquotId, String analyzedFileId, String matchedFileId, String dccProjectCode,
      WorkflowTypes workflowType, DataTypes dataType) {
    return new PcawgSSMMetadata(variationCallingAlgorithm, matchedSampleId, analyzedSampleId, isUsProject, aliquotId,
        analyzedFileId, matchedFileId, dccProjectCode, workflowType, dataType);
  }

  @Override
  public String getAssemblyVersion() {
    return DEFAULT_ASSEMBLY_VERSION;
  }

  @Override
  public String getPlatform() {
    return DEFAULT_PLATFORM;
  }

  @Override
  public String getExperimentalProtocol() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getBaseCallingAlgorithm() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getAlignmentAlgorithm(){
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getVariationCallingAlgorithm() {
    return variationCallingAlgorithm;
  }

  @Override
  public String getOtherAnalysisAlgorithm() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getSequencingStrategy() {
    return DEFAULT_SEQUENCING_STRATEGY;
  }

  @Override
  public String getSeqCoverage() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getRawDataRepository() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getRawDataAccession() {
    return COLON.join(getAnalyzedFileId(),getMatchedFileId());
  }

  //For andy, just a placeholder
  @Override
  public String getStudy() {
    return DEFAULT_STUDY;
  }

}
