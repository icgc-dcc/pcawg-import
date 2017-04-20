package org.icgc.dcc.pcawg.client.model.ssm.metadata.impl;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.model.NACodes;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Joiners.COLON;

@RequiredArgsConstructor(access = PRIVATE)
@EqualsAndHashCode
public class PcawgSSMMetadata implements SSMMetadata {
  private static final String DEFAULT_ASSEMBLY_VERSION = "GRCh37";
  private static final String DEFAULT_PLATFORM = "Illumina HiSeq";
  private static final String DEFAULT_SEQUENCING_STRATEGY = "WGS";
  private static final String TCGA = "TCGA";
  private static final String EGA = "EGA";

  public static final PcawgSSMMetadata newSSMMetadataImpl(String variationCallingAlgorithm,
      String matchedSampleId,
      String analysisId,
      String analyzedSampleId,
      boolean isUsProject,
      String aliquotId,
      String analyzedFileId,
      String matchedFileId ){
    return new PcawgSSMMetadata(variationCallingAlgorithm, matchedSampleId, analysisId, analyzedSampleId, isUsProject, aliquotId, analyzedFileId, matchedFileId);
  }

  @NonNull
  private final String variationCallingAlgorithm;

  @NonNull
  @Getter
  private final String matchedSampleId;

  @NonNull
  @Getter
  private final String analysisId;

  @NonNull
  @Getter
  private final String analyzedSampleId;

  private final boolean isUsProject;

  @NonNull
  private final String aliquotId;

  @NonNull
  @Getter
  private final String analyzedFileId;

  @NonNull
  @Getter
  private final String matchedFileId;

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
    return isUsProject ? TCGA:EGA;
  }

  /**
   * TODO: [DCC-5507] hardcoded nonUS RawDataAccession id. Once DCC-5507 is complete, will be able to create class that retreives this information. For now baked in
   */
  @Override
  public String getRawDataAccession() {
    return isUsProject ? getAnalyzedSampleId() : COLON.join(getAnalyzedFileId(),getMatchedFileId());
  }

  //For andy, just a placeholder
  @Override
  public boolean getPcawgFlag() {
    return true;
  }

}
