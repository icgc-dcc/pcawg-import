package org.icgc.dcc.pcawg.client.model.ssm.metadata;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.model.ssm.primary.FieldExtractor;

import java.util.function.Function;

@RequiredArgsConstructor
public enum SSMMetadataFieldMapping implements FieldExtractor<SSMMetadata> {

  ANALYSIS_ID                  ("analysis_id", SSMMetadata::getAnalysisId),
  ANALYZED_SAMPLE_ID           ("analyzed_sample_id", SSMMetadata::getAnalyzedSampleId),
  MATCHED_SAMPLE_ID            ("matched_sample_id", SSMMetadata::getMatchedSampleId),
  ASSEMBLY_VERSION             ("assembly_version", SSMMetadata::getAssemblyVersion),
  PLATFORM                     ("platform", SSMMetadata::getPlatform),
  EXPERIMENTAL_PROTOCOL        ("experimental_protocol", SSMMetadata::getExperimentalProtocol),
  BASE_CALLING_ALGORITHM       ("base_calling_algorithm", SSMMetadata::getBaseCallingAlgorithm),
  ALIGNMENT_ALGORITHM          ("alignment_algorithm", SSMMetadata::getAlignmentAlgorithm),
  VARIATION_CALLING_ALGORITHM  ("variation_calling_algorithm", SSMMetadata::getVariationCallingAlgorithm),
  OTHER_ANALYSIS_ALGORITHM     ("other_analysis_algorithm", SSMMetadata::getOtherAnalysisAlgorithm),
  SEQUENCING_STRATEGY          ("sequencing_strategy", SSMMetadata::getSequencingStrategy),
  SEQ_COVERAGE                 ("seq_coverage", SSMMetadata::getSeqCoverage),
  RAW_DATA_REPOSITORY          ("raw_data_repository", SSMMetadata::getRawDataRepository),
  RAW_DATA_ACCESSION           ("raw_data_accession", SSMMetadata::getRawDataAccession),
  STUDY                        ("_study", SSMMetadata::getStudy);

  @NonNull
  private final String name;

  @NonNull
  private final Function<SSMMetadata, ?> functor;

  @Override
  public String toString(){
    return name;
  }

  public String extractStringValue(SSMMetadata data){
    return functor.apply(data).toString();
  }

}
