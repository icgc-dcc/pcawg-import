package org.icgc.dcc.pcawg.client.core.model.ssm.primary;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.function.Function;

@RequiredArgsConstructor
public enum SSMPrimaryFieldMapping implements FieldExtractor<SSMPrimary> {
  ANALYSIS_ID                         ("analysis_id", SSMPrimary::getAnalysisId),
  ANALYZED_SAMPLE_ID                  ("analyzed_sample_id", SSMPrimary::getAnalyzedSampleId),
  MUTATION_TYPE                       ("mutation_type", SSMPrimary::getMutationType),
  CHROMOSOME                          ("chromosome", SSMPrimary::getChromosome),
  CHROMOSOME_START                    ("chromosome_start", SSMPrimary::getChromosomeStart),
  CHROMOSOME_END                      ("chromosome_end", SSMPrimary::getChromosomeEnd),
  CHROMOSOME_STRAND                   ("chromosome_strand", SSMPrimary::getChromosomeStrand),
  REFERENCE_GENOME_ALLELE             ("reference_genome_allele", SSMPrimary::getReferenceGenomeAllele),
  CONTROL_GENOTYPE                    ("control_genotype", SSMPrimary::getControlGenotype),
  MUTATED_FROM_ALLELE                 ("mutated_from_allele", SSMPrimary::getMutatedFromAllele),
  MUTATED_TO_ALLELE                   ("mutated_to_allele", SSMPrimary::getMutatedToAllele),
  TUMOUR_GENOTYPE                     ("tumour_genotype", SSMPrimary::getTumorGenotype),
  EXPRESSED_ALLELE                    ("expressed_allele", SSMPrimary::getExpressedAllele),
  QUALITY_SCORE                       ("quality_score", SSMPrimary::getQualityScore),
  PROBABILITY                         ("probability", SSMPrimary::getProbability),
  TOTAL_READ_COUNT                    ("total_read_count", SSMPrimary::getTotalReadCount),
  MUTANT_ALLELE_READ_COUNT            ("mutant_allele_read_count", SSMPrimary::getMutantAlleleReadCount),
  VERIFICATION_STATUS                 ("verification_status", SSMPrimary::getVerificationStatus),
  VERIFICATION_PLATFORM               ("verification_platform", SSMPrimary::getVerificationPlatform),
  BIOLOGICAL_VALIDATION_STATUS        ("biological_validation_status", SSMPrimary::getBiologicalValidationStatus),
  BIOLOGICAL_VALIDATION_PLATFORM      ("biological_validation_platform", SSMPrimary::getBiologicalValidationPlatform),
  STUDY                               ("_study", SSMPrimary::getStudy);


  @NonNull
  private final String name;

  @NonNull
  private final Function<SSMPrimary, ? super Object> functor;

  @Override
  public String toString(){
    return name;
  }

  @Override
  public String extractStringValue(SSMPrimary data){
    return functor.apply(data).toString();
  }

}
