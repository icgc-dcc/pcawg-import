package org.icgc.dcc.pcawg.client.model.ssm.primary.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.types.DataTypes;
import org.icgc.dcc.pcawg.client.model.types.WorkflowTypes;

@Builder
@Value
public class PlainSSMPrimary implements SSMPrimary {

  public static PlainSSMPrimaryBuilder builderWith(SSMPrimary p){
    return builder()
              .workflowType                    (p.getWorkflowType())
              .dataType                        (p.getDataType())
              .dccProjectCode                  (p.getDccProjectCode())
              .analyzedSampleId                (p.getAnalyzedSampleId())
              .mutationType                    (p.getMutationType())
              .study                       (p.getStudy())
              .chromosome                      (p.getChromosome())
              .chromosomeStart                 (p.getChromosomeStart())
              .chromosomeEnd                   (p.getChromosomeEnd())
              .chromosomeStrand                (p.getChromosomeStrand())
              .referenceGenomeAllele           (p.getReferenceGenomeAllele())
              .controlGenotype                 (p.getControlGenotype())
              .mutatedFromAllele               (p.getMutatedFromAllele())
              .tumorGenotype                   (p.getTumorGenotype())
              .mutatedToAllele                 (p.getMutatedToAllele())
              .expressedAllele                 (p.getExpressedAllele())
              .qualityScore                    (p.getQualityScore())
              .probability                     (p.getProbability())
              .totalReadCount                  (p.getTotalReadCount())
              .mutantAlleleReadCount           (p.getMutantAlleleReadCount())
              .verificationStatus              (p.getVerificationStatus())
              .verificationPlatform            (p.getVerificationPlatform())
              .biologicalValidationStatus      (p.getBiologicalValidationStatus())
              .biologicalValidationPlatform    (p.getBiologicalValidationPlatform())
              .note                            (p.getNote());
  }

 @NonNull private final String   analyzedSampleId;
 @NonNull private final String   mutationType;
 @NonNull private final String study;
 @NonNull private final String   chromosome;
          private final int      chromosomeStart;
          private final int      chromosomeEnd;
          private final int      chromosomeStrand;
 @NonNull private final String   referenceGenomeAllele;
 @NonNull private final String   controlGenotype;
 @NonNull private final String   mutatedFromAllele;
 @NonNull private final String   tumorGenotype;
 @NonNull private final String   mutatedToAllele;
 @NonNull private final String   expressedAllele;
 @NonNull private final String   qualityScore;
 @NonNull private final String   probability;
          private final int      totalReadCount;
          private final int      mutantAlleleReadCount;
 @NonNull private final String   verificationStatus;
 @NonNull private final String   verificationPlatform;
 @NonNull private final String   biologicalValidationStatus;
 @NonNull private final String   biologicalValidationPlatform;
 @NonNull private final String   note;
 @NonNull private final WorkflowTypes workflowType;
 @NonNull private final String dccProjectCode;
 @NonNull private final DataTypes dataType;

 @Override
 public String getStudy(){
   return study;
 }

}
