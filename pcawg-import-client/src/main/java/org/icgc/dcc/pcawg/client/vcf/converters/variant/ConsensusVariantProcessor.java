package org.icgc.dcc.pcawg.client.vcf.converters.variant;

import com.google.common.collect.ImmutableSet;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;
import org.icgc.dcc.pcawg.client.vcf.NotConsensusWorkflowTypeException;
import org.icgc.dcc.pcawg.client.model.types.WorkflowTypes;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategyMux;

import java.util.Set;
import java.util.stream.Stream;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.DEFAULT_STUDY;
import static org.icgc.dcc.pcawg.client.model.NACodes.CORRUPTED_DATA;
import static org.icgc.dcc.pcawg.client.model.NACodes.DATA_VERIFIED_TO_BE_UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getAltCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getChomosome;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getRefCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.streamCallers;
import static org.icgc.dcc.pcawg.client.model.types.WorkflowTypes.CONSENSUS;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.VariantProcessor.resolveMutationType;

@Slf4j
public class ConsensusVariantProcessor implements VariantProcessor {

  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;
  private static final int DEFAULT_STRAND = 1;
  private static final String DEFAULT_VERIFICATION_STATUS = "not tested";

  public static ConsensusVariantProcessor newConsensusVariantProcessor(SampleMetadata sampleMetadata,
      VariantConverterStrategyMux variantConverterStrategyMux) {
    return new ConsensusVariantProcessor(sampleMetadata, variantConverterStrategyMux);
  }

  private static void checkIsConsensus(WorkflowTypes workflowType){
    if(workflowType != CONSENSUS){
      throw new NotConsensusWorkflowTypeException(
          String.format("The workflow type [%s] is not supported, only the [%s] is supported",
              workflowType, CONSENSUS));
    }
  }

  public static Set<WorkflowTypes> extractWorkflowTypes(VariantContext  variant){
    return streamWorkflowTypes(variant)
        .collect(toImmutableSet());
  }

  private static Stream<WorkflowTypes> streamWorkflowTypes(VariantContext  variant){
    return streamCallers(variant)
        .map(c ->  WorkflowTypes.parseMatch(c, F_CHECK_CORRECT_WORKTYPE ));
  }


  private static SSMPrimary createWorkflowTypeSpecificSSMPrimary( SSMPrimary ssmPrimaryConsensus,
      WorkflowTypes workflowType){
    return PlainSSMPrimary.builderWith(ssmPrimaryConsensus)
        .workflowType(workflowType)
        .totalReadCount(DATA_VERIFIED_TO_BE_UNKNOWN.toInt())
        .mutantAlleleReadCount(DATA_VERIFIED_TO_BE_UNKNOWN.toInt())
        .build();
  }


  private static int calcTotalReadCount(VariantContext variant) {
    val altCount = getAltCount(variant);
    val refCount = getRefCount(variant);
    if (altCount.isPresent() && refCount.isPresent()){
      return altCount.get()+refCount.get();
    } else {
      return CORRUPTED_DATA.toInt();
    }
  }

  private static int calcMutantAlleleReadCount(VariantContext variant) {
    val altCount = getAltCount(variant);
    if (altCount.isPresent()){
      return altCount.get();
    } else {
      return CORRUPTED_DATA.toInt();
    }
  }

  @NonNull private final SampleMetadata sampleMetadata;
  @NonNull private final VariantConverterStrategyMux variantConverterStrategyMux;

  /**
   *
   * @throws org.icgc.dcc.pcawg.client.vcf.NotConsensusWorkflowTypeException
   * @param sampleMetadata
   */
  private ConsensusVariantProcessor(SampleMetadata sampleMetadata,
      VariantConverterStrategyMux variantConverterStrategyMux) {
    this.sampleMetadata = sampleMetadata;
    this.variantConverterStrategyMux = variantConverterStrategyMux;
    checkIsConsensus(sampleMetadata.getWorkflowType());
  }


  @Override
  public Set<SSMPrimary> convertSSMPrimary(VariantContext variant){
    val workflowType = sampleMetadata.getWorkflowType();
    val mutationType = resolveMutationType(variant);
    // Need mux because variants from the same file can be any one of the MutationTypes.
    val variantConverter = variantConverterStrategyMux.select(mutationType);
    val consensusSSMPrimary =  buildSSMPrimary(workflowType,variantConverter,variant);
    val setBuilder = ImmutableSet.<SSMPrimary>builder();
    setBuilder.add(consensusSSMPrimary);
    for (val workflow : extractWorkflowTypes(variant)){
      val ssmPrimary = createWorkflowTypeSpecificSSMPrimary(consensusSSMPrimary,workflow);
      setBuilder.add(ssmPrimary);
    }
    return setBuilder.build();
  }

  private SSMPrimary buildSSMPrimary(WorkflowTypes workflowTypes, VariantConverterStrategy<VariantContext> converter, VariantContext variant){
    val mutationType = resolveMutationType(variant);
    val dataType = VariantProcessor.resolveDataType(mutationType);
    val dccProjectCode = sampleMetadata.getDccProjectCode();
    val analyzedSampleId = sampleMetadata.getAnalyzedSampleId();

    return PlainSSMPrimary.builder()
        .analyzedSampleId(analyzedSampleId)
        .mutationType(mutationType.toString())
        .workflowType(workflowTypes)
        .dataType(dataType)
        .dccProjectCode(dccProjectCode)
        .chromosomeStart(converter.convertChromosomeStart(variant))
        .chromosomeEnd(converter.convertChromosomeEnd(variant))
        .referenceGenomeAllele(converter.convertReferenceGenomeAllele(variant))
        .controlGenotype(converter.convertControlGenotype(variant))
        .mutatedFromAllele(converter.convertMutatedFromAllele(variant))
        .tumorGenotype(converter.convertTumorGenotype(variant))
        .mutatedToAllele(converter.convertMutatedToAllele(variant))
        .chromosome(getChomosome(variant))
        .chromosomeStrand(DEFAULT_STRAND)
        .expressedAllele( DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .qualityScore( DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .probability( DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .totalReadCount(calcTotalReadCount(variant))
        .mutantAlleleReadCount(calcMutantAlleleReadCount(variant))
        .verificationStatus(DEFAULT_VERIFICATION_STATUS)
        .verificationPlatform( DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .biologicalValidationPlatform( DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .biologicalValidationStatus( DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .note(DATA_VERIFIED_TO_BE_UNKNOWN.toString())
        .study(DEFAULT_STUDY)
        .build();
  }

}
