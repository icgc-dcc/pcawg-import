package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;
import org.icgc.dcc.pcawg.client.vcf.converters.ChromosomeEndConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.ChromosomeStartConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.ControlGenotypeConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.MutatedFromAlleleConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.MutatedToAlleleConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.ReferenceGenomeAlleleConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.TumorGenotypeConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters2.VariantConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters2.VariantConverterStrategyMux;

import java.util.List;
import java.util.Set;

import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.model.NACodes.CORRUPTED_DATA;
import static org.icgc.dcc.pcawg.client.model.NACodes.DATA_VERIFIED_TO_BE_UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.DELETION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.INSERTION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.SINGLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.resolveMutationType;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getAltCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getChomosome;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getRefCount;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@RequiredArgsConstructor
public class ConsensusVariantConverter  {

  private static final ChromosomeEndConverterStrategy CHROMOSOME_END_CONVERTER_STRATEGY = new ChromosomeEndConverterStrategy();
  private static final ChromosomeStartConverterStrategy CHROMOSOME_START_CONVERTER_STRATEGY = new ChromosomeStartConverterStrategy();
  private static final ReferenceGenomeAlleleConverterStrategy REFERENCE_GENOME_ALLELE_CONVERTER_STRATEGY = new ReferenceGenomeAlleleConverterStrategy();
  private static final ControlGenotypeConverterStrategy CONTROL_GENOTYPE_CONVERTER_STRATEGY = new ControlGenotypeConverterStrategy();
  private static final MutatedFromAlleleConverterStrategy MUTATED_FROM_ALLELE_CONVERTER_STRATEGY = new MutatedFromAlleleConverterStrategy();
  private static final MutatedToAlleleConverterStrategy MUTATED_TO_ALLELE_CONVERTER_STRATEGY = new MutatedToAlleleConverterStrategy();
  private static final TumorGenotypeConverterStrategy TUMOR_GENOTYPE_CONVERTER_STRATEGY = new TumorGenotypeConverterStrategy();
  private static final VariantConverterStrategyMux VARIANT_CONVERTER_STRATEGY_MUX = new VariantConverterStrategyMux();

  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;
  private static final int DEFAULT_STRAND = 1;
  private static final String DEFAULT_VERIFICATION_STATUS = "not tested";

  public static class DataTypeConversionException extends RuntimeException{

    public DataTypeConversionException(String message) {
      super(message);
    }
  }

  public static DataTypes convertToDataType(MutationTypes mutationType){
    if (mutationType == SINGLE_BASE_SUBSTITUTION || mutationType == MULTIPLE_BASE_SUBSTITUTION){
      return SNV_MNV;
    } else if (mutationType == DELETION_LTE_200BP || mutationType == INSERTION_LTE_200BP){
      return INDEL;
    } else if (mutationType == UNKNOWN) {
      return DataTypes.UNKNOWN;
    } else {
      throw new DataTypeConversionException(String.format("No implementation defined for converting the mutationtype [%s] to a DataType", mutationType.name()));
    }
  }
  @NonNull private final SampleMetadata sampleMetadataConsensus;

  private static Set<WorkflowTypes> extractWorkflowTypes(VariantContext  variant){
    return VCF.streamCallers(variant)
        .map(c ->  WorkflowTypes.parseMatch(c, F_CHECK_CORRECT_WORKTYPE ))
        .collect(toImmutableSet());
  }

  private static SSMPrimary createCallerSpecificSSMPrimary(SampleMetadata sampleMetadataConsensus,
      SSMPrimary ssmPrimaryConsensus, WorkflowTypes workflowType, DataTypes dataType){
    val analysisId = calcAnalysisId(sampleMetadataConsensus.getDccProjectCode(),workflowType,dataType);
    return PlainSSMPrimary.builderWith(ssmPrimaryConsensus)
        .analysisId(analysisId)
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

  private static void addSSMPrimary(List<DccTransformerContext<SSMPrimary>> ssmPrimaryList, WorkflowTypes workflowType, SSMPrimary ssmPrimary, DataTypes dataType){
    ssmPrimaryList.add(
        DccTransformerContext.<SSMPrimary>builder()
            .object(ssmPrimary)
            .workflowTypes(workflowType)
            .dataType(dataType)
            .build());
  }

  public String getConsensusAnalysisId(DataTypes dataType){
    return calcAnalysisId(sampleMetadataConsensus.getDccProjectCode(), sampleMetadataConsensus.getWorkflowType(), dataType);
  }
  public static String calcAnalysisId(String dccProjectCode, WorkflowTypes workflowType,  DataTypes dataType){
    return UNDERSCORE.join(dccProjectCode, workflowType,dataType);
  }

  private SSMPrimary buildConsensusSSMPrimary(MutationTypes mutationType, VariantConverterStrategy<VariantContext> converter,
      String analysisId, VariantContext variant){
    val analyzedSampleId = sampleMetadataConsensus.getAnalyzedSampleId();
    return PlainSSMPrimary.builder()
        .analysisId(analysisId)
        .analyzedSampleId(analyzedSampleId)
        .mutationType(mutationType.toString())
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
        .build();

  }
    private SSMPrimary buildConsensusSSMPrimary(MutationTypes mutationType, String analysisId, VariantContext variant){
    val analyzedSampleId = sampleMetadataConsensus.getAnalyzedSampleId();
    return PlainSSMPrimary.builder()
        .analysisId(analysisId)
        .analyzedSampleId(analyzedSampleId)
        .mutationType(mutationType.toString())
        .chromosomeStart(CHROMOSOME_START_CONVERTER_STRATEGY.convert(mutationType, variant ))
        .chromosomeEnd(CHROMOSOME_END_CONVERTER_STRATEGY.convert(mutationType, variant))
        .referenceGenomeAllele(REFERENCE_GENOME_ALLELE_CONVERTER_STRATEGY.convert(mutationType,variant))
        .controlGenotype(CONTROL_GENOTYPE_CONVERTER_STRATEGY.convert(mutationType,variant))
        .mutatedFromAllele(MUTATED_FROM_ALLELE_CONVERTER_STRATEGY.convert(mutationType,variant))
        .tumorGenotype(TUMOR_GENOTYPE_CONVERTER_STRATEGY.convert(mutationType,variant))
        .mutatedToAllele(MUTATED_TO_ALLELE_CONVERTER_STRATEGY.convert(mutationType,variant))
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
        .build();
  }

  public List<DccTransformerContext<SSMPrimary>> convertSSMPrimary(VariantContext variant){
    val mutationType = resolveMutationType(variant);
    val dccPrimaryTransformerCTXList = Lists.<DccTransformerContext<SSMPrimary>>newArrayList();
    val dataType = convertToDataType(mutationType);
    val consensusAnalysisId = getConsensusAnalysisId(dataType);
    val converter = VARIANT_CONVERTER_STRATEGY_MUX.select(mutationType);
    val ssmPrimaryConsensus= buildConsensusSSMPrimary(mutationType, converter, consensusAnalysisId, variant);
    addSSMPrimary(dccPrimaryTransformerCTXList, CONSENSUS, ssmPrimaryConsensus , dataType);

    for (val workflowType : extractWorkflowTypes(variant)){
      val ssmPrimary = createCallerSpecificSSMPrimary(sampleMetadataConsensus, ssmPrimaryConsensus, workflowType, dataType);
      addSSMPrimary(dccPrimaryTransformerCTXList, workflowType, ssmPrimary, dataType);
    }
    return ImmutableList.copyOf(dccPrimaryTransformerCTXList);
  }

}
