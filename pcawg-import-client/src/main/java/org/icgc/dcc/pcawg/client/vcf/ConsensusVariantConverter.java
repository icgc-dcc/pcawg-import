package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;
import org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter.Tuple;
import org.icgc.dcc.pcawg.client.vcf.converters.VariantConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.VariantConverterStrategyMux;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.model.NACodes.CORRUPTED_DATA;
import static org.icgc.dcc.pcawg.client.model.NACodes.DATA_VERIFIED_TO_BE_UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter.Tuple.newTuple;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.DELETION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.INSERTION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.SINGLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.resolveMutationType;
import static org.icgc.dcc.pcawg.client.vcf.SSMClassification.newSSMClassification;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getAltCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getChomosome;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getRefCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.streamCallers;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@RequiredArgsConstructor
public class ConsensusVariantConverter  {

  private static final VariantConverterStrategyMux VARIANT_CONVERTER_STRATEGY_MUX = new VariantConverterStrategyMux();

  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;
  private static final int DEFAULT_STRAND = 1;
  private static final String DEFAULT_VERIFICATION_STATUS = "not tested";

  public static SSMMetadata newSSMMetadata(SampleMetadata sampleMetadata, WorkflowTypes workflowType,DataTypes dataType){
    val analysisId = calcAnalysisId(sampleMetadata.getDccProjectCode(), workflowType, dataType);
    return PcawgSSMMetadata.newSSMMetadataImpl(
        VariationCallingAlgorithms.get(workflowType, dataType),
        sampleMetadata.getMatchedSampleId(),
        analysisId,
        sampleMetadata.getAnalyzedSampleId(),
        sampleMetadata.isUsProject(),
        sampleMetadata.getAliquotId(),
        sampleMetadata.getAnalyzedFileId(),
        sampleMetadata.getMatchedFileId());
  }

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

  public static Set<WorkflowTypes> extractWorkflowTypes(VariantContext  variant){
    return streamWorkflowTypes(variant)
        .collect(toImmutableSet());
  }

  public static Stream<WorkflowTypes> streamWorkflowTypes(VariantContext  variant){
    return streamCallers(variant)
        .map(c ->  WorkflowTypes.parseMatch(c, F_CHECK_CORRECT_WORKTYPE ));
  }

  public static Stream<Tuple> streamTuple(VariantContext variant){
    val mutationType = MutationTypes.resolveMutationType(variant);
    return streamWorkflowTypes(variant)
        .map(w -> newTuple(w, convertToDataType(mutationType)))
        .distinct();
  }


  private DccTransformerContext<SSMMetadata> createDccMetadataTCtx(WorkflowTypes workflowType, MutationTypes mutationType){
    val ssmClassification = newSSMClassification(workflowType,mutationType);
    val dataType = ssmClassification.getDataType();
    if(dataType == INDEL || dataType == SNV_MNV){
      val ssmMetadata = newSSMMetadata(sampleMetadataConsensus,workflowType, dataType);
      return newDccTransformerContext(ssmClassification, ssmMetadata);
    } else {
      throw new PcawgVCFException("move this to vcf converter",String.format("The dataType [%s] is not supported", dataType.getName()));
    }
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

  private static void addSSMPrimary(List<DccTransformerContext<SSMPrimary>> ssmPrimaryList, SSMPrimary ssmPrimary, SSMClassification ssmClassification){
    ssmPrimaryList.add( newDccTransformerContext(ssmClassification, ssmPrimary));
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

  public Stream<DccTransformerContext<SSMPrimary>> streamSSMPrimary(VariantContext variant){
    val mutationType = resolveMutationType(variant);
    val ssmClassification = newSSMClassification(CONSENSUS, mutationType);
    val dataType = ssmClassification.getDataType();
    val consensusAnalysisId = getConsensusAnalysisId(dataType);
    val converter = VARIANT_CONVERTER_STRATEGY_MUX.select(mutationType);
    val ssmPrimaryConsensus= buildConsensusSSMPrimary(mutationType, converter, consensusAnalysisId, variant);
    val list = Lists.<DccTransformerContext<SSMPrimary>>newArrayList();
    val ssmPrimaryDccTransformerContextConsensus = newDccTransformerContext(ssmClassification, ssmPrimaryConsensus);
    list.add(ssmPrimaryDccTransformerContextConsensus);
    for (val workflowType : extractWorkflowTypes(variant)){
      val ssmPrimary = createCallerSpecificSSMPrimary(sampleMetadataConsensus, ssmPrimaryConsensus, workflowType, dataType);
      val dccPrimaryTransformerCTX = newDccTransformerContext(workflowType, mutationType, ssmPrimary);
      list.add(dccPrimaryTransformerCTX);
    }
    return list.stream();
  }

  public Set<DccTransformerContext<SSMPrimary>> convertSSMPrimary(VariantContext variant){
    val mutationType = resolveMutationType(variant);
    val ssmConsensusClassification = newSSMClassification(CONSENSUS, mutationType);
    val dccPrimaryTransformerCTXList = Lists.<DccTransformerContext<SSMPrimary>>newArrayList();
    val dataType = ssmConsensusClassification.getDataType();
    val consensusAnalysisId = getConsensusAnalysisId(dataType);
    val converter = VARIANT_CONVERTER_STRATEGY_MUX.select(mutationType);
    val ssmPrimaryConsensus= buildConsensusSSMPrimary(mutationType, converter, consensusAnalysisId, variant);
    addSSMPrimary(dccPrimaryTransformerCTXList, ssmPrimaryConsensus , ssmConsensusClassification);

    for (val workflowType : extractWorkflowTypes(variant)){
      val ssmPrimary = createCallerSpecificSSMPrimary(sampleMetadataConsensus, ssmPrimaryConsensus, workflowType, dataType);
      val ssmClassification = newSSMClassification(workflowType, mutationType);
      addSSMPrimary(dccPrimaryTransformerCTXList, ssmPrimary, ssmClassification);
    }
    return ImmutableSet.copyOf(dccPrimaryTransformerCTXList);
  }

}
