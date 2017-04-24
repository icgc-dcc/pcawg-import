package org.icgc.dcc.pcawg.client.vcf.converters.variant;

import com.google.common.collect.ImmutableSet;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.metadata.ConsensusSampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;
import org.icgc.dcc.pcawg.client.vcf.DataTypeConversionException;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.VCF;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategy;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategyMux;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.util.Set;
import java.util.stream.Stream;

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
import static org.icgc.dcc.pcawg.client.vcf.VCF.getAltCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getChomosome;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getRefCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.streamCallers;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED_ERROR;

@Slf4j
public class ConsensusVariantConverter  {

  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;
  private static final int DEFAULT_STRAND = 1;
  private static final String DEFAULT_VERIFICATION_STATUS = "not tested";
  private static final boolean DEFAULT_THROW_EXCEPTION_FLAG = true;

  public static ConsensusVariantConverter newConsensusVariantConverter(ConsensusSampleMetadata consensusSampleMetadata){
    return new ConsensusVariantConverter(consensusSampleMetadata);
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

  private static void checkIsConsensus(SampleMetadata sampleMetadata){
    if(sampleMetadata.getWorkflowType() != CONSENSUS){
      throw new NotConsensusWorkflowTypeException(
          String.format("The workflow type [%s] is not supported, only the [%s] is supported",
              sampleMetadata.getWorkflowType(), CONSENSUS));
    }
  }

  @NonNull private final ConsensusSampleMetadata consensusSampleMetadata;

  public ConsensusVariantConverter(ConsensusSampleMetadata consensusSampleMetadata) {
    this.consensusSampleMetadata = consensusSampleMetadata;
    checkIsConsensus(consensusSampleMetadata);
  }

  public static MutationTypes resolveMutationType(VariantContext v){
    return resolveMutationType(DEFAULT_THROW_EXCEPTION_FLAG, v);
  }

  public static MutationTypes resolveMutationType(boolean throwException, VariantContext v){
    val ref = VCF.getReferenceAlleleString(v);
    val alt = VCF.getFirstAlternativeAlleleString(v);
    val refLength = getReferenceAlleleLength(v);
    val altLength = getFirstAlternativeAlleleLength(v);
    val altIsOne = altLength ==1;
    val refIsOne = refLength ==1;
    val refStartsWithAlt = ref.startsWith(alt);
    val altStartsWithRef = alt.startsWith(ref);
    val  lengthDifference = refLength - altLength;

    if (lengthDifference < 0 && !altIsOne && !refStartsWithAlt){
      return refIsOne && altStartsWithRef ? MutationTypes.INSERTION_LTE_200BP : MutationTypes.MULTIPLE_BASE_SUBSTITUTION;

    } else if(lengthDifference == 0 && !refStartsWithAlt && !altStartsWithRef){
      return refIsOne ? MutationTypes.SINGLE_BASE_SUBSTITUTION : MutationTypes.MULTIPLE_BASE_SUBSTITUTION;

    } else if(lengthDifference > 0 && !refIsOne && !altStartsWithRef ){
      return altIsOne && refStartsWithAlt ? MutationTypes.DELETION_LTE_200BP : MutationTypes.MULTIPLE_BASE_SUBSTITUTION;

    } else {
      val message = String.format("The mutationType of the variant cannot be resolved: Ref[%s] Alt[%s] --> RefLength-AltLength=%s,  RefLengthIsOne[%s] AltLengthIsOne[%s], RefStartsWithAlt[%s] AltStartsWithRef[%s] ", lengthDifference, ref, alt, refIsOne, altIsOne, refStartsWithAlt, altStartsWithRef);

      if (throwException){
        throw new PcawgVariantException(message, v, MUTATION_TYPE_NOT_SUPPORTED_ERROR);
      } else {
        return MutationTypes.UNKNOWN;
      }
    }
  }


  public static DataTypes resolveDataType(VariantContext variantContext){
    val mutationType = resolveMutationType(variantContext);
    return resolveDataType(mutationType);
  }

  public static DataTypes resolveDataType(MutationTypes mutationType){
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

  public Set<SSMPrimary> convertSSMPrimary(VariantConverterStrategyMux variantConverterMux, VariantContext variant){
    val workflowType = consensusSampleMetadata.getWorkflowType();
    val mutationType = resolveMutationType(variant);
    val variantConverter = variantConverterMux.select(mutationType);
    val consensusSSMPrimary =  buildSSMPrimary(workflowType,variantConverter,variant);
    val setBuilder = ImmutableSet.<SSMPrimary>builder();
    setBuilder.add(consensusSSMPrimary);
    for (val workflow : extractWorkflowTypes(variant)){
      val ssmPrimary = createWorkflowTypeSpecificSSMPrimary(consensusSSMPrimary,workflow);
      setBuilder.add(ssmPrimary);
    }
    return setBuilder.build();
  }

  public SSMPrimary buildSSMPrimary(WorkflowTypes workflowTypes, VariantConverterStrategy<VariantContext> converter, VariantContext variant){
    val mutationType = resolveMutationType(variant);
    val dataType = resolveDataType(mutationType);
    val dccProjectCode = consensusSampleMetadata.getDccProjectCode();
    val analyzedSampleId = consensusSampleMetadata.getAnalyzedSampleId();

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
        .build();
  }

}
