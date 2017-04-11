package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PlainSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMMetadata;
import static org.icgc.dcc.pcawg.client.model.NACodes.DATA_VERIFIED_TO_BE_UNKNOWN;
import static org.icgc.dcc.pcawg.client.model.ssm.primary.impl.IndelPcawgSSMPrimary.newIndelSSMPrimary;
import static org.icgc.dcc.pcawg.client.model.ssm.primary.impl.SnvMnvPcawgSSMPrimary.newSnvMnvSSMPrimary;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.VCF.streamCallers;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@Slf4j
public class ConsensusVCFConverter {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;

  public static final ConsensusVCFConverter newConsensusVCFConverter(@NonNull Path vcfPath,
      @NonNull SampleMetadata sampleMetadataConsensus){
    return new ConsensusVCFConverter(vcfPath, sampleMetadataConsensus);
  }

  private static SSMPrimary buildSSMPrimary(SampleMetadata sampleMetadata, VariantContext variant) throws PcawgVariantException{
    val dataType = sampleMetadata.getDataType();
    val analysisId = sampleMetadata.getAnalysisId();
    val analyzedSampleId = sampleMetadata.getAnalyzedSampleId();

    if (dataType == INDEL){
      return newIndelSSMPrimary(variant, analysisId, analyzedSampleId);
    } else if(dataType == SNV_MNV){
      return newSnvMnvSSMPrimary(variant, analysisId, analyzedSampleId);
    } else {
      val message = String.format("The DataType [%s] is not supported", dataType.name());
      throw new PcawgVariantException(message, variant, PcawgVariantErrors.DATA_TYPE_NOT_SUPPORTED);
    }
  }

  private static Set<WorkflowTypes> extractWorkflowTypes(VariantContext  variant){
    return streamCallers(variant)
        .map(c ->  WorkflowTypes.parseMatch(c, F_CHECK_CORRECT_WORKTYPE ))
        .collect(toImmutableSet());
  }

  // As specified in the spec, the callerSpecific ssmPrimary is the same as the consensus one,
  // with only the analysisId, totalReadCount and mutantAlleleReadCount being
  // different (hence the cloning and modifing)
  private static SSMPrimary createCallerSpecificSSMPrimary(SampleMetadata sampleMetadataConsensus,
      SSMPrimary ssmPrimaryConsensus, WorkflowTypes workflowType){
    val callerSampleMetadata = SampleMetadata.builderWith(sampleMetadataConsensus)
        .workflowType(workflowType) // Overwrite workflowType, since everything else is the same
        .build();
    return PlainSSMPrimary.builderWith(ssmPrimaryConsensus)
        .analysisId(callerSampleMetadata.getAnalysisId())
        .totalReadCount(DATA_VERIFIED_TO_BE_UNKNOWN.toInt())
        .mutantAlleleReadCount(DATA_VERIFIED_TO_BE_UNKNOWN.toInt())
        .build();
  }

  private static SSMMetadata createCallerSpecificSSMMetadata(SampleMetadata sampleMetadataConsensus,
      SSMMetadata ssmMetadataConsensus, WorkflowTypes workflowType){
    val callerSampleMetadata = SampleMetadata.builderWith(sampleMetadataConsensus)
        .workflowType(workflowType) // Overwrite workflowType, since everything else is the same
        .build();
    val variationCallingAlgorithmText = VariationCallingAlgorithms.get(workflowType, callerSampleMetadata.getDataType());
    return PlainSSMMetadata.builderWith(ssmMetadataConsensus)
        .analysisId(callerSampleMetadata.getAnalysisId())
        .variationCallingAlgorithm(variationCallingAlgorithmText)
        .build();
  }

  /**
   * Configuration
   */
  private final VCFFileReader vcf;
  private final File vcfFile;
  private final SampleMetadata sampleMetadataConsensus;

  /**
   * State
   */
  private final List<DccTransformerContext<SSMPrimary>> ssmPrimaryList = Lists.newArrayList();
  private final Set<DccTransformerContext<SSMMetadata>> ssmMetadataSet = Sets.newHashSet();
  private final Set<WorkflowTypes> workflowTypesSet = Sets.newHashSet();
  private PcawgVCFException candidateException;

  @Getter
  private int variantCount;

  private ConsensusVCFConverter(@NonNull Path vcfPath, @NonNull SampleMetadata sampleMetadataConsensus){
    this.vcfFile = vcfPath.toFile();
    checkArgument(vcfFile.exists(), "The VCF File [{}] DNE", vcfPath.toString());
    this.vcf = new VCFFileReader(vcfFile, REQUIRE_INDEX_CFG);
    this.sampleMetadataConsensus = sampleMetadataConsensus;
  }

  private void addSSMPrimary(WorkflowTypes workflowType, SSMPrimary ssmPrimary){
    ssmPrimaryList.add(
        DccTransformerContext.<SSMPrimary>builder()
        .object(ssmPrimary)
        .workflowTypes(workflowType)
        .build());
  }

  private void addSSMMetadata(WorkflowTypes workflowType, SSMMetadata ssmMetadata){
    ssmMetadataSet.add(
        DccTransformerContext.<SSMMetadata>builder()
            .object(ssmMetadata)
            .workflowTypes(workflowType)
            .build());
  }

  /**
   * Converts input variant to Consensus ssmPrimary and other ssmPrimary (depending on Callers attribute in info field), and stores it state variable
   * @param variant input variant to be converted/processed
   */
  private void convertConsensusVariant(VariantContext variant){
    val ssmPrimaryConsensus = buildSSMPrimary(sampleMetadataConsensus, variant);
    addSSMPrimary(CONSENSUS, ssmPrimaryConsensus);

    for (val workflowType : extractWorkflowTypes(variant) ){
      workflowTypesSet.add(workflowType);
      val ssmPrimary = createCallerSpecificSSMPrimary(sampleMetadataConsensus, ssmPrimaryConsensus, workflowType);
      addSSMPrimary(workflowType, ssmPrimary);
    }
  }

  /**
   * Creates SSMMetadata based on Consensus ssmMetadata, and using the state variables that recorded the unique set of Callers recorded, generated the other callers' ssmMetadata;
   */
  private void buildSSMMetadatas(){
    val ssmMetadataConsensus = newSSMMetadata(sampleMetadataConsensus);
    addSSMMetadata(CONSENSUS, ssmMetadataConsensus);

    for (val workflowType : workflowTypesSet){
      val ssmMetadata = createCallerSpecificSSMMetadata(sampleMetadataConsensus,ssmMetadataConsensus,workflowType);
      addSSMMetadata(workflowType, ssmMetadata);
    }
  }


  /**
   * Main loading method. Uses configuration variable to maniluplate state variables
   */

  public void process(){
    variantCount = 1;
    candidateException = new PcawgVCFException(vcfFile.getAbsolutePath(),
        String.format("VariantErrors occured in the file [%s]", vcfFile.getAbsolutePath()));
    for (val variant : vcf){
      try{
        convertConsensusVariant(variant);
      } catch (PcawgVariantException e){
        for (val error : e.getErrors()){
          candidateException.addError(error, variantCount);
        }
      } finally{
        variantCount++;
      }
    }

    buildSSMMetadatas();

    if (candidateException.hasErrors()){
      val sb = new StringBuilder();
      for (val error : candidateException.getVariantErrors()){
        sb.append(String.format("\t%s:%s ---- ",error.name(),candidateException.getErrorVariantStart(error)));
      }
      log.error("The vcf file [{}] has the following errors with start positions: {}", vcfFile.getAbsolutePath(), sb.toString());
      throw candidateException;
    }

  }

  public int getBadSSMPrimaryCount(){
    return  candidateException.getVariantErrors().stream()
        .flatMap(e -> candidateException.getErrorVariantStart(e).stream())
        .mapToInt(x -> x)
        .sum();
  }



  public Set<DccTransformerContext<SSMMetadata>> readSSMMetadata(){
    return ImmutableSet.copyOf(this.ssmMetadataSet);
  }

  public List<DccTransformerContext<SSMPrimary>> readSSMPrimary(){
    return ImmutableList.copyOf(this.ssmPrimaryList);
  }


}
