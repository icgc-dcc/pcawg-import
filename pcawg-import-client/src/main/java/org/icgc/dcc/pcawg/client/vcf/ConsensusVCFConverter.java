package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilter;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor;
import org.icgc.dcc.pcawg.client.vcf.ConsensusVariantConverter.DataTypeConversionException;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor.newMonitor;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter.Tuple.newTuple;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVariantConverter.calcAnalysisId;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_TO_DATA_TYPE_CONVERSION_ERROR;

@Slf4j
public class ConsensusVCFConverter {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;

  public static final ConsensusVCFConverter newConsensusVCFConverter(@NonNull Path vcfPath,
      @NonNull SampleMetadata sampleMetadataConsensus, VariantFilterFactory variantFilterFactory){
    return new ConsensusVCFConverter(vcfPath, sampleMetadataConsensus, variantFilterFactory);
  }

  @Value
  public static class Tuple{
    public static final Tuple newTuple(WorkflowTypes workflowType, DataTypes dataType){
      return new Tuple(workflowType, dataType);
    }
    @NonNull private final WorkflowTypes workflowType;
    @NonNull private final DataTypes dataType;
  }

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

  private static List<Tuple> aggregateDistinctWorkflowTypeAndDataType(Set<DccTransformerContext<SSMPrimary>> dccPrimaryTransformerContexts){
    val set = Sets.<Tuple>newHashSet();
    val list = ImmutableList.<Tuple>builder();
    // Create unique order list of tuples
    for (val ctx : dccPrimaryTransformerContexts){
      val tuple = newTuple(ctx.getWorkflowTypes(), ctx.getDataType());
      if (!set.contains(tuple)){
        list.add(tuple);
        set.add(tuple);
      }
    }
    return list.build();
  }


  /**
   * Configuration
   */
  private final VCFFileReader vcf;
  private final File vcfFile;
  private final SampleMetadata sampleMetadataConsensus;
  private final VariantFilter variantFilter;

  /**
   * State
   */
  private final Set<DccTransformerContext<SSMPrimary>> ssmPrimarySet = Sets.newHashSet();
  private final Set<DccTransformerContext<SSMMetadata>> ssmMetadataSet = Sets.newHashSet();
  private final Set<WorkflowTypes> workflowTypesSet = Sets.newHashSet();
  private final ConsensusVariantConverter consensusVariantConverter;
  private PcawgVCFException candidateException;
  private int erroredVariantCount = 0;
  private final CounterMonitor variantMonitor = newMonitor("variantCounter", 100000);

  @Getter
  private int variantCount;

  private ConsensusVCFConverter(@NonNull Path vcfPath, @NonNull SampleMetadata sampleMetadataConsensus, @NonNull VariantFilterFactory variantFilterFactory){
    this.vcfFile = vcfPath.toFile();
    checkArgument(vcfFile.exists(), "The VCF File [{}] DNE", vcfPath.toString());
    this.vcf = newDefaultVCFFileReader(vcfFile);
    this.sampleMetadataConsensus = sampleMetadataConsensus;
    this.consensusVariantConverter = new ConsensusVariantConverter(sampleMetadataConsensus);
    this.variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadataConsensus.isUsProject());
  }



  private void addSSMMetadata(WorkflowTypes workflowType, DataTypes dataType, SSMMetadata ssmMetadata){
    ssmMetadataSet.add(
        DccTransformerContext.<SSMMetadata>builder()
            .object(ssmMetadata)
            .dataType(dataType)
            .workflowTypes(workflowType)
            .build());
  }

  /**
   * Converts input variant to Consensus ssmPrimary and other ssmPrimary (depending on Callers attribute in info field), and stores it state variable
   * @param variant input variant to be converted/processed
   */
  private void convertConsensusVariant(VariantContext variant){
    val dccPrimaryTransformerContextSet =  consensusVariantConverter.convertSSMPrimary(variant);
    ssmPrimarySet.addAll(dccPrimaryTransformerContextSet);
    variantMonitor.incr(dccPrimaryTransformerContextSet.size());
  }

  private void buildSSMMetadatas(){
    val uniqeTupleList = aggregateDistinctWorkflowTypeAndDataType(ssmPrimarySet);
    for (val tuple : uniqeTupleList){
      val workflowType = tuple.getWorkflowType();
      val dataType= tuple.getDataType();
      if(dataType == INDEL || dataType == SNV_MNV){
        val ssmMetadata = newSSMMetadata(sampleMetadataConsensus,workflowType, dataType);
        addSSMMetadata(workflowType, dataType, ssmMetadata);
      } else {
        throw new PcawgVCFException(this.vcfFile.getName(),String.format("The dataType [%s] is not supported", dataType.getName()));
      }
    }
  }



  /**
   * Main loading method. Uses configuration variable to maniluplate state variables
   */

  //TODO: need to add tests for malformed VCFs not being included in data set


  public void process(){
    variantCount = 1;
    candidateException = new PcawgVCFException(vcfFile.getAbsolutePath(),
        String.format("VariantErrors occured in the file [%s]", vcfFile.getAbsolutePath()));
    erroredVariantCount = 0;

    variantMonitor.start();
    for (val variant : vcf){
      if (!variantFilter.isFiltered(variant)) {
        try {
          convertConsensusVariant(variant);
        } catch (DataTypeConversionException e) {
          candidateException.addError(MUTATION_TYPE_TO_DATA_TYPE_CONVERSION_ERROR, getStart(variant));
          erroredVariantCount++;
        } catch (PcawgVariantException e) {
          val start = getStart(variant);
          for (val error : e.getErrors()) {
            candidateException.addError(error, start);
          }
          erroredVariantCount++;
        } finally {
          variantCount++;
        }
      }

    }
    variantMonitor.stop();
    variantMonitor.displaySummary();

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

  public int getNumBadVariantsCount(){
    return erroredVariantCount;
  }

  public int getBadSSMPrimaryCount(){
    val set = Sets.newHashSet();
    for (val error : candidateException.getVariantErrors()){
      val startSet = candidateException.getErrorVariantStart(error);
      set.addAll(startSet);
    }
    return set.size();
  }



  public Set<DccTransformerContext<SSMMetadata>> readSSMMetadata(){
    return ImmutableSet.copyOf(this.ssmMetadataSet);
  }

  public Set<DccTransformerContext<SSMPrimary>> readSSMPrimary(){
    return ImmutableSet.copyOf(this.ssmPrimarySet);
  }


}
