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
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor.newMonitor;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter.Tuple.newTuple;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_TO_DATA_TYPE_CONVERSION_ERROR;

@Slf4j
public class ConsensusVCFConverter {

  private static final Set<DccTransformerContext<SSMPrimary>> EMPTY_DCC_PRIMARY_TRANSFORMER_CONTEXT = ImmutableSet.<DccTransformerContext<SSMPrimary>>of();

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

  private static List<Tuple> aggregateDistinctWorkflowTypeAndDataType(Set<DccTransformerContext<SSMPrimary>> dccPrimaryTransformerContexts){
    val set = Sets.<Tuple>newHashSet();
    val list = ImmutableList.<Tuple>builder();
    // Create unique order list of tuples
    for (val ctx : dccPrimaryTransformerContexts){
      val ssmClassification = ctx.getSSMClassification();
      val tuple = newTuple(ssmClassification.getWorkflowType(), ssmClassification.getDataType());
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
    ssmMetadataSet.add( newDccTransformerContext(workflowType, dataType, ssmMetadata));
  }

  /**
   * Converts input variant to Consensus ssmPrimary and other ssmPrimary (depending on Callers attribute in info field), and stores it state variable
   * @param variant input variant to be converted/processed
   */
  private void convertConsensusVariant(VariantContext variant){
    val dccPrimaryTransformerContextSet =  consensusVariantConverter.convertSSMPrimary(variant);
    ssmPrimarySet.addAll(dccPrimaryTransformerContextSet);
  }

  private Set<DccTransformerContext<SSMPrimary>> convertConsensusVariant2(VariantContext variant, CounterMonitor variantCounterMonitor){
    val dccPrimaryTransformerContextSet =  consensusVariantConverter.convertSSMPrimary(variant);
    variantCounterMonitor.incr(dccPrimaryTransformerContextSet.size());
    return dccPrimaryTransformerContextSet;
  }

  private void buildSSMMetadatas(){
    val uniqeTupleList = aggregateDistinctWorkflowTypeAndDataType(ssmPrimarySet);
    for (val tuple : uniqeTupleList){
      val workflowType = tuple.getWorkflowType();
      val dataType= tuple.getDataType();
      if(dataType == INDEL || dataType == SNV_MNV){
        val ssmMetadata = ConsensusVariantConverter.newSSMMetadata(sampleMetadataConsensus,workflowType, dataType);
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

  public Stream<DccTransformerContext<SSMPrimary>> streamSSMPrimary(CounterMonitor variantCounterMonitor){
    return stream(vcf)
        .map(v -> subSetSSMPrimary(v, variantCounterMonitor))
        .flatMap(Collection::stream);
  }



  private Set<DccTransformerContext<SSMPrimary>>  subSetSSMPrimary(VariantContext variant, CounterMonitor variantCounterMonitor){
    Set<DccTransformerContext<SSMPrimary>> out = EMPTY_DCC_PRIMARY_TRANSFORMER_CONTEXT;
    if (!variantFilter.isFiltered(variant)) {
      try {
        out = convertConsensusVariant2(variant, variantCounterMonitor);
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
    return out;
  }

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
