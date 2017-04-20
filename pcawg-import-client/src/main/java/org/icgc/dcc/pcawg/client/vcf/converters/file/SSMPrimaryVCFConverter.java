package org.icgc.dcc.pcawg.client.vcf.converters.file;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilter;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.utils.measurement.Countable;
import org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor;
import org.icgc.dcc.pcawg.client.vcf.DataTypeConversionException;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantConverter;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.utils.measurement.IntegerCounter.newDefaultIntegerCounter;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_TO_DATA_TYPE_CONVERSION_ERROR;

@Slf4j
public class SSMPrimaryVCFConverter {

  private static final Set<DccTransformerContext<SSMPrimary>> EMPTY_DCC_PRIMARY_TRANSFORMER_CONTEXT = ImmutableSet.<DccTransformerContext<SSMPrimary>>of();
  private static final int DEFAULT_PRIMARY_COUNT_INTERVAL = 100000;

  public static SSMPrimaryVCFConverter newSSMPrimaryVCFConverter(Path vcfPath,
      SampleMetadata sampleMetadataConsensus, VariantFilterFactory variantFilterFactory){
    val vcfFile = vcfPath.toFile();
    checkArgument(vcfFile.exists(), "The VCF File [{}] DNE", vcfPath.toString());
    val vcf = newDefaultVCFFileReader(vcfFile);
    val consensusVariantConverter = new ConsensusVariantConverter(sampleMetadataConsensus);
    val variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadataConsensus.isUsProject());
    return newSSMPrimaryVCFConverter(vcfPath,vcf, variantFilter,consensusVariantConverter);
  }

  public static SSMPrimaryVCFConverter newSSMPrimaryVCFConverter( Path vcfPath, VCFFileReader vcf,
      VariantFilter variantFilter, ConsensusVariantConverter consensusVariantConverter){
    return new SSMPrimaryVCFConverter(vcfPath, vcf, variantFilter, consensusVariantConverter);
  }


  /**
   * Configuration
   */
  @NonNull private final Path vcfPath;
  @NonNull private final VCFFileReader vcf;
  @NonNull private final VariantFilter variantFilter;
  @NonNull private final ConsensusVariantConverter consensusVariantConverter;

  /**
   * State
   */
  private PcawgVCFException candidateException;
  private int erroredVariantCount = 0;
  private Countable<Integer> variantBeforeFilterCounter ;
  private Countable<Integer> variantAfterFilterCounter ;

  public SSMPrimaryVCFConverter(Path vcfPath, VCFFileReader vcf, VariantFilter variantFilter,
      ConsensusVariantConverter consensusVariantConverter) {
    this.vcfPath = vcfPath;
    this.vcf = vcf;
    this.variantFilter = variantFilter;
    this.consensusVariantConverter = consensusVariantConverter;
    resetStreamState();
  }

  private void resetStreamState(){
    variantBeforeFilterCounter = newDefaultIntegerCounter();
    variantAfterFilterCounter = newDefaultIntegerCounter();
    candidateException = new PcawgVCFException(vcfPath.toString(),
        String.format("VariantErrors occured in the file [%s]", vcfPath));
  }

  public Stream<DccTransformerContext<SSMPrimary>> streamSSMPrimary(CounterMonitor primaryCounterMonitor){
    resetStreamState();
    return stream(vcf)
        .map(variantBeforeFilterCounter::streamIncr)
        .filter(variantFilter::isNotFiltered)
        .map(variantAfterFilterCounter::streamIncr)
        .map(v -> convertConsensusVariant(v, primaryCounterMonitor))
        .flatMap(Set::stream);
  }

  public Stream<DccTransformerContext<SSMPrimary>> streamSSMPrimary(){
    return streamSSMPrimary(null);
  }

  private Set<DccTransformerContext<SSMPrimary>> convertConsensusVariant(VariantContext variantContext, CounterMonitor primaryCounterMonitor){
    Set<DccTransformerContext<SSMPrimary>> out = EMPTY_DCC_PRIMARY_TRANSFORMER_CONTEXT;
    try{
      out = consensusVariantConverter.convertSSMPrimary(variantContext);
      if(primaryCounterMonitor != null){
        primaryCounterMonitor.incr(out.size());
      }
    } catch (DataTypeConversionException e) {
      candidateException.addError(MUTATION_TYPE_TO_DATA_TYPE_CONVERSION_ERROR, getStart(variantContext));
      erroredVariantCount++;
    } catch (PcawgVariantException e) {
      val start = getStart(variantContext);
      for (val error : e.getErrors()) {
        candidateException.addError(error, start);
      }
      erroredVariantCount++;
    }
    return out;
  }

  public void checkForErrors(){
    if (candidateException.hasErrors()){
      val sb = new StringBuilder();
      for (val error : candidateException.getVariantErrors()){
        sb.append(String.format("\t%s:%s ---- ",error.name(),candidateException.getErrorVariantStart(error)));
      }
      log.error("The vcf file [{}] has the following errors with start positions: {}", vcfPath.toAbsolutePath(), sb.toString());
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

  public int getTotalNumVariants(){
    return variantBeforeFilterCounter.getCount();
  }

  public int getFilteredNumVariants(){
    return variantAfterFilterCounter.getCount();
  }

}
