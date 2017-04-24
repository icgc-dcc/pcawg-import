package org.icgc.dcc.pcawg.client.vcf.converters.file;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor;
import org.icgc.dcc.pcawg.client.vcf.DataTypeConversionException;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantConverter;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategyMux;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgErrorException;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.util.Set;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_TO_DATA_TYPE_CONVERSION_ERROR;

@Slf4j
public class PrimaryDTCConverter {

  public static PrimaryDTCConverter newPrimaryDTCConverter(ConsensusVariantConverter consensusVariantConverter,
      VariantConverterStrategyMux variantConverterStrategyMux ){
    return new PrimaryDTCConverter(consensusVariantConverter, variantConverterStrategyMux);
  }

  /**
   * Constants
   */
  private static final Set<DccTransformerContext<SSMPrimary>> EMPTY_DCC_PRIMARY_TRANSFORMER_CONTEXT = ImmutableSet.<DccTransformerContext<SSMPrimary>>of();

  /**
   * Dependencies
   */
  @NonNull private final ConsensusVariantConverter consensusVariantConverter;
  @NonNull private final VariantConverterStrategyMux variantConverterStrategyMux;

  /**
   * State
   */
  private PcawgErrorException candidateException;
  private int erroredVariantCount;

  public PrimaryDTCConverter(
      ConsensusVariantConverter consensusVariantConverter, VariantConverterStrategyMux variantConverterStrategyMux) {
    this.consensusVariantConverter = consensusVariantConverter;
    this.variantConverterStrategyMux = variantConverterStrategyMux;
    resetStreamState();
  }

  private void resetStreamState(){
    candidateException = new PcawgErrorException( "VariantErrors occured");
    erroredVariantCount = 0;
  }

  public Set<DccTransformerContext<SSMPrimary>> convert(VariantContext variantContext, CounterMonitor primaryCounterMonitor){
    Set<DccTransformerContext<SSMPrimary>> out = EMPTY_DCC_PRIMARY_TRANSFORMER_CONTEXT;
    try{
      val ssmPrimarySet = consensusVariantConverter.convertSSMPrimary(variantConverterStrategyMux, variantContext);
      out = ssmPrimarySet.stream()
          .map(x -> newDccTransformerContext(x.getWorkflowType(),x))
          .collect(toImmutableSet());
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
      log.error("The input variant filter stream has the following errors with start positions: {}", sb.toString());
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
}
