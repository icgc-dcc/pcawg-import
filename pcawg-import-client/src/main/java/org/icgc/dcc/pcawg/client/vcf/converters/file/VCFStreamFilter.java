package org.icgc.dcc.pcawg.client.vcf.converters.file;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilter;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.utils.measurement.Countable;

import java.nio.file.Path;
import java.util.stream.Stream;

import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.utils.measurement.IntegerCounter.newDefaultIntegerCounter;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;

@Slf4j
public class VCFStreamFilter {

  public static VCFStreamFilter newVCFStreamFilter(VCFFileReader vcf, VariantFilter variantFilter){
    return new VCFStreamFilter(vcf, variantFilter);
  }

  public static VCFStreamFilter newVCFStreamFilter(Path vcfPath, SampleMetadata sampleMetadataConsensus,
      VariantFilterFactory variantFilterFactory){
    val vcf = newDefaultVCFFileReader(vcfPath.toFile());
    val variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadataConsensus.isUsProject());
    return newVCFStreamFilter(vcf, variantFilter);
  }

  /**
   * Configuration
   */
  @NonNull private final VCFFileReader vcf;
  @NonNull private final VariantFilter variantFilter;

  /**
   * State
   */
  private Countable<Integer> variantBeforeFilterCounter ;
  private Countable<Integer> variantAfterFilterCounter ;

  public VCFStreamFilter(VCFFileReader vcf, VariantFilter variantFilter) {
    this.vcf = vcf;
    this.variantFilter = variantFilter;
    resetStreamState();
  }

  private void resetStreamState(){
    variantBeforeFilterCounter = newDefaultIntegerCounter();
    variantAfterFilterCounter = newDefaultIntegerCounter();
  }

  public Stream<VariantContext> streamFilteredVariants(){
    return stream(vcf)
        .map(variantBeforeFilterCounter::streamIncr)
        .filter(variantFilter::passedAllFilters)
        .map(variantAfterFilterCounter::streamIncr);
  }

  public int getTotalNumVariants(){
    return variantBeforeFilterCounter.getCount();
  }

  public int getFilteredNumVariants(){
    return variantAfterFilterCounter.getCount();
  }

}

