package org.icgc.dcc.pcawg.client.filter.variant;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.filter.coding.SnpEffCodingFilter;

@RequiredArgsConstructor
@Slf4j
public class VariantFilter {

  @NonNull private final VCFEncoder encoder;
  private final SnpEffCodingFilter snpEffCodingFilter;
  private final boolean isUsProject;
  private final boolean bypassTcgaFiltering;
  private final boolean bypassNoiseFiltering;

  public boolean passedAllFilters(VariantContext variantContext){
    return passedNoiseFilter(variantContext) && passedTcgaFilter(variantContext);
  }

  public boolean notPassedAllFilters(VariantContext variantContext){
    return ! passedAllFilters(variantContext);
  }

  public boolean passedTcgaFilter(VariantContext variantContext){
    if (bypassTcgaFiltering){
      return true;
    } else if (isUsProject){
      if (snpEffCodingFilter == null){
        log.error("SnpEffCodingFilter is null but not expecting null");
        return false;
      }else {
        val variantString = encoder.encode(variantContext);
        return snpEffCodingFilter.isCoding(variantString);
      }
    } else {
      return true;
    }
  }

  public boolean passedNoiseFilter(VariantContext variantContext){
    if (bypassNoiseFiltering) {
      return true;
    } else {
      return variantContext.isNotFiltered();
    }
  }



}
