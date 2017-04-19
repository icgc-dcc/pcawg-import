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

  public boolean isFiltered(VariantContext variantContext){
    return isNoiseFiltered(variantContext) || isTcgaFiltered(variantContext);
  }

  public boolean isNotFiltered(VariantContext variantContext){
    return ! isFiltered(variantContext);
  }

  public boolean isTcgaFiltered(VariantContext variantContext){
    if (bypassTcgaFiltering){
      return false;
    } else if (isUsProject){
      if (snpEffCodingFilter == null){
        log.error("SnpEffCodingFilter is null but not expecting null");
        return true;
      }else {
        val variantString = encoder.encode(variantContext);
        return !snpEffCodingFilter.isCoding(variantString);
      }
    } else {
      return false;
    }
  }

  public boolean isNoiseFiltered(VariantContext variantContext){
    if (bypassNoiseFiltering) {
      return false;
    } else {
      return variantContext.isFiltered();
    }
  }



}
