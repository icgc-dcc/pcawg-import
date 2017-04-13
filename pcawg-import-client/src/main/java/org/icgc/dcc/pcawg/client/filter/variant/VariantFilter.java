package org.icgc.dcc.pcawg.client.filter.variant;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFHeader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.filter.coding.SnpEffCodingFilter;

@RequiredArgsConstructor
@Slf4j
public class VariantFilter {
  private static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  private static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;

  private static final VCFEncoder newVCFEncoder(VCFHeader vcfHeader){
    return new VCFEncoder(vcfHeader,ALLOW_MISSING_FIELDS_IN_HEADER_CFG, OUTPUT_TRAILING_FORMAT_FIELDS_CFG );
  }

  public static final VariantFilter newVariantFilter(VCFHeader vcfHeader, SnpEffCodingFilter snpEffCodingFilter,
      final boolean isUsProject, final boolean bypassTcgaFiltering, final boolean bypassNoiseFiltering ){
    val encoder = newVCFEncoder(vcfHeader);
    return new VariantFilter(encoder, snpEffCodingFilter, isUsProject, bypassTcgaFiltering, bypassNoiseFiltering);
  }

  @NonNull private final VCFEncoder encoder;
  private final SnpEffCodingFilter snpEffCodingFilter;
  private final boolean isUsProject;
  private final boolean bypassTcgaFiltering;
  private final boolean bypassNoiseFiltering;

  public boolean isFiltered(VariantContext variantContext){
    return isNoiseFiltered(variantContext) || isTcgaFiltered(variantContext);
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
