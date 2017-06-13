package org.icgc.dcc.pcawg.client.filter.variant;

import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.filter.coding.SnpEffCodingFilter;

import java.io.Closeable;
import java.io.IOException;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFEncoder;

@RequiredArgsConstructor(access = PRIVATE)
@Getter
public class VariantFilterFactory implements Closeable{

  public static final VariantFilterFactory newVariantFilterFactory(final boolean bypassTcgaFiltering, final boolean bypassNoiseFiltering){
    SnpEffCodingFilter snpEffCodingFilter = null;
    if (!bypassTcgaFiltering){
      snpEffCodingFilter = new SnpEffCodingFilter();
    }
    return new VariantFilterFactory(bypassTcgaFiltering, bypassNoiseFiltering, snpEffCodingFilter);
  }

  public static final VariantFilterFactory newVariantFilterFactory(SnpEffCodingFilter snpEffCodingFilter, final boolean bypassTcgaFiltering, final boolean bypassNoiseFiltering){
    return new VariantFilterFactory(bypassTcgaFiltering, bypassNoiseFiltering, snpEffCodingFilter);
  }

  private final boolean bypassTcgaFiltering;
  private final boolean bypassNoiseFiltering;
  private final SnpEffCodingFilter snpEffCodingFilter;

  public VariantFilter createVariantFilter(VCFHeader vcfHeader, final boolean isUsProject){
    val vcfEncoder = newDefaultVCFEncoder(vcfHeader);
    return createVariantFilter(vcfEncoder, isUsProject);
  }

  public VariantFilter createVariantFilter(VCFFileReader vcfFileReader, final boolean isUsProject){
    val vcfEncoder = newDefaultVCFEncoder(vcfFileReader);
    return createVariantFilter(vcfEncoder, isUsProject);
  }

  public VariantFilter createVariantFilter(VCFEncoder vcfEncoder, final boolean isUsProject){
    return new VariantFilter(vcfEncoder,snpEffCodingFilter,isUsProject,bypassTcgaFiltering, bypassNoiseFiltering);
  }

  public VariantFilter createVariantFilter(SnpEffCodingFilter snpEffCodingFilter, VCFEncoder vcfEncoder, final boolean isUsProject){
    return new VariantFilter(vcfEncoder,snpEffCodingFilter,isUsProject,bypassTcgaFiltering, bypassNoiseFiltering);
  }

  @Override public void close() throws IOException {
    if (snpEffCodingFilter != null){
      snpEffCodingFilter.destroy();
    }
  }
}
