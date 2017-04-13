package org.icgc.dcc.pcawg.client.filter.variant;

import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.filter.coding.SnpEffCodingFilter;

import java.io.Closeable;
import java.io.IOException;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public class VariantFilterFactory implements Closeable{

  public static final VariantFilterFactory newVariantFilterFactory(final boolean bypassTcgaFiltering, final boolean bypassNoiseFiltering){
    SnpEffCodingFilter snpEffCodingFilter = null;
    if (!bypassTcgaFiltering){
      snpEffCodingFilter = new SnpEffCodingFilter();
    }
    return new VariantFilterFactory(bypassTcgaFiltering, bypassNoiseFiltering, snpEffCodingFilter);
  }

  private final boolean bypassTcgaFiltering;
  private final boolean bypassNoiseFiltering;
  private final SnpEffCodingFilter snpEffCodingFilter;

  public VariantFilter createVariantFilter(VCFHeader vcfHeader, final boolean isUsProject){
    return VariantFilter.newVariantFilter(vcfHeader,snpEffCodingFilter,isUsProject,bypassTcgaFiltering, bypassNoiseFiltering);
  }

  public VariantFilter createVariantFilter(VCFFileReader vcfFileReader, final boolean isUsProject){
    return createVariantFilter(vcfFileReader.getFileHeader(), isUsProject);
  }

  public VariantFilter createVariantFilter(VCFEncoder vcfEncoder, final boolean isUsProject){
    return new VariantFilter(vcfEncoder,snpEffCodingFilter,isUsProject,bypassTcgaFiltering, bypassNoiseFiltering);
  }

  @Override public void close() throws IOException {
    snpEffCodingFilter.destroy();
  }
}
