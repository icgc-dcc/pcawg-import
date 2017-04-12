package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;

public class ChromosomeEndConverterStrategy extends VariantConverterTemplate<Integer, VariantContext> {

  @Override protected Integer convertDeletion(VariantContext data) {
    return getStart(data) + getReferenceAlleleLength(data) - 1;
  }

  @Override protected Integer convertInsertion(VariantContext data) {
    return getStart(data) + 1;
  }

  @Override protected Integer convertSnv(VariantContext data) {
    return getStart(data) + getReferenceAlleleLength(data) - 1;
  }

  @Override protected Integer convertMnv(VariantContext data) {
    return getStart(data) + getReferenceAlleleLength(data) - 1;
  }

  @Override protected Integer convertUnknown(VariantContext data) {
    throw new IllegalStateException("There is no legal implementation for the UNKNOWN mutation type");
  }

}
