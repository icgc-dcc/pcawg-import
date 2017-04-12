package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;

public final class ChromosomeStartConverterStrategy extends VariantConverterTemplate<Integer, VariantContext> {

  @Override protected Integer convertDeletion(VariantContext variant) {
    return getStart(variant)+1;
  }

  @Override protected Integer convertInsertion(VariantContext variant) {
    return getStart(variant)+1;
  }

  @Override protected Integer convertSnv(VariantContext variant) {
    return getStart(variant);
  }

  @Override protected Integer convertMnv(VariantContext variant) {
    return getStart(variant);
  }

  @Override protected Integer convertUnknown(VariantContext variant) {
    throw new IllegalStateException("There is no legal implementation for the UNKNOWN mutation type");
  }

}
