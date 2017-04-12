package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStrippedFirstAlternativeAlleleString;

public final class MutatedToAlleleConverterStrategy extends VariantConverterTemplate<String, VariantContext> {

  @Override protected String convertDeletion(VariantContext data) {
    return EMPTY_ALLELE_STRING;
  }

  @Override protected String convertInsertion(VariantContext data) {
    return getStrippedFirstAlternativeAlleleString(data);
  }

  @Override protected String convertSnv(VariantContext data) {
    return getFirstAlternativeAlleleString(data);
  }

  @Override protected String convertMnv(VariantContext data) {
    return getFirstAlternativeAlleleString(data);
  }

  @Override protected String convertUnknown(VariantContext variant) {
    throw new IllegalStateException("There is no legal implementation for the UNKNOWN mutation type");
  }

}
