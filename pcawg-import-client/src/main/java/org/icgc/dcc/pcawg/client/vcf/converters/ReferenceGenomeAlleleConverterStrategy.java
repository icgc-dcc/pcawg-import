package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStrippedReferenceAlleleString;

public final class ReferenceGenomeAlleleConverterStrategy extends VariantConverterTemplate<String, VariantContext> {

  @Override protected String convertDeletion(VariantContext data) {
    return getStrippedReferenceAlleleString(data);
  }

  @Override protected String convertInsertion(VariantContext data) {
    return EMPTY_ALLELE_STRING;
  }

  @Override protected String convertSnv(VariantContext data) {
    return getReferenceAlleleString(data);
  }

  @Override protected String convertMnv(VariantContext data) {
    return getReferenceAlleleString(data);
  }

  @Override protected String convertUnknown(VariantContext variant) {
    throw new IllegalStateException("There is no legal implementation for the UNKNOWN mutation type");
  }

}
