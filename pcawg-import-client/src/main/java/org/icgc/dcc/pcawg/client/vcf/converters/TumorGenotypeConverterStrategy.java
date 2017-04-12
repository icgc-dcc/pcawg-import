package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStrippedReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.joinAlleles;

public final class TumorGenotypeConverterStrategy extends VariantConverterTemplate<String, VariantContext> {

  @Override protected String convertDeletion(VariantContext data) {
    return joinAlleles(getStrippedReferenceAlleleString(data), EMPTY_ALLELE_STRING);
  }

  @Override protected String convertInsertion(VariantContext data) {
    return joinAlleles(EMPTY_ALLELE_STRING, getFirstAlternativeAlleleString(data));
  }

  @Override protected String convertSnv(VariantContext data) {
    return joinAlleles(getReferenceAlleleString(data), getFirstAlternativeAlleleString(data));
  }

  @Override protected String convertMnv(VariantContext data) {
    return joinAlleles(getReferenceAlleleString(data), getFirstAlternativeAlleleString(data));
  }

  @Override protected String convertUnknown(VariantContext variant) {
    throw new IllegalStateException("There is no legal implementation for the UNKNOWN mutation type");
  }

}
