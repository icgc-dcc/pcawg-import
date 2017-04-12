package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStrippedReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.joinAlleles;

public final class ControlGenotypeConverterStrategy extends VariantConverterTemplate<String, VariantContext> {

  private static final String INSERTION_RESULT = joinAlleles(EMPTY_ALLELE_STRING, EMPTY_ALLELE_STRING);

  @Override protected String convertDeletion(VariantContext data) {
    val strippedRef = getStrippedReferenceAlleleString(data);
    return joinAlleles(strippedRef, strippedRef);
  }

  @Override protected String convertInsertion(VariantContext data) {
    return INSERTION_RESULT;
  }

  @Override protected String convertSnv(VariantContext data) {
    val ref = getReferenceAlleleString(data);
    return joinAlleles(ref, ref);
  }

  @Override protected String convertMnv(VariantContext data) {
    val ref = getReferenceAlleleString(data);
    return joinAlleles(ref, ref);
  }

  @Override protected String convertUnknown(VariantContext variant) {
    throw new IllegalStateException("There is no legal implementation for the UNKNOWN mutation type");
  }

}
