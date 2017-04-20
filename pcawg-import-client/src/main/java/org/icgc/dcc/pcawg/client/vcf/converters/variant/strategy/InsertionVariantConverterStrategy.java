package org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy;

import htsjdk.variant.variantcontext.VariantContext;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStrippedFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.joinAlleles;

public class InsertionVariantConverterStrategy implements VariantConverterStrategy<VariantContext> {

  private static final String INSERTION_RESULT = joinAlleles(EMPTY_ALLELE_STRING, EMPTY_ALLELE_STRING);

  @Override public int convertChromosomeEnd(VariantContext variantContext) {
    return getStart(variantContext) + 1;
  }

  @Override public int convertChromosomeStart(VariantContext variantContext) {
    return getStart(variantContext)+1;
  }

  @Override public String convertControlGenotype(VariantContext variantContext) {
    return INSERTION_RESULT;
  }

  @Override public String convertMutatedFromAllele(VariantContext variantContext) {
    return EMPTY_ALLELE_STRING;
  }

  @Override public String convertMutatedToAllele(VariantContext variantContext) {
    return getStrippedFirstAlternativeAlleleString(variantContext);
  }

  @Override public String convertReferenceGenomeAllele(VariantContext variantContext) {
    return EMPTY_ALLELE_STRING;
  }

  @Override public String convertTumorGenotype(VariantContext variantContext) {
    return joinAlleles(EMPTY_ALLELE_STRING, getFirstAlternativeAlleleString(variantContext));
  }

}
