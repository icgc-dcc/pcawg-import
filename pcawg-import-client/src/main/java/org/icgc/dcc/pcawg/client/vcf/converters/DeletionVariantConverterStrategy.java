package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStrippedReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.joinAlleles;

public class DeletionVariantConverterStrategy implements VariantConverterStrategy<VariantContext>{

  @Override public int convertChromosomeEnd(VariantContext variantContext) {
    return getStart(variantContext) + getReferenceAlleleLength(variantContext) - 1;
  }

  @Override public int convertChromosomeStart(VariantContext variantContext) {
    return getStart(variantContext)+1;
  }

  @Override public String convertControlGenotype(VariantContext variantContext) {
    val strippedRef = getStrippedReferenceAlleleString(variantContext);
    return joinAlleles(strippedRef, strippedRef);
  }

  @Override public String convertMutatedFromAllele(VariantContext variantContext) {
    return getStrippedReferenceAlleleString(variantContext);
  }

  @Override public String convertMutatedToAllele(VariantContext variantContext) {
    return EMPTY_ALLELE_STRING;
  }

  @Override public String convertReferenceGenomeAllele(VariantContext variantContext) {
    return getStrippedReferenceAlleleString(variantContext);
  }

  @Override public String convertTumorGenotype(VariantContext variantContext) {
    return joinAlleles(getStrippedReferenceAlleleString(variantContext), EMPTY_ALLELE_STRING);
  }

}
