package org.icgc.dcc.pcawg.client.vcf.converters;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.joinAlleles;

public class SnvVariantConverterStrategy implements VariantConverterStrategy<VariantContext> {

  @Override public int convertChromosomeEnd(VariantContext variantContext) {
    return getStart(variantContext) + getReferenceAlleleLength(variantContext) - 1;
  }

  @Override public int convertChromosomeStart(VariantContext variantContext) {
    return getStart(variantContext);
  }

  @Override public String convertControlGenotype(VariantContext variantContext) {
    val ref = getReferenceAlleleString(variantContext);
    return joinAlleles(ref, ref);
  }

  @Override public String convertMutatedFromAllele(VariantContext variantContext) {
    return getReferenceAlleleString(variantContext);
  }

  @Override public String convertMutatedToAllele(VariantContext variantContext) {
    return getFirstAlternativeAlleleString(variantContext);
  }

  @Override public String convertReferenceGenomeAllele(VariantContext variantContext) {
    return getReferenceAlleleString(variantContext);
  }

  @Override public String convertTumorGenotype(VariantContext variantContext) {
    return joinAlleles(getReferenceAlleleString(variantContext), getFirstAlternativeAlleleString(variantContext));
  }
}
