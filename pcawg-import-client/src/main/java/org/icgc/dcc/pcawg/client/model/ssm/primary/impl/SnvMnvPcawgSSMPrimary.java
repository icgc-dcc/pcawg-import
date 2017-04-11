package org.icgc.dcc.pcawg.client.model.ssm.primary.impl;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.SINGLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED_ERROR;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.SNV_MNV_REF_ALT_LENGTH_NOT_EQUAL_ERROR;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.SNV_MNV_REF_LENGTH_LT_1_ERROR;

@Slf4j
public class SnvMnvPcawgSSMPrimary extends AbstractPcawgSSMPrimaryBase {


  public static final SnvMnvPcawgSSMPrimary newSnvMnvSSMPrimary(final VariantContext variant, final String analysisId, final String analyzedSampleId)  {
    return new SnvMnvPcawgSSMPrimary(variant, analysisId, analyzedSampleId);
  }

  private final MutationTypes mutationType;

  public SnvMnvPcawgSSMPrimary(VariantContext variant, String analysisId, String analyzedSampleId) {
    super(variant, analysisId, analyzedSampleId);
    this.mutationType = calcMutationType(variant);
  }

  private static MutationTypes calcMutationType(VariantContext variant){
    val refLength = getReferenceAlleleLength(variant);
    val altLength = getFirstAlternativeAlleleLength(variant);

    if (refLength != altLength){
      val message = String.format("The MutationType cannot be found since ReferenceAlleleLength[%s] != AlternativeAlleleLength[%s]", refLength, altLength);
      throw new PcawgVariantException(message, variant, SNV_MNV_REF_ALT_LENGTH_NOT_EQUAL_ERROR);
    }

    if(refLength == 1){
      return SINGLE_BASE_SUBSTITUTION;
    } else if(refLength > 1){
      return MULTIPLE_BASE_SUBSTITUTION;
    } else {
      val message = String.format("The MutationType cannot be found since ReferenceAlleleLength[%s] < 1", refLength);
      throw new PcawgVariantException(message, variant, SNV_MNV_REF_LENGTH_LT_1_ERROR);
    }

  }

  @Override
  public String getMutationType()  {
    if (mutationType == UNKNOWN){
      val message = String.format("The MutationType [%s] is not supported for getMutationType", mutationType.name());
      throw new PcawgVariantException(message, getVariant(), MUTATION_TYPE_NOT_SUPPORTED_ERROR);
    } else {
      return mutationType.toString();
    }
  }

  @Override
  public int getChromosomeStart() {
    return getStart(getVariant());
  }

  @Override
  public int getChromosomeEnd() {
    val v = getVariant();
    return getStart(v) + getReferenceAlleleLength(v) - 1;
  }

  @Override
  public String getReferenceGenomeAllele() {
    return getReferenceAlleleString(getVariant());
  }

  @Override
  public String getControlGenotype() {
    return joinAlleles(getReferenceAlleleString(getVariant()),getReferenceGenomeAllele());
  }

  @Override
  public String getMutatedFromAllele() {
    return getReferenceAlleleString(getVariant());
  }

  /**
   * TODO: Assumption is there there is ONLY ONE alternative allele.
   * @throws IllegalStateException for when there is more than one alternative allele
   */
  @Override
  public String getTumorGenotype() {
    val v = getVariant();
    return joinAlleles(getReferenceAlleleString(v), getFirstAlternativeAlleleString(v));
  }


  /**
   * TODO: Assumption is there there is ONLY ONE alternative allele.
   * @throws IllegalStateException for when there is more than one alternative allele
   */
  @Override
  public String getMutatedToAllele() {
    return getFirstAlternativeAlleleString(getVariant());
  }

}
