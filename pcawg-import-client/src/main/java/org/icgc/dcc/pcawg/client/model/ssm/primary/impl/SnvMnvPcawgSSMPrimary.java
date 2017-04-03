package org.icgc.dcc.pcawg.client.model.ssm.primary.impl;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleString;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;

@Slf4j
public class SnvMnvPcawgSSMPrimary extends AbstractPcawgSSMPrimaryBase {


  public static final SnvMnvPcawgSSMPrimary newSnvMnvSSMPrimary(final VariantContext variant, final String analysisId, final String analyzedSampleId)  {
    return new SnvMnvPcawgSSMPrimary(variant, analysisId, analyzedSampleId);
  }

  private final MutationTypes mutationType;

  public SnvMnvPcawgSSMPrimary(VariantContext variant, String analysisId, String analyzedSampleId) {
    super(variant, analysisId, analyzedSampleId);
    this.mutationType = calcMutationType();
  }

  private MutationTypes calcMutationType(){
    val refLength = getReferenceAlleleLength(getVariant());
    if(refLength == 1){
      return MutationTypes.SINGLE_BASE_SUBSTITUTION;
    } else if(refLength > 1){
      return MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
    } else {
      val message = String.format("The MutationType cannot be found since ReferenceAlleleLength[%s] < 1", refLength);
      throw new PcawgVariantException(message, getVariant(), PcawgVariantErrors.SNV_MNV_REF_LENGTH_LT_1);
    }
  }

  @Override
  public String getMutationType()  {
    if (mutationType == MutationTypes.UNKNOWN){
      val message = String.format("The MutationType [%s] is not supported for getMutationType", mutationType.name());
      throw new PcawgVariantException(message, getVariant(), PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED);
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
