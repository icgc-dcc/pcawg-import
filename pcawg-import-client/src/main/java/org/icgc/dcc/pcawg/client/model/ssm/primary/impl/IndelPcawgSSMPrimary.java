package org.icgc.dcc.pcawg.client.model.ssm.primary.impl;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import static org.icgc.dcc.pcawg.client.vcf.VCF.getAlternativeAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAllele;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAllele;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getStart;
import static org.icgc.dcc.pcawg.client.vcf.VCF.removeFirstBase;

@Slf4j
public class IndelPcawgSSMPrimary extends AbstractPcawgSSMPrimaryBase {


  public static final IndelPcawgSSMPrimary newIndelSSMPrimary(final VariantContext variant, final String analysisId, final String analyzedSampleId)  {
    return new IndelPcawgSSMPrimary(variant, analysisId, analyzedSampleId);
  }

  private final MutationTypes mutationType;
  private int refLength;
  private int altLength;

  public IndelPcawgSSMPrimary(VariantContext variant, String analysisId, String analyzedSampleId) {
    super(variant, analysisId, analyzedSampleId);
    this.mutationType = calcMutationType();
  }

  private MutationTypes calcMutationType(){
    val v = getVariant();
    this.refLength = getReferenceAlleleLength(v);
    this.altLength = getAlternativeAlleleLength(v);
    if(altLength >refLength){
      return MutationTypes.INSERTION_LTE_200BP;
    } else if(altLength < refLength) {
      return MutationTypes.DELETION_LTE_200BP;
    } else {
      val message = String.format("The MutationType cannot be found since ReferenceAlleleLength[%s] == AlternativeAlleleLength[%s]",
          refLength, altLength);
      throw new PcawgVariantException(message, getVariant(), PcawgVariantErrors.INDEL_MATCHING_REF_ALT_LENGTH_ERROR);
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
    return getStart(getVariant())+1;
  }

  @Override
  public int getChromosomeEnd() {
    val v = getVariant();
    if (mutationType == MutationTypes.INSERTION_LTE_200BP){
      return getStart(v)+1;
    } else if (mutationType == MutationTypes.DELETION_LTE_200BP){
      return getStart(v)+getReferenceAlleleLength(v)-1;
    }
    val message = String.format("The MutationType [%s] is not supported for getChormosomeEnd", mutationType.name());
    throw new PcawgVariantException(message, getVariant(), PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED);
  }

  @Override
  public String getReferenceGenomeAllele() {
    return getValueBasedOnMutationType("-", getStrippedReferenceAlleleString());
  }

  @Override
  public String getControlGenotype() {
    val allele = getStrippedReferenceAlleleString();
    return getValueBasedOnMutationType(
        joinAlleles("-","-"),
        joinAlleles(allele, allele));
  }

  private String getStrippedReferenceAlleleString(){
    return removeFirstBase(getReferenceAllele(getVariant()));
  }

  /**
   * TODO: Assumption is there there is ONLY ONE alternative allele.
   * @throws IllegalStateException for when there is more than one alternative allele
   */
  private String getStrippedAlternativeAlleleString(){
    val variant = getVariant();
    val firstAllele = getFirstAlternativeAllele(variant);
    return removeFirstBase(firstAllele);
  }

  @Override
  public String getMutatedFromAllele() {
    return getValueBasedOnMutationType("-", getStrippedReferenceAlleleString());
  }

  @Override
  public String getTumorGenotype() {
    return getValueBasedOnMutationType(
        joinAlleles("-", getStrippedAlternativeAlleleString()),
        joinAlleles(getStrippedReferenceAlleleString(),"-" ));
  }

  @Override
  public String getMutatedToAllele() {
    return getValueBasedOnMutationType(getStrippedAlternativeAlleleString(), "-");
  }

  private String getValueBasedOnMutationType(String insertionOption, String deletionOption){
    if (mutationType == MutationTypes.INSERTION_LTE_200BP){
      return insertionOption;
    } else if (mutationType == MutationTypes.DELETION_LTE_200BP){
      return deletionOption;
    }
    val message = String.format("The MutationType [%s] is not supported for getValueBasedOnMutationType", mutationType.name());
    throw new PcawgVariantException(message, getVariant(), PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED);
  }

}
