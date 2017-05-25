package org.icgc.dcc.pcawg.client.model.ssm.primary.impl;

import com.google.common.base.Joiner;
import htsjdk.variant.variantcontext.VariantContext;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.pcawg.client.model.NACodes;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;

import static lombok.AccessLevel.PROTECTED;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getAltCount;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getChomosome;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getRefCount;

/**
 * Common implementations for all subclasses of AbstractPcawgSSMPrimaryBase
 */
@Slf4j
public abstract class AbstractPcawgSSMPrimaryBase implements SSMPrimary {

  private static final int DEFAULT_STRAND = 1;
  private static final String DEFAULT_VERIFICATION_STATUS = "not tested";
  private static final Joiner ALLELE_JOINER = Joiners.SLASH;

  public static String joinAlleles(String ref, String alt){
    return ALLELE_JOINER.join(ref, alt);
  }


  @NonNull
  @Getter(PROTECTED)
  private final VariantContext variant;

  @NonNull
  @Getter
  private final String analysisId;

  @NonNull
  @Getter
  private final String analyzedSampleId;

  public AbstractPcawgSSMPrimaryBase(VariantContext variant, String analysisId, String analyzedSampleId) {
    this.variant = variant;
    this.analysisId = analysisId;
    this.analyzedSampleId = analyzedSampleId;
  }

  @Override
  public String getChromosome() {
    return getChomosome(variant);
  }

  @Override
  public int getChromosomeStrand() {
    return DEFAULT_STRAND;
  }

  @Override
  public String getExpressedAllele() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getQualityScore() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getProbability() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  //TODO: this is only for consensus and is baked in for now. Will need CallProcessors that implement specific ways to get the data needed, such as total_read_count, mutant_allele_read_count...etc
  @Override
  public int getTotalReadCount() {
    val altCount = getAltCount(variant);
    val refCount = getRefCount(variant);
    if (altCount.isPresent() && refCount.isPresent()){
      return altCount.get()+refCount.get();
    } else {
      return NACodes.CORRUPTED_DATA.toInt();
    }
  }

  @Override
  public int getMutantAlleleReadCount() {
    val altCount = getAltCount(variant);
    if (altCount.isPresent()){
      return altCount.get();
    } else {
      return NACodes.CORRUPTED_DATA.toInt();
    }
  }

  @Override
  public String getVerificationStatus() {
    return DEFAULT_VERIFICATION_STATUS;
  }

  @Override
  public String getVerificationPlatform() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getBiologicalValidationStatus() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getBiologicalValidationPlatform() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  @Override
  public String getNote() {
    return NACodes.DATA_VERIFIED_TO_BE_UNKNOWN.toString();
  }

  //For Andy, just a placeholder
  @Override
  public boolean getPcawgFlag() {
    return true;
  }

}

