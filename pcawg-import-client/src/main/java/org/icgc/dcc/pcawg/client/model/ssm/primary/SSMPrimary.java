package org.icgc.dcc.pcawg.client.model.ssm.primary;

import org.icgc.dcc.pcawg.client.model.ssm.Common;


public interface SSMPrimary extends Common {

  String getMutationType();
  String getChromosome();
  int getChromosomeStart();
  int getChromosomeEnd();
  int getChromosomeStrand();
  String getReferenceGenomeAllele();
  String getControlGenotype();
  String getMutatedFromAllele();
  String getTumorGenotype();
  String getMutatedToAllele();
  String getExpressedAllele();
  String getQualityScore();
  String getProbability();
  int getTotalReadCount();
  int getMutantAlleleReadCount();
  String getVerificationStatus();
  String getVerificationPlatform();
  String getBiologicalValidationStatus();
  String getBiologicalValidationPlatform();
  String getNote();

}
