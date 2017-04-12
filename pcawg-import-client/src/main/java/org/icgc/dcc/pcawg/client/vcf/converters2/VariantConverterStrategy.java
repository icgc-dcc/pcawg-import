package org.icgc.dcc.pcawg.client.vcf.converters2;

public interface VariantConverterStrategy<T> {

  String EMPTY_ALLELE_STRING = "-";

  int convertChromosomeEnd(T t);
  int convertChromosomeStart(T t);
  String convertControlGenotype(T t);
  String convertMutatedFromAllele(T t);
  String convertMutatedToAllele(T t);
  String convertReferenceGenomeAllele(T t);
  String convertTumorGenotype(T t);

}
