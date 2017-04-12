package org.icgc.dcc.pcawg.client.vcf.converters;

import org.icgc.dcc.pcawg.client.vcf.MutationTypes;

public abstract class VariantConverterTemplate<T, D> implements VariantConverterStrategy<T, D> {

  public static final String EMPTY_ALLELE_STRING = "-";

  @Override public T convert(MutationTypes mutationType, D data){
    switch(mutationType){
    case DELETION_LTE_200BP:
      return convertDeletion(data);
    case INSERTION_LTE_200BP:
      return convertInsertion(data);
    case SINGLE_BASE_SUBSTITUTION:
      return convertSnv(data);
    case MULTIPLE_BASE_SUBSTITUTION:
      return convertMnv(data);
    case UNKNOWN:
      return convertUnknown(data);
    default:
      throw new IllegalStateException(String.format("Unimplemented MutationType[%s] = %s",
          mutationType.name(), mutationType.toString()));
    }

  }

  protected abstract T convertDeletion(D data);
  protected abstract T convertInsertion(D data);
  protected abstract T convertSnv(D data);
  protected abstract T convertMnv(D data);
  protected abstract T convertUnknown(D data);



}
