package org.icgc.dcc.pcawg.client.vcf.converters2;

import htsjdk.variant.variantcontext.VariantContext;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;

public class VariantConverterStrategyMux {
  private static final DeletionVariantConverterStrategy DELETION_VARIANT_CONVERTER_STRATEGY = new DeletionVariantConverterStrategy();
  private static final InsertionVariantConverterStrategy INSERTION_VARIANT_CONVERTER_STRATEGY = new InsertionVariantConverterStrategy();
  private static final SnvVariantConverterStrategy SNV_VARIANT_CONVERTER_STRATEGY = new SnvVariantConverterStrategy();
  private static final MnvVariantConverterStrategy MNV_VARIANT_CONVERTER_STRATEGY = new MnvVariantConverterStrategy();

  public VariantConverterStrategy<VariantContext> select(MutationTypes mutationType){
    switch(mutationType){
      case DELETION_LTE_200BP:
        return DELETION_VARIANT_CONVERTER_STRATEGY;
      case INSERTION_LTE_200BP:
        return INSERTION_VARIANT_CONVERTER_STRATEGY;
      case SINGLE_BASE_SUBSTITUTION:
        return SNV_VARIANT_CONVERTER_STRATEGY;
      case MULTIPLE_BASE_SUBSTITUTION:
        return MNV_VARIANT_CONVERTER_STRATEGY;
      default:
        throw new IllegalStateException(String.format("Unimplemented MutationType[%s] = %s",
            mutationType.name(), mutationType.toString()));
    }
  }

}
