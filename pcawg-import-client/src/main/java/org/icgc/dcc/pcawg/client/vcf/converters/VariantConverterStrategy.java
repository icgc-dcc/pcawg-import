package org.icgc.dcc.pcawg.client.vcf.converters;

import org.icgc.dcc.pcawg.client.vcf.MutationTypes;

public interface VariantConverterStrategy<T, D> {

  T convert(MutationTypes mutationType, D data);
}
