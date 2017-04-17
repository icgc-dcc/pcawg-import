package org.icgc.dcc.pcawg.client.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter;

import java.nio.file.Path;

import static org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter.newConsensusVCFConverter;

@RequiredArgsConstructor
public class ConsensusVCFConverterFactory {

  public static ConsensusVCFConverterFactory newConsensusVCFConverterFactory(VariantFilterFactory variantFilterFactory){
    return new ConsensusVCFConverterFactory(variantFilterFactory);
  }

  @NonNull private final VariantFilterFactory variantFilterFactory;

  public ConsensusVCFConverter getConsensusVCFConverter(Path vcfPath, SampleMetadata consensusSampleMetadata){
    return newConsensusVCFConverter(vcfPath,consensusSampleMetadata,variantFilterFactory);
  }

}
