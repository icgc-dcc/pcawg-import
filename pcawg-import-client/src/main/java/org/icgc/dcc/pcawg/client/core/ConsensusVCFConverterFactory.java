package org.icgc.dcc.pcawg.client.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.vcf.ConsensusSSMMetadataConverter;
import org.icgc.dcc.pcawg.client.vcf.ConsensusSSMPrimaryConverter;

import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusSSMMetadataConverter.newConsensusSSMMetadataConverter;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusSSMPrimaryConverter.newConsensusSSMPrimaryConverter;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVariantConverter.newConsensusVariantConverter;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;

@RequiredArgsConstructor
public class ConsensusVCFConverterFactory {

  public static ConsensusVCFConverterFactory newConsensusVCFConverterFactory(Path vcfPath,
      SampleMetadata sampleMetadata, VariantFilterFactory variantFilterFactory){
    val vcfFile = vcfPath.toFile();
    checkArgument(vcfFile.exists(), "The VCF File [{}] DNE", vcfPath.toString());
    return new ConsensusVCFConverterFactory(vcfPath, sampleMetadata,variantFilterFactory);
  }

  @NonNull private final Path vcfPath;
  @NonNull private final SampleMetadata sampleMetadata;
  @NonNull private final VariantFilterFactory variantFilterFactory;

  public ConsensusSSMPrimaryConverter getConsensusSSMPrimaryConverter(){
    val vcf = newDefaultVCFFileReader(vcfPath.toFile());
    val variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadata.isUsProject());
    val consensusVariantConverter = newConsensusVariantConverter(sampleMetadata);
    return newConsensusSSMPrimaryConverter(vcfPath, vcf, variantFilter, consensusVariantConverter);
  }

  public ConsensusSSMMetadataConverter getConsensusSSMMetadataConverter(){
    val vcf = newDefaultVCFFileReader(vcfPath.toFile());
    val variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadata.isUsProject());
    val consensusVariantConverter = newConsensusVariantConverter(sampleMetadata);
    return newConsensusSSMMetadataConverter(vcf,sampleMetadata,variantFilter, consensusVariantConverter);
  }

}
