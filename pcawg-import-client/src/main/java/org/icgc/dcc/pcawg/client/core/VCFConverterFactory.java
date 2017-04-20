package org.icgc.dcc.pcawg.client.core;

import htsjdk.variant.vcf.VCFFileReader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilter;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.vcf.ConsensusVariantConverter;
import org.icgc.dcc.pcawg.client.vcf.SSMMetadataVCFConverter;
import org.icgc.dcc.pcawg.client.vcf.SSMPrimaryVCFConverter;

import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVariantConverter.newConsensusVariantConverter;
import static org.icgc.dcc.pcawg.client.vcf.SSMMetadataVCFConverter.newSSMMetadataVCFConverter;
import static org.icgc.dcc.pcawg.client.vcf.SSMPrimaryVCFConverter.newSSMPrimaryVCFConverter;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;

@RequiredArgsConstructor
public class VCFConverterFactory {

  public static VCFConverterFactory newInitializedVCFConverterFactory(Path vcfPath,
      SampleMetadata sampleMetadata, VariantFilterFactory variantFilterFactory){
    val vcfFile = vcfPath.toFile();
    checkArgument(vcfFile.exists(), "The VCF File [{}] DNE", vcfPath.toString());
    val out = new VCFConverterFactory(vcfPath, sampleMetadata,variantFilterFactory);
    out.init();
    return out;
  }

  @NonNull private final Path vcfPath;
  @NonNull private final SampleMetadata sampleMetadata;
  @NonNull private final VariantFilterFactory variantFilterFactory;

  /**
   * State
   */
  private VCFFileReader vcf;
  private VariantFilter variantFilter;
  private ConsensusVariantConverter consensusVariantConverter;
  private boolean initialzed = false;

  public void init(){
    vcf = newDefaultVCFFileReader(vcfPath.toFile());
    variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadata.isUsProject());
    consensusVariantConverter = newConsensusVariantConverter(sampleMetadata);
    initialzed = true;
  }

  private void checkInit(){
    checkState(initialzed, "The {} was not initialized", this.getClass().getSimpleName());
  }

  public SSMPrimaryVCFConverter getConsensusSSMPrimaryConverter(){
    checkInit();
    return newSSMPrimaryVCFConverter(vcfPath, vcf, variantFilter, consensusVariantConverter);
  }

  public SSMMetadataVCFConverter getConsensusSSMMetadataConverter(){
    checkInit();
    return newSSMMetadataVCFConverter(vcf,sampleMetadata,variantFilter, consensusVariantConverter);
  }

}
