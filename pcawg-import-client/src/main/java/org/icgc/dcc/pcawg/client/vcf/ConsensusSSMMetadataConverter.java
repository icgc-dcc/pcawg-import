package org.icgc.dcc.pcawg.client.vcf;

import htsjdk.variant.vcf.VCFFileReader;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilter;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;

import java.io.File;
import java.util.Collection;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata.newSSMMetadataImpl;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVariantConverter.calcAnalysisId;

@RequiredArgsConstructor
public class ConsensusSSMMetadataConverter {

  public static ConsensusSSMMetadataConverter newConsensusSSMMetadataConverter(File vcfFile, SampleMetadata sampleMetadata,
      VariantFilterFactory variantFilterFactory, ConsensusVariantConverter consensusVariantConverter ){
    val vcf = VCF.newDefaultVCFFileReader(vcfFile);
    val variantFilter = variantFilterFactory.createVariantFilter(vcf, sampleMetadata.isUsProject());
    return new ConsensusSSMMetadataConverter(vcf, sampleMetadata, variantFilter, consensusVariantConverter);
  }

  @NonNull private final VCFFileReader vcf;
  @NonNull private final SampleMetadata sampleMetadata;
  @NonNull private final VariantFilter variantFilter;
  @NonNull private final ConsensusVariantConverter consensusVariantConverter;

  public Set<DccTransformerContext<SSMMetadata>> convert(){
    return stream(vcf)
        .filter(variantFilter::isNotFiltered)
        .map(consensusVariantConverter::getSSMClassificationSet)
        .flatMap(Collection::stream)
        .collect(toSet())
        .stream()
        .map(x -> newDccTransformerContext(x, newSSMMetadata(sampleMetadata, x.getWorkflowType(), x.getDataType())))
        .collect(toImmutableSet());
  }

  public static SSMMetadata newSSMMetadata(SampleMetadata sampleMetadata, WorkflowTypes workflowType,DataTypes dataType){
    val analysisId = calcAnalysisId(sampleMetadata.getDccProjectCode(), workflowType, dataType);
    return newSSMMetadataImpl(
        VariationCallingAlgorithms.get(workflowType, dataType),
        sampleMetadata.getMatchedSampleId(),
        analysisId,
        sampleMetadata.getAnalyzedSampleId(),
        sampleMetadata.isUsProject(),
        sampleMetadata.getAliquotId(),
        sampleMetadata.getAnalyzedFileId(),
        sampleMetadata.getMatchedFileId());
  }

}
