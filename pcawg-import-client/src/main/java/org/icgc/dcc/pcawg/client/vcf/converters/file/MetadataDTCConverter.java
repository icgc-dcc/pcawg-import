package org.icgc.dcc.pcawg.client.vcf.converters.file;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.classification.SSMClassification;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.VariationCallingAlgorithms;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.model.ssm.classification.impl.SSMMetadataClassification.newSSMMetadataClassification;
import static org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata.newSSMMetadataImpl;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantConverter.calcAnalysisId;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantConverter.extractWorkflowTypes;

public class MetadataDTCConverter {

  public static MetadataDTCConverter newMetadataDTCConverter(){
    return new MetadataDTCConverter();
  }

  private Map<SampleMetadata, Set<SSMClassification>> map = newHashMap();

  public void resetState(){
    map = newHashMap();
  }

  public VariantContext accumulateVariants(SampleMetadata consensusSampleMetadata,VariantContext variant){
    if (!map.containsKey(consensusSampleMetadata)){
      map.put(consensusSampleMetadata, newHashSet());
    }
    val mutationType = MutationTypes.resolveMutationType(variant);

    val ssmClassificationSet = extractWorkflowTypes(variant).stream()
        .map(w -> newSSMMetadataClassification(w, mutationType))
        .collect(toSet());
    ssmClassificationSet.add(newSSMMetadataClassification(consensusSampleMetadata.getWorkflowType(), mutationType));

    map.get(consensusSampleMetadata).addAll(ssmClassificationSet);
    return variant;
  }

  public Set<DccTransformerContext<SSMMetadata>> convert(){
    return map.entrySet().stream()
        .flatMap(e -> e.getValue()
            .stream()
            .map(s -> createMetadataDTC(s, e.getKey())))
        .collect(toImmutableSet());
  }

  private static DccTransformerContext<SSMMetadata> createMetadataDTC(SSMClassification ssmClassification, SampleMetadata sampleMetadata){
    val dataType = ssmClassification.getDataType();
    val workflowType = ssmClassification.getWorkflowType();
    val generatedSampleMetadata = SampleMetadata.builderWith(sampleMetadata)
        .workflowType(workflowType)
        .build();
    val ssmMetadata = newSSMMetadata(generatedSampleMetadata, dataType);
    return newDccTransformerContext(ssmClassification, ssmMetadata);
  }

  private static SSMMetadata newSSMMetadata(SampleMetadata sampleMetadata, DataTypes dataType){
    val workflowType = sampleMetadata.getWorkflowType();
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

