package org.icgc.dcc.pcawg.client.vcf.converters.file;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.ConsensusSampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.VariationCallingAlgorithms;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata.newPcawgSSMMetadata;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantConverter.extractWorkflowTypes;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantConverter.resolveDataType;

public class MetadataDTCConverter {

  public static MetadataDTCConverter newMetadataDTCConverter(){
    return new MetadataDTCConverter();
  }

  private Set<SSMMetadata> ssmMetadataSet = newHashSet();

  public void resetState(){
    ssmMetadataSet = newHashSet();
  }

  @Deprecated
  public VariantContext accumulateVariants(ConsensusSampleMetadata consensusSampleMetadata,VariantContext variant){
    ssmMetadataSet.addAll(convertSSMMetadata(consensusSampleMetadata, variant));
    return variant;
  }

  public SSMPrimary accumulateSSMPrimary(ConsensusSampleMetadata consensusSampleMetadata, SSMPrimary ssmPrimary){
    val ssmMetadata = buildWorkflowSpecificSSMMetadata(ssmPrimary.getWorkflowType(), consensusSampleMetadata, ssmPrimary.getDataType());
    ssmMetadataSet.add(ssmMetadata);
    return ssmPrimary;
  }

  public DccTransformerContext<SSMPrimary> accumulatePrimaryDTC(ConsensusSampleMetadata consensusSampleMetadata, DccTransformerContext<SSMPrimary> input){
    val workflowType = input.getWorkflowTypes();
    val dataType = input.getObject().getDataType();
    val ssmMetadata = buildWorkflowSpecificSSMMetadata(workflowType, consensusSampleMetadata, dataType );
    ssmMetadataSet.add(ssmMetadata);
    return input;
  }

  public Set<DccTransformerContext<SSMMetadata>> convert(){
    return ssmMetadataSet.stream()
        .map(MetadataDTCConverter::createMetadataDTC)
        .collect(toImmutableSet());
  }

  private static DccTransformerContext<SSMMetadata> createMetadataDTC(SSMMetadata ssmMetadata){
    return newDccTransformerContext(ssmMetadata.getWorkflowType(), ssmMetadata);
  }

  private static SSMMetadata buildWorkflowSpecificSSMMetadata(WorkflowTypes workflowType, SampleMetadata sampleMetadata, DataTypes dataType){
    return newPcawgSSMMetadata(
        VariationCallingAlgorithms.get(workflowType, dataType),
        sampleMetadata.getMatchedSampleId(),
        sampleMetadata.getAnalyzedSampleId(),
        sampleMetadata.isUsProject(),
        sampleMetadata.getAliquotId(),
        sampleMetadata.getAnalyzedFileId(),
        sampleMetadata.getMatchedFileId(),
        sampleMetadata.getDccProjectCode(),
        workflowType,
        dataType);

  }

  private static Set<SSMMetadata> convertSSMMetadata(ConsensusSampleMetadata consensusSampleMetadata, VariantContext variantContext){
    val dataType = resolveDataType(variantContext);
    return extractWorkflowTypes(variantContext).stream()
        .map(w -> buildWorkflowSpecificSSMMetadata(w, consensusSampleMetadata, dataType))
        .collect(toImmutableSet());

  }


}

