package org.icgc.dcc.pcawg.client.vcf.converters.file;

import lombok.val;
import org.icgc.dcc.pcawg.client.tsv.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.types.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.VariationCallingAlgorithms;
import org.icgc.dcc.pcawg.client.model.types.WorkflowTypes;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.tsv.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata.newPcawgSSMMetadata;

public class MetadataDTCConverter {

  public static MetadataDTCConverter newMetadataDTCConverter(){
    return new MetadataDTCConverter();
  }

  private Set<SSMMetadata> ssmMetadataSet = newHashSet();

  public void resetState(){
    ssmMetadataSet = newHashSet();
  }

  public SSMPrimary accumulateSSMPrimary(SampleMetadata sampleMetadata, SSMPrimary ssmPrimary){
    val ssmMetadata = buildWorkflowSpecificSSMMetadata(ssmPrimary.getWorkflowType(), sampleMetadata, ssmPrimary.getDataType());
    ssmMetadataSet.add(ssmMetadata);
    return ssmPrimary;
  }

  public DccTransformerContext<SSMPrimary> accumulatePrimaryDTC(SampleMetadata sampleMetadata, DccTransformerContext<SSMPrimary> input){
    val workflowType = input.getWorkflowTypes();
    val dataType = input.getObject().getDataType();
    val ssmMetadata = buildWorkflowSpecificSSMMetadata(workflowType, sampleMetadata, dataType );
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

}

