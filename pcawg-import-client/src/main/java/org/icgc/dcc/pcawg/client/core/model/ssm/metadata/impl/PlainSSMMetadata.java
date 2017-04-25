package org.icgc.dcc.pcawg.client.core.model.ssm.metadata.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.core.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.core.types.DataTypes;
import org.icgc.dcc.pcawg.client.core.types.WorkflowTypes;

@Builder
@Value
public class PlainSSMMetadata implements SSMMetadata {

  public static PlainSSMMetadataBuilder builderWith(SSMMetadata ssmMetadata){
    return PlainSSMMetadata.builder()
      .analysisId                  (ssmMetadata.getAnalysisId()								  )
      .analyzedSampleId            (ssmMetadata.getAnalyzedSampleId()					  )
      .study                       (ssmMetadata.getStudy()									)
      .matchedSampleId             (ssmMetadata.getMatchedSampleId()						)
      .assemblyVersion             (ssmMetadata.getAssemblyVersion()						)
      .platform                    (ssmMetadata.getPlatform()									  )
      .experimentalProtocol        (ssmMetadata.getExperimentalProtocol()			  )
      .baseCallingAlgorithm        (ssmMetadata.getBaseCallingAlgorithm()			  )
      .alignmentAlgorithm          (ssmMetadata.getAlignmentAlgorithm()				  )
      .variationCallingAlgorithm   (ssmMetadata.getVariationCallingAlgorithm()	)
      .otherAnalysisAlgorithm      (ssmMetadata.getOtherAnalysisAlgorithm()		  )
      .sequencingStrategy          (ssmMetadata.getSequencingStrategy()				  )
      .seqCoverage                 (ssmMetadata.getSeqCoverage()								)
      .rawDataRepository           (ssmMetadata.getRawDataRepository()					)
      .workflowType                (ssmMetadata.getWorkflowType())
      .dccProjectCode              (ssmMetadata.getDccProjectCode())
      .dataType                    (ssmMetadata.getDataType())
      .rawDataAccession            (ssmMetadata.getRawDataAccession()					  );
  }

  @NonNull private final String analysisId;
  @NonNull private final String analyzedSampleId;
  @NonNull private final String study;
  @NonNull private final String matchedSampleId;
  @NonNull private final String assemblyVersion;
  @NonNull private final String platform;
  @NonNull private final String experimentalProtocol;
  @NonNull private final String baseCallingAlgorithm;
  @NonNull private final String alignmentAlgorithm;
  @NonNull private final String variationCallingAlgorithm;
  @NonNull private final String otherAnalysisAlgorithm;
  @NonNull private final String sequencingStrategy;
  @NonNull private final String seqCoverage;
  @NonNull private final String rawDataRepository;
  @NonNull private final String rawDataAccession;
  @NonNull private final String dccProjectCode;
  @NonNull private final WorkflowTypes workflowType;
  @NonNull private final DataTypes dataType;

  public String getStudy(){
    return this.study;
  }

}
