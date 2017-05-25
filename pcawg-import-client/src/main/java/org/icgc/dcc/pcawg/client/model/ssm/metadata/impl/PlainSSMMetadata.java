package org.icgc.dcc.pcawg.client.model.ssm.metadata.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;

@Builder
@Value
public class PlainSSMMetadata implements SSMMetadata {

  public static PlainSSMMetadataBuilder builderWith(SSMMetadata ssmMetadata){
    return PlainSSMMetadata.builder()
      .analysisId                  (ssmMetadata.getAnalysisId()								  )
      .analyzedSampleId            (ssmMetadata.getAnalyzedSampleId()					  )
      .pcawgFlag                   (ssmMetadata.getPcawgFlag()									)
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
      .rawDataAccession            (ssmMetadata.getRawDataAccession()					  );
  }

  @NonNull private final String   analysisId;
  @NonNull private final String   analyzedSampleId;
           private final boolean  pcawgFlag;
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

  public boolean getPcawgFlag(){
    return this.pcawgFlag;
  }


}
