package org.icgc.dcc.pcawg.client.tsv.converter.impl;

import lombok.NoArgsConstructor;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.tsv.converter.TSVConverter;

import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@NoArgsConstructor
public class SSMMetadataTSVConverter implements TSVConverter<SSMMetadata> {

  public static final SSMMetadataTSVConverter newSSMMetadataTSVConverter(){
    return new SSMMetadataTSVConverter();
  }

  @Override
  public String toTSVData(SSMMetadata ssmMetadata) {
    return stream(SSMMetadataFieldMapping.values())
        .map(x -> x.extractStringValue(ssmMetadata))
        .collect(getTsvCollectorInstance());
  }

  @Override
  public String toTSVHeader() {
    return stream(SSMMetadataFieldMapping.values())
        .map(SSMMetadataFieldMapping::toString)
        .collect(getTsvCollectorInstance());
  }

}
