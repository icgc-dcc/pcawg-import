package org.icgc.dcc.pcawg.client.data.metadata;

import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;

public interface SampleMetadataDAO  {

  SampleMetadata fetchSampleMetadata(PortalFilename portalFilename) throws SampleMetadataNotFoundException;

}
