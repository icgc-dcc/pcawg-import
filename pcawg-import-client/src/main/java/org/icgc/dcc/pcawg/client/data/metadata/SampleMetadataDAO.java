package org.icgc.dcc.pcawg.client.data.metadata;

import org.icgc.dcc.pcawg.client.model.portal.PortalFilename;

public interface SampleMetadataDAO  {

  SampleMetadata fetchSampleMetadata(PortalFilename portalFilename) throws SampleMetadataNotFoundException;

}
