package org.icgc.dcc.pcawg.client.data;

import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;

public interface SampleMetadataDAO  {

  static class SampleMetadataNotFoundException extends Exception{
    public SampleMetadataNotFoundException(String message) {
      super(message);
    }
  }

  static boolean isUSProject(String projectCode){
    return projectCode.matches("^.*-US$");
  }

  SampleMetadata fetchSampleMetadata(PortalFilename portalFilename) throws SampleMetadataNotFoundException;
}
