package org.icgc.dcc.pcawg.client.core.writer;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class FileWriterContextPojo implements FileWriterContext {

  @NonNull private final String outputFilename;
  private final boolean append;
  @NonNull private final String hostname;
  @NonNull private final String port;

}


