package org.icgc.dcc.pcawg.client.core.writer2.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.pcawg.client.core.writer2.WriterContext;

@Builder
@Value
public final class HadoopWriterContext implements WriterContext<Path> {

  @NonNull private final Path path;
  private final boolean append;

}
