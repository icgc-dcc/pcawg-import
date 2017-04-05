package org.icgc.dcc.pcawg.client.core.writer.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.core.writer.WriterContext;

import java.nio.file.Path;

@Builder
@Value
public final class LocalWriterContext implements WriterContext<Path> {

  @NonNull private final Path path;

  @NonNull private final boolean append;

}
