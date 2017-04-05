package org.icgc.dcc.pcawg.client.core.fscontroller;

import org.icgc.dcc.pcawg.client.core.writer.WriterContext;

import java.io.IOException;
import java.io.Writer;

public interface FsController<T> {

  boolean exists(T path) throws IOException;

  Writer createWriter(WriterContext<T> writerContext) throws IOException;

  void deleteIfExists(T path) throws IOException;

  void mkdirs(T path) throws IOException;

}
