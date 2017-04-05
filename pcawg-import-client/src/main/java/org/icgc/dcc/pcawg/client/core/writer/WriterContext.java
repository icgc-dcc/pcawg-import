package org.icgc.dcc.pcawg.client.core.writer;

public interface WriterContext<T>{

  T getPath();

  boolean isAppend();

}
