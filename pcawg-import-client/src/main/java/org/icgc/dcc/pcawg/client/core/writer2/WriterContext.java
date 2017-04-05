package org.icgc.dcc.pcawg.client.core.writer2;

public interface WriterContext<T>{

  T getPath();

  boolean isAppend();

}
