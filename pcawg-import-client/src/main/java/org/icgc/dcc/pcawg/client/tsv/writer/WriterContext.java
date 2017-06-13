package org.icgc.dcc.pcawg.client.tsv.writer;

public interface WriterContext<T>{

  T getPath();

  boolean isAppend();

}
