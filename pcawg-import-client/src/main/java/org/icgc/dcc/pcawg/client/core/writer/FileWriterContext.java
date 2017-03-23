package org.icgc.dcc.pcawg.client.core.writer;

public interface FileWriterContext {

  String getOutputFilename();

  boolean isAppend();

  String getHostname();

  String getPort();
}
