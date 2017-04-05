package org.icgc.dcc.pcawg.client.core.transformer;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

public interface Transformer<T> extends Closeable, Flushable {

  void transform(T t) throws IOException;

}
