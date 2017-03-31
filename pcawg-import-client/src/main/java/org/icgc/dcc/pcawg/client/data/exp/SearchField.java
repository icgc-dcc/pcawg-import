package org.icgc.dcc.pcawg.client.data.exp;

public interface SearchField<T> {

  T get();
  boolean isWildcard();
}
