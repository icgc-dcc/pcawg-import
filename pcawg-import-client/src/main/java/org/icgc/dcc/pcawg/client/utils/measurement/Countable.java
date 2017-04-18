package org.icgc.dcc.pcawg.client.utils.measurement;

public interface Countable<T> {

  void incr();

  void incr(T amount);

  void reset();

  T getCount();

}
