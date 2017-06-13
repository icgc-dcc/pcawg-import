package org.icgc.dcc.pcawg.client.download.query;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

@RequiredArgsConstructor
public class QueryIterator<T, I> implements Iterator<T> {

  @NonNull
  private final Iterator<List<I>> internalIterator;

  @NonNull
  private final Function<List<I>, T> ctorFunctor;

  @Override public boolean hasNext() {
    return internalIterator.hasNext();
  }

  @Override public T next() {
    return ctorFunctor.apply(internalIterator.next());
  }
}
