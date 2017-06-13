package org.icgc.dcc.pcawg.client.utils;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Iterator;
import java.util.List;

@RequiredArgsConstructor
public class PartitioningIterator<I> implements Iterator<List<I>> {

  @NonNull
  private final List<I> data;

  private final int fetchSize;

  private int count = 0;

  @Override public boolean hasNext() {
    return count <= data.size() - 1;
  }

  @Override public List<I> next() {
    int candidateEnd = count + fetchSize;
    int size = data.size();
    int end = candidateEnd >= size ? size - 1 : candidateEnd;
    val out = ImmutableList.copyOf(data.subList(count, end));
    count += fetchSize;
    return out;
  }

}
