package org.icgc.dcc.pcawg.client.utils;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor
public enum CompareState {
  LT(i -> i < 0, -1),
  EQ(i -> i == 0, 0),
  GT(i -> i > 0, 1);

  @Getter(PRIVATE)
  private final Predicate<Integer> predicate;

  @Getter
  private final int value;

  public static <T extends Comparable<T>> CompareState getState(T t1, T t2){
    return getState(t1.compareTo(t2));
  }

  public static <T> CompareState getState(Comparator<T>  comparator,  T t1, T t2){
    return getState(comparator.compare(t1, t2));
  }

  public static CompareState getState(int i){
    for (val state : values()){
      if (state.getPredicate().test(i)){
        return state;
      }
    }
    throw new IllegalStateException("Cannot have no state");
  }

  public static <T extends  Comparable<T>, A extends  Comparable<A>> List<CompareState>
  getStateArray(Iterable<Function<T, A>> functionList, T t1, T t2){
    val outList = ImmutableList.<CompareState>builder();
    for (val function : functionList){
      A value1 = function.apply(t1);
      A value2 = function.apply(t2);
      val compareState = CompareState.getState(value1, value2);
      outList.add(compareState);
    }
    return outList.build();
  }

  public static <T extends  Comparable<T>, A extends  Comparable<A>> CompareState getState(Iterable<Function<T,A>> functions, T t1, T t2){
    val flist = getStateArray(functions, t1, t2);
    val firstDifferentOptional = flist.stream().filter(x -> x != EQ).findFirst();
    return firstDifferentOptional.orElse(EQ);
  }

}
