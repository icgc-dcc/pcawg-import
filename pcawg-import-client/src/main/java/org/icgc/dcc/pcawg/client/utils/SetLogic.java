package org.icgc.dcc.pcawg.client.utils;

import com.google.common.collect.Sets;
import lombok.NoArgsConstructor;
import lombok.val;

import java.util.Set;

import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class SetLogic {

  public static <T> Set<T> missingFromActual(Set<T> actualSet, Set<T> expectedSet){
    val inter = Sets.intersection(actualSet, expectedSet);
    return Sets.difference(expectedSet, inter);
  }

  public static <T> Set<T> extraInActual(Set<T> actualSet, Set<T> expectedSet){
    val inter = Sets.intersection(actualSet, expectedSet);
    return Sets.difference(actualSet, inter);
  }

}
