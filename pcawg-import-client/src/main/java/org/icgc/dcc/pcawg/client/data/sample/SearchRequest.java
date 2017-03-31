package org.icgc.dcc.pcawg.client.data.sample;

import lombok.val;

import java.util.function.BiPredicate;
import java.util.function.Function;

public interface SearchRequest<R extends SearchRequest> {
   String STAR = "*";
   String EMPTY = "";

   static boolean isWildcardSearch(String value){
      val isWild = EMPTY.equals(value) || STAR.equals(value);
      return value == null || isWild;
   }

   boolean matches(R request);


   static <R> boolean matchFunctions(R lr, R rr, Function<R, String> functor, BiPredicate<String, String> comparingFunctor){
      val lrValue = functor.apply(lr);
      val rrValue = functor.apply(rr);
      return isWildcardSearch(lrValue) || isWildcardSearch(rrValue) || comparingFunctor.test(lrValue, rrValue);
   }

   static boolean lowercaseAndContains(String left, String right){
      val l = left.toLowerCase();
      val r = right.toLowerCase();
      return l.contains(r) || r.contains(l);
   }

}
