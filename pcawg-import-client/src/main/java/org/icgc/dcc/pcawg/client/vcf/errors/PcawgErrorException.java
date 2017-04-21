package org.icgc.dcc.pcawg.client.vcf.errors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class PcawgErrorException extends PcawgException {

  private static final List<Integer> EMPTY_INTEGER_LIST = ImmutableList.of();

  private final Map<PcawgVariantErrors, List<Integer>> errorMap = newHashMap();

  public PcawgErrorException(String message) {
    super(message);
  }

  public PcawgErrorException(Throwable cause) {
    super(cause);
  }

  public PcawgErrorException(String message, Throwable cause) {
    super(message, cause);
  }

  public void addError(@NonNull PcawgVariantErrors variantError, final int variantStart){
    if (!errorMap.containsKey(variantError)){
      errorMap.put(variantError, newArrayList());
    }
    val lineList = errorMap.get(variantError);
    lineList.add(variantStart);
  }

  public boolean hasErrors(){
    return !errorMap.keySet().isEmpty();
  }

  public Set<PcawgVariantErrors> getVariantErrors(){
    return ImmutableSet.copyOf(errorMap.keySet());
  }

  public List<Integer> getErrorVariantStart(PcawgVariantErrors error){
    return errorMap.getOrDefault(error, EMPTY_INTEGER_LIST);
  }
}
