package org.icgc.dcc.pcawg.client.data.exp;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor
@EqualsAndHashCode
public class StringSearchField implements SearchField<String> {
  private static final String EMPTY = "";
  private static final String STAR= "*";

  public static StringSearchField newStringSearchField(String value) {
    return new StringSearchField(value);
  }
  public static StringSearchField newWildStringSearchField() {
    return new StringSearchField(STAR);
  }

  @NonNull
  private final String value;

  @Override public String get() {
    return value;
  }

  @Override public boolean isWildcard() {
      val isWild = EMPTY.equals(value) || STAR.equals(value);
      return value == null || isWild;
  }

}
