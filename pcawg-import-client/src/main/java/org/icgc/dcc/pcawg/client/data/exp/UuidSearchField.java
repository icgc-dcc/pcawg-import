package org.icgc.dcc.pcawg.client.data.exp;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public class UuidSearchField implements SearchField<String> {

  public static UuidSearchField newUuidSearchField(String value) {
    return new UuidSearchField(StringSearchField.newStringSearchField(value.toLowerCase()));
  }

  public static UuidSearchField newWildUuidSearchField() {
    return new UuidSearchField(StringSearchField.newWildStringSearchField());
  }

  @NonNull
  private final StringSearchField internalField;

  @Override public String get() {
    return internalField.get();
  }

  @Override public boolean isWildcard() {
    return internalField.isWildcard();
  }
}
