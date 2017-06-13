package org.icgc.dcc.pcawg.client.data.icgc;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
@Value class IdPair {

  public static IdPair newPair(String submitterSampleId, String fileId) {
    return new IdPair(submitterSampleId, fileId);
  }

  @NonNull
  private final String submitterSampleId;

  @NonNull
  private final String fileId;
}
