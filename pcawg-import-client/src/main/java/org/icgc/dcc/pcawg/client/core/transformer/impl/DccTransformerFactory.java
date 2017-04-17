package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.core.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;

import java.nio.file.Path;

/**
 * DccTransformer factory for building DccTransformer based on dccProjectCode
 */
@RequiredArgsConstructor
public class DccTransformerFactory<T> {

  public static <T> DccTransformerFactory<T> newDccTransformerFactory(
    FsController<Path> fsController, TSVConverter<T> tsvConverter, Path   outputDirectory,
    String fileNamePrefix, String fileExtension, final boolean append){
    return new DccTransformerFactory<T>(fsController, tsvConverter, outputDirectory, fileNamePrefix, fileExtension, append);
  }

  /**
   * Configuration
   */
  @NonNull private final FsController<Path> fsController;
  @NonNull private final TSVConverter<T> tsvConverter;
  @NonNull private final Path   outputDirectory;
  @NonNull private final String fileNamePrefix;
  @NonNull private final String fileExtension;
  private final boolean append;

  public DccTransformer<T> getDccTransformer(String dccProjectCode){
    return DccTransformer.<T>newDccTransformer(fsController, tsvConverter,
        outputDirectory,dccProjectCode, fileNamePrefix,
        fileExtension, append);
  }

}
