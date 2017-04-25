package org.icgc.dcc.pcawg.client.storage;

import com.google.common.hash.Hashing;
import lombok.NonNull;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;

public interface Storage {

  static String calcMd5Sum(@NonNull Path file) throws IOException {
    checkState(file.toFile().isFile(), "The input path [%s] is not a file", file);
    val bytes = Files.readAllBytes(file);
    return Hashing.md5()
        .newHasher()
        .putBytes(bytes)
        .hash()
        .toString();
  }

  File getFile(@NonNull PortalMetadata portalMetadata);

}
