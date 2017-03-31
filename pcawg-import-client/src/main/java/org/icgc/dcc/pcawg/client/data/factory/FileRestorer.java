package org.icgc.dcc.pcawg.client.data.factory;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access =  PRIVATE)
public class FileRestorer<T extends Serializable> {

  public static <T extends Serializable> FileRestorer<T> newFileRestorer(String persistedFilename){
    return new FileRestorer<T>(persistedFilename);
  }

  @NonNull
  @Getter
  private final String persistedFilename;

  @SuppressWarnings("unchecked")
  public T restore() throws IOException, ClassNotFoundException {
    if (isPersisted()){
      return (T) ObjectPersistance.restore(getPersistedFilename());
    } else {
      throw new IllegalStateException(String.format("Cannot restore if persistedFilename [%s] DNE", persistedFilename));
    }
  }

  public void store(T t) throws IOException {
    ObjectPersistance.store(t,getPersistedFilename());
  }

  public void clean() throws IOException {
    Files.deleteIfExists(Paths.get(getPersistedFilename()));
  }

  public boolean isPersisted(){
    val persistedPath = Paths.get(getPersistedFilename());
    return Files.exists(persistedPath);
  }


}
