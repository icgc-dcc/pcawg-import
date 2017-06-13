package org.icgc.dcc.pcawg.client.data.factory;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.SearchRequest;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.icgc.dcc.pcawg.client.storage.impl.PortalStorage.downloadFileByURL;

@RequiredArgsConstructor
@Getter
@Slf4j
public abstract class AbstractDaoFactory<B, R extends SearchRequest<R>, D extends AbstractFileDao<B, R>> {
  // take input url that should be used to download local_file
  // take local_file, record if exists
  // take persitance dat file, record if exists

  /*
       inputFnExists, outputFnExists
  if ( 0, 0) ->  download inputFn using url, and store to .dat
  if ( 0, 1) -> download inputFn using url, delete .dat, and store .dat
  if ( 1, 0) -> do not download, process the inputFn and parse it like normal, store .dat
  if ( 1, 1) -> do not download and do not process inputFn. Just restore .dat
   */

  @NonNull private final String downloadUrl;
  @NonNull private final String inputFilename;
  @NonNull private final LocalFileRestorer<D> localFileRestorer;

  @SuppressWarnings("unchecked")
  public final D getObject() throws IOException, ClassNotFoundException {
    List<String> a;
    val inputPath = Paths.get(inputFilename);
    val inputFileExists = Files.exists(inputPath);
    val persistedFileExists = localFileRestorer.isPersisted();

    D dao = null;
    if (inputFileExists && persistedFileExists){
      log.info("Inputfile [{}] and persistanceFile [{}] exist, deserializing...", inputFilename, localFileRestorer.getPersistedPath());
      dao = localFileRestorer.restore();
    } else if (inputFileExists && !persistedFileExists){
      log.info("Inputfile [{}] exists and persistanceFile [{}] DNE, creating new DAO and persisting it...", inputFilename, localFileRestorer
          .getPersistedPath());
      dao = newObject(getInputFilename());
      localFileRestorer.store(dao);
    } else if (!inputFileExists && persistedFileExists) {
      log.info("Inputfile [{}] DNE and persistanceFile [{}] exists, cleaning everyting, creating new DAO and persisting it...", inputFilename, localFileRestorer
          .getPersistedPath());
      val inputFile = downloadFileByURL(getDownloadUrl(), getInputFilename());
      localFileRestorer.clean();
      dao = newObject(inputFile.getAbsolutePath());
      localFileRestorer.store(dao);
    } else {
      log.info("Inputfile [{}] DNE and persistanceFile [{}] DNE, download, creating new DAO and persisting it...", inputFilename, localFileRestorer
          .getPersistedPath());
      val inputFile = downloadFileByURL(getDownloadUrl(), getInputFilename());
      dao = newObject(inputFile.getAbsolutePath());
      localFileRestorer.store(dao);
    }
    return dao;
  }

  protected abstract D newObject(String filename);


}

