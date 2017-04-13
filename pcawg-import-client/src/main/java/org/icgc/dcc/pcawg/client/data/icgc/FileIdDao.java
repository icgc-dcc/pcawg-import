package org.icgc.dcc.pcawg.client.data.icgc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.ObjectNodeConverter;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.data.BasicDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalFiles;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorer;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.data.icgc.IdResolver.getAllSubmitterSampleIds;
import static org.icgc.dcc.pcawg.client.data.icgc.IdResolver.getAllTcgaAliquotBarcodes;
import static org.icgc.dcc.pcawg.client.download.query.PortalSubmittedSampleIdQueryCreator.newSubmitterSampleIdQueryCreator;
import static org.icgc.dcc.pcawg.client.download.query.PortalTcgaAliquotBarcodeQueryCreator.newTcgaAliquotBarcodeQueryCreator;

@Slf4j
@RequiredArgsConstructor
public class FileIdDao implements Serializable {

  public static final long serialVersionUID = 1490966500L;

  private static final int DEFAULT_BATCH_SIZE = 200;

  public static FileIdDao newFileIdDao(SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao,
      BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao){
    val submitterSampleIds = getAllSubmitterSampleIds(sampleSheetDao, barcodeSheetDao);
    val tcgaAliquotBarcodes = getAllTcgaAliquotBarcodes(sampleSheetDao, barcodeSheetDao);
    val dao = new FileIdDao(tcgaAliquotBarcodes, submitterSampleIds);
    dao.init();
    return dao;
  }

  @SneakyThrows
  public static FileIdDao newFileIdDao(String persistedFilename,
      SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao,
      BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao){
    val fileRestorer = LocalFileRestorer.<FileIdDao>newLocalFileRestorer(Paths.get(persistedFilename));
    if (fileRestorer.isPersisted()){
      log.info("Persisted filename for IcgcFileIdDao found [{}], restoring it...", fileRestorer.getPersistedPath());
      return fileRestorer.restore();
    } else {
      log.info("Persisted filename for IcgcFileIdDao NOT found [{}], creating a new one and storing it...", fileRestorer.getPersistedPath());
      val dao = newFileIdDao(sampleSheetDao, barcodeSheetDao);
      fileRestorer.store(dao);
      return dao;
    }
  }

  @NonNull
  private transient final Set<String> tcgaAliquotBarcodes;

  @NonNull
  private transient final Set<String> submitterSampleIds;

  private Map<String, String> map = Maps.newHashMap();

  private Set<IdPair> procSubmitterSampleIdQuery(int batchSize) {
    log.info("Querying portal for SubmitterSampleIds...");
    return procQuery(newSubmitterSampleIdQueryCreator(submitterSampleIds, batchSize), submitterSampleIds.size(), batchSize, FileIdDao::extractSubmittedSamplePair);
  }

  private Set<IdPair> procTcgaAliquotBarcodeQuery(int batchSize) {
    log.info("Querying portal for TcgaAliguotBarcodes...");
    return procQuery(newTcgaAliquotBarcodeQueryCreator(tcgaAliquotBarcodes, batchSize), tcgaAliquotBarcodes.size(), batchSize, FileIdDao::extractBarcodePair);

  }

  private static <T extends ObjectNodeConverter>  Set<IdPair> procQuery(Iterable<T> queries, int totalSize, int batchSize, Function<ObjectNode, IdPair> extractFunctor) {
    val setBuilder = ImmutableSet.<IdPair>builder();
    int count = 0;
    int total = (int)Math.ceil(totalSize/(double)batchSize);
    for (val q : queries){
      log.info("Querying Portal for Request {} / {}", ++count, total);
      val pairs = getResponse(q).stream()
          .map(extractFunctor)
          .distinct()
          .collect(toImmutableSet());
      setBuilder.addAll(pairs);
    }
    return setBuilder.build();
  }


  private void init(){
    log.info("Initializing {}...", this.getClass().getName());
    val usPairs = procTcgaAliquotBarcodeQuery(DEFAULT_BATCH_SIZE);
    val nonUsPairs = procSubmitterSampleIdQuery(DEFAULT_BATCH_SIZE);
    val set = Sets.newHashSet(nonUsPairs);
    set.addAll(usPairs);
    for (val pair : set){
      val submittedSampleId = pair.getSubmitterSampleId();
      val previouslyDefined = map.containsKey(submittedSampleId);
      checkState(!previouslyDefined,
          "The SubmittedSampleId [%s] is already defined with FileId [%s]. The assumption is that there is a 1-1 mapping",
          submittedSampleId, pair.getFileId());
      map.put(submittedSampleId, pair.getFileId());
    }
    map = ImmutableMap.copyOf(map);
    log.info("\t-Done");
  }

  private static IdPair extractSubmittedSamplePair(ObjectNode o){
    val fileId = PortalFiles.getFileId(o);
    val submitterSampleId = PortalFiles.getSubmittedSampleId(o);
    return IdPair.newPair(submitterSampleId, fileId);
  }
  private static IdPair extractBarcodePair(ObjectNode o){
    val fileId = PortalFiles.getFileId(o);
    val submitterSampleId = PortalFiles.getTcgaAliquotBarcode(o);
    return IdPair.newPair(submitterSampleId, fileId);
  }

  public Optional<String> find(String submitterSampleId){
    if (map.containsKey(submitterSampleId)){
      return Optional.of(map.get(submitterSampleId));
    } else {
      return Optional.empty();
    }
  }

  private static List<ObjectNode> getResponse(ObjectNodeConverter jsonQueryGenerator){
    val p = Portal.builder()
        .jsonQueryGenerator(jsonQueryGenerator)
        .build();
    return p.getFileMetas();
  }

}
