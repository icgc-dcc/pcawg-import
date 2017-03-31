package org.icgc.dcc.pcawg.client.data;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.ObjectNodeConverter;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.factory.FileRestorer;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalFiles;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.data.IdResolver.getAllSubmitterSampleIds;
import static org.icgc.dcc.pcawg.client.data.IdResolver.getAllTcgaAliquotBarcodes;
import static org.icgc.dcc.pcawg.client.download.PortalSubmittedSampleIdQueryCreator.newSubmitterSampleIdQueryCreator;
import static org.icgc.dcc.pcawg.client.download.PortalTcgaAliquotBarcodeQueryCreator.newTcgaAliquotBarcodeQueryCreator;

@Slf4j
@RequiredArgsConstructor
public class IcgcFileIdDao implements Serializable {

  public static final long serialVersionUID = 1490966500L;

  private static final int DEFAULT_BATCH_SIZE = 100;

  public static IcgcFileIdDao newIcgcFileIdDao(SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao){
    val submitterSampleIds = getAllSubmitterSampleIds(sampleDao, barcodeDao);
    val tcgaAliquotBarcodes = getAllTcgaAliquotBarcodes(sampleDao, barcodeDao);
    val dao = new IcgcFileIdDao(tcgaAliquotBarcodes, submitterSampleIds);
    dao.init();
    return dao;
  }

  @SneakyThrows
  public static IcgcFileIdDao newIcgcFileIdDao(String persistedFilename,
      SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao){
    val fileRestorer = FileRestorer.<IcgcFileIdDao>newFileRestorer(persistedFilename);
    if (fileRestorer.isPersisted()){
      log.info("Persisted filename for IcgcFileIdDao found [{}], restoring it...", fileRestorer.getPersistedFilename());
      return fileRestorer.restore();
    } else {
      log.info("Persisted filename for IcgcFileIdDao NOT found [{}], creating a new one and storing it...", fileRestorer.getPersistedFilename());
      val dao = newIcgcFileIdDao(sampleDao, barcodeDao);
      fileRestorer.store(dao);
      return dao;
    }
  }

  @NonNull
  private transient final Set<String> tcgaAliquotBarcodes;

  @NonNull
  private transient final Set<String> submitterSampleIds;

  private Map<String, String> map = Maps.newHashMap();

  private Set<Pair> procSubmitterSampleIdQuery(int batchSize) {
    log.info("Querying portal for SubmitterSampleIds...");
    return procQuery(newSubmitterSampleIdQueryCreator(submitterSampleIds, batchSize), submitterSampleIds.size(), batchSize);
  }

  private Set<Pair> procTcgaAliquotBarcodeQuery(int batchSize) {
    log.info("Querying portal for TcgaAliguotBarcodes...");
    return procQuery(newTcgaAliquotBarcodeQueryCreator(tcgaAliquotBarcodes, batchSize), tcgaAliquotBarcodes.size(), batchSize);

  }

  private static <T extends ObjectNodeConverter>  Set<Pair> procQuery(Iterable<T> queries, int totalSize, int batchSize) {
    val setBuilder = ImmutableSet.<Pair>builder();
    int count = 0;
    int total = (int)Math.ceil(totalSize/(double)batchSize);
    for (val q : queries){
      log.info("Querying Portal for Request {} / {}", ++count, total);
      val pairs = getResponse(q).stream()
          .map(IcgcFileIdDao::extractPair)
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

  private static Pair extractPair(ObjectNode o){
    val fileId = PortalFiles.getFileId(o);
    val submitterSampleId = PortalFiles.getSubmittedSampleId(o);
    return Pair.newPair(submitterSampleId, fileId);
  }

  public Optional<String> find(String submitterSampleId){
    if (map.containsKey(submitterSampleId)){
      return Optional.of(map.get(submitterSampleId));
    } else {
      return Optional.empty();
    }
  }

  @RequiredArgsConstructor(access = PRIVATE)
  @Value
  private static class Pair{

    public static Pair newPair(String submitterSampleId, String fileId){
      return new Pair(submitterSampleId, fileId);
    }

    @NonNull
    private final String submitterSampleId;

    @NonNull
    private final String fileId;
  }

  private static List<ObjectNode> getResponse(ObjectNodeConverter jsonQueryGenerator){
    val p = Portal.builder()
        .jsonQueryGenerator(jsonQueryGenerator)
        .build();
    return p.getFileMetas();
  }

}
