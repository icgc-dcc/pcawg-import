package org.icgc.dcc.pcawg.client.data;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.ObjectNodeConverter;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalFiles;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.data.IdResolver.getAllSubmitterSampleIds;
import static org.icgc.dcc.pcawg.client.data.IdResolver.getAllTcgaAliquotBarcodes;
import static org.icgc.dcc.pcawg.client.download.PortalSubmitterSampleIdQueryCreator.newSubmitterSampleIdQueryCreator;
import static org.icgc.dcc.pcawg.client.download.PortalTcgaAliquotBarcodeQueryCreator.newTcgaAliquotBarcodeQueryCreator;

@Slf4j
@RequiredArgsConstructor
public class IcgcFileIdDao {

  private static final int DEFAULT_BATCH_SIZE = 100;

  public static IcgcFileIdDao newIcgcFileIdDao(SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, String> barcodeDao){
    val submitterSampleIds = getAllSubmitterSampleIds(sampleDao, barcodeDao);
    val tcgaAliquotBarcodes = getAllTcgaAliquotBarcodes(sampleDao, barcodeDao);
    return new IcgcFileIdDao(tcgaAliquotBarcodes, submitterSampleIds);
  }

  @NonNull
  private final Set<String> tcgaAliquotBarcode;

  @NonNull
  private final Set<String> submitterSampleIds;

  private Map<String, String> map = Maps.newHashMap();

  private Set<Pair> procSubmitterSampleIdQuery(int batchSize) {
    log.info("Querying portal for SubmitterSampleIds...");
    return procQuery(newSubmitterSampleIdQueryCreator(submitterSampleIds, batchSize), submitterSampleIds.size(), batchSize);
  }

  private Set<Pair> procTcgaAliquotBarcodeQuery(int batchSize) {
    log.info("Querying portal for TcgaAliguotBarcodes...");
    return procQuery(newTcgaAliquotBarcodeQueryCreator(tcgaAliquotBarcode, batchSize), tcgaAliquotBarcode.size(), batchSize);

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


  public void init(){
    log.info("Initializing {}...", this.getClass().getName());
    val usPairs = procTcgaAliquotBarcodeQuery(DEFAULT_BATCH_SIZE);
    val nonUsPairs = procSubmitterSampleIdQuery(DEFAULT_BATCH_SIZE);
    val set = Sets.newHashSet(nonUsPairs);
    set.addAll(usPairs);
    set.forEach( p -> map.put(p.getSubmitterSampleId(), p.getFileId()));
    map = ImmutableMap.copyOf(map);
    log.info("\t-Done");
  }

  private static Pair extractPair(ObjectNode o){
    val fileId = PortalFiles.getFileId(o);
    val submitterSampleId = PortalFiles.getSampleId(o);
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
