package org.icgc.dcc.pcawg.client.data;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.SampleMetadataDAO.SampleMetadataNotFoundException;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;
import static org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao.createWildcardRequestBuilder;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class IdResolver {

  private static final String WGS = "WGS";
  private static final String NORMAL = "normal";

  private static <T> T getFirst(List<T> list){
    checkArgument(list.size()>0, "The list is empty");
    return list.get(0);
  }

  private static BarcodeBean getFirstBarcodeBean(BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao, BarcodeSearchRequest request) throws
      SampleMetadataNotFoundException {
    val beans = barcodeDao.find(request);
    if(beans.isEmpty()){
      throw new SampleMetadataNotFoundException(
          String.format("Could not find first BarcodeBean for BarcodeSearchRequest [%s]", request));
    }
    return getFirst(beans);
  }

  @SneakyThrows
  public static String getAnalyzedSampleId(BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao, boolean isUsProject, BarcodeSearchRequest request ) {
    if (isUsProject){
      return getFirstBarcodeBean(barcodeDao, request).getBarcode();
    } else {
      return request.getUuid();
    }
  }

  private static SampleBean getFirstSampleBean(SampleDao<SampleBean, SampleSearchRequest> sampleDao, SampleSearchRequest request) throws SampleMetadataNotFoundException{
    val beans = sampleDao.find(request);
    if (beans.isEmpty()){
      throw new SampleMetadataNotFoundException(
          String.format("Could not find first SampleBean for SampleSearchRequest: %s", request ));
    }
    return getFirst(beans);
  }

  @SneakyThrows
  public static String getMatchedSampleId(SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao,
      String donorUniqueId ){
    val request = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .dcc_specimen_type(NORMAL)
        .library_strategy(WGS)
        .build();
    val result = getFirstSampleBean(sampleDao,request);
    val isUsProject = result.isUsProject();
    val submitterSampleId = result.getSubmitter_sample_id();
    val barcodeSearchRequest = newBarcodeRequest(submitterSampleId);
    return getAnalyzedSampleId(barcodeDao, isUsProject, barcodeSearchRequest);
  }

  public static Set<String> getAllTcgaAliquotBarcodes(SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao){
    log.info("Fetching all tcga_aliquot_barcodes from SampleDao: {} and BarcodeDao: {} instances", sampleDao.getClass().getName(), barcodeDao.getClass().getName());
    return sampleDao.findAll().stream()
        .filter(SampleBean::isUsProject)
        .map(s -> createSampleIds(sampleDao, barcodeDao, s))
        .flatMap(Set::stream)
        .collect(toImmutableSet());
  }

  public static Set<String> getAllSubmitterSampleIds(SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao){
    log.info("Fetching all submitter_sample_ids from SampleDao: {} and BarcodeDao: {} instances", sampleDao.getClass().getName(), barcodeDao.getClass().getName());
    return sampleDao.findAll().stream()
        .filter(s -> !s.isUsProject())
        .map(s -> createSampleIds(sampleDao, barcodeDao, s))
        .flatMap(Set::stream)
        .collect(toImmutableSet());
  }


  private static Set<String> createSampleIds(SampleDao<SampleBean, SampleSearchRequest> sampleDao,
      BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao, SampleBean sampleBean){
    val submitterSampleId = sampleBean.getSubmitter_sample_id();
    val barcodeSearchRequest = newBarcodeRequest(submitterSampleId);
      val donorUniqueId = sampleBean.getDonor_unique_id();
    val isUsProject = sampleBean.isUsProject();
    val analyzedSampleId = IdResolver.getAnalyzedSampleId(barcodeDao,isUsProject,barcodeSearchRequest);
    val matchedSampleId = IdResolver.getMatchedSampleId(sampleDao, barcodeDao,donorUniqueId);
    return newHashSet(analyzedSampleId, matchedSampleId);
  }

}
