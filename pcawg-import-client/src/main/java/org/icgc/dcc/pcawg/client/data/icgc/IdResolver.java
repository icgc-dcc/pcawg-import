package org.icgc.dcc.pcawg.client.data.icgc;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.BasicDao;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataNotFoundException;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;
import static org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetBeanDao.createWildcardRequestBuilder;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public class IdResolver {

  private static final String WGS = "WGS";
  private static final String NORMAL = "normal";

  private static <T> T getFirst(List<T> list){
    checkArgument(list.size()>0, "The list is empty");
    return list.get(0);
  }

  private static BarcodeSheetBean getFirstBarcodeBean(BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao, BarcodeSearchRequest request) throws
      SampleMetadataNotFoundException {
    val beans = barcodeSheetDao.find(request);
    if(beans.isEmpty()){
      throw new SampleMetadataNotFoundException(
          String.format("Could not find first BarcodeSheetBean for BarcodeSearchRequest [%s]", request));
    }
    return getFirst(beans);
  }

  @SneakyThrows
  public static String getAnalyzedSampleId(BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao, boolean isUsProject, BarcodeSearchRequest request ) {
    if (isUsProject){
      return getFirstBarcodeBean(barcodeSheetDao, request).getBarcode();
    } else {
      return request.getUuid();
    }
  }

  private static SampleSheetBean getFirstSampleBean(SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao, SampleSheetSearchRequest request) throws SampleMetadataNotFoundException{
    val beans = sampleSheetDao.find(request);
    if (beans.isEmpty()){
      throw new SampleMetadataNotFoundException(
          String.format("Could not find first SampleSheetBean for SampleSheetSearchRequest: %s", request ));
    }
    return getFirst(beans);
  }

  @SneakyThrows
  public static String getMatchedSampleId(SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao,
      BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao,
      String donorUniqueId ){
    val request = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .dcc_specimen_type(NORMAL)
        .library_strategy(WGS)
        .build();
    val result = getFirstSampleBean(sampleSheetDao,request);
    val isUsProject = result.isUsProject();
    val submitterSampleId = result.getSubmitter_sample_id();
    val barcodeSearchRequest = newBarcodeRequest(submitterSampleId);
    return getAnalyzedSampleId(barcodeSheetDao, isUsProject, barcodeSearchRequest);
  }

  public static Set<String> getAllTcgaAliquotBarcodes(SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao,
      BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao){
    log.info("Fetching all tcga_aliquot_barcodes from SampleDao: {} and BarcodeDao: {} instances", sampleSheetDao.getClass().getName(), barcodeSheetDao
        .getClass().getName());
    return sampleSheetDao.findAll().stream()
        .filter(SampleSheetBean::isUsProject)
        .map(s -> createSampleIds(sampleSheetDao, barcodeSheetDao, s))
        .flatMap(Set::stream)
        .collect(toImmutableSet());
  }

  public static Set<String> getAllSubmitterSampleIds(SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao,
      BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao){
    log.info("Fetching all submitter_sample_ids from SampleDao: {} and BarcodeDao: {} instances", sampleSheetDao.getClass().getName(), barcodeSheetDao
        .getClass().getName());
    return sampleSheetDao.findAll().stream()
        .filter(s -> !s.isUsProject())
        .map(s -> createSampleIds(sampleSheetDao, barcodeSheetDao, s))
        .flatMap(Set::stream)
        .collect(toImmutableSet());
  }


  private static Set<String> createSampleIds(SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao,
      BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao, SampleSheetBean sampleSheetBean){
    val submitterSampleId = sampleSheetBean.getSubmitter_sample_id();
    val barcodeSearchRequest = newBarcodeRequest(submitterSampleId);
      val donorUniqueId = sampleSheetBean.getDonor_unique_id();
    val isUsProject = sampleSheetBean.isUsProject();
    val analyzedSampleId = IdResolver.getAnalyzedSampleId(barcodeSheetDao,isUsProject,barcodeSearchRequest);
    val matchedSampleId = IdResolver.getMatchedSampleId(sampleSheetDao, barcodeSheetDao,donorUniqueId);
    return newHashSet(analyzedSampleId, matchedSampleId);
  }

}
