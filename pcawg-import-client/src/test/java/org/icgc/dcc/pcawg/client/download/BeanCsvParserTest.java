package org.icgc.dcc.pcawg.client.download;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class BeanCsvParserTest {

  private static final String TEST_SAMPLE_SHEET_FILENAME = "fixtures/test_sample_sheet.tsv";
  private static final String TEST_UUID2BARCODE_SHEET_FILENAME = "fixtures/test_uuid2barcode_sheet.tsv";
  private static Reader sampleSheetReader;
  private static Reader barcodeSheetReader;

  @BeforeClass
  @SneakyThrows
  public static void beforeClass(){
    sampleSheetReader = new FileReader(getTestFile(TEST_SAMPLE_SHEET_FILENAME));
    barcodeSheetReader = new FileReader(getTestFile(TEST_UUID2BARCODE_SHEET_FILENAME));
  }

  @AfterClass
  @SneakyThrows
  public static void afterClass(){
    if (sampleSheetReader != null){
      sampleSheetReader.close();
    }

    if (barcodeSheetReader != null){
      barcodeSheetReader.close();
    }
  }

  @Test
  @SneakyThrows
  public void testSampleDaoUs(){
    val dao = SampleBeanDao.newSampleBeanDao(sampleSheetReader);
    val aliquotId = "e0fccaf5-925a-41f9-b87c-cd5ee4aecb59";
    log.info("AliquotId: {}", aliquotId);
    log.info("fff: {}",
        dao.findAliquotId(aliquotId)
        .stream()
        .map(SampleBean::toString)
        .collect(joining("\n")) );
  }

  @Test
  @SneakyThrows
  public void testSampleDaoNonUs(){
    val dao = SampleBeanDao.newSampleBeanDao(sampleSheetReader);
    val aliquotId = "f82d213f-bc96-5b1d-e040-11ac0c486880";
    log.info("AliquotId: {}", aliquotId);
    log.info("fff: {}",
        dao.findAliquotId(aliquotId)
            .stream()
            .map(SampleBean::toString)
            .collect(joining("\n")) );
  }

  @Test
  @SneakyThrows
  public void testRequestWildCardAliquotId(){
    val request = SampleSearchRequest.builder()
        .aliquot_id("*")
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .dcc_specimen_type("normal")
        .library_strategy("WGS")
        .build();
    val dao = SampleBeanDao.newSampleBeanDao(sampleSheetReader);
    val results = dao.find(request);
    assertThat(results).hasSize(2);
  }

  @Test
  @SneakyThrows
  public void testBarcodeDao(){
    val dao = BarcodeBeanDao.newBarcodeBeanDao(barcodeSheetReader);
    val uuid = "0304b12d-7640-4150-a581-2eea2b1f2ad5";
    log.info("UUID: {}", uuid);
    log.info(dao.find(uuid)
        .stream()
        .map(BarcodeBean::toString)
        .collect(joining("\n")));

  }


  @SneakyThrows
  public static File getTestFile(String filename){
    val url = Resources.getResource(filename);
    return new File(url.toURI());
  }


}
