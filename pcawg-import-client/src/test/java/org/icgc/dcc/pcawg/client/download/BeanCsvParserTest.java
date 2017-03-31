package org.icgc.dcc.pcawg.client.download;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBeanDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean.newBarcodeBean;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;

@Slf4j
public class BeanCsvParserTest {

  private static final String TEST_SAMPLE_SHEET_FILENAME = "fixtures/test_sample_sheet.tsv";
  private static final String TEST_UUID2BARCODE_SHEET_FILENAME = "fixtures/test_uuid2barcode_sheet.tsv";
  private  Reader sampleSheetReader;
  private Reader barcodeSheetReader;


  @After
  @SneakyThrows
  public  void afterTest(){
    if (sampleSheetReader != null){
      sampleSheetReader.close();
    }

    if (barcodeSheetReader != null){
      barcodeSheetReader.close();
    }
  }


  @Before
  @SneakyThrows
  public void beforeTest(){
    sampleSheetReader = new FileReader(getTestFile(TEST_SAMPLE_SHEET_FILENAME));
    barcodeSheetReader = new FileReader(getTestFile(TEST_UUID2BARCODE_SHEET_FILENAME));
  }

  private Reader getSampleSheetReader(){
    return sampleSheetReader;
  }
  private Reader getBarcodeSheetReader(){
    return barcodeSheetReader;
  }

  @Test
  public void testBarcodeBeanEquality(){
    val ref = newBarcodeBean("a", "b", "c", "d");
    val same = newBarcodeBean("a", "b", "c", "d");
    val diff = newBarcodeBean("a", "b", "c", "z");
    assertThat(ref.equals(same)).isTrue();
    assertThat(ref.equals(diff)).isFalse();
    assertThat(ref.hashCode() == same.hashCode()).isTrue();
  }

  @Test
  public void testBarcodeSearchRequestEquality(){
    val ref = BarcodeSearchRequest.newBarcodeRequest("a");
    val same = BarcodeSearchRequest.newBarcodeRequest("a");
    val diff = BarcodeSearchRequest.newBarcodeRequest("c");
    assertThat(ref.equals(same)).isTrue();
    assertThat(ref.equals(diff)).isFalse();
    assertThat(ref.hashCode() == same.hashCode()).isTrue();
  }

  @Test
  public void testSampleSearchRequestEquality(){
    val ref = SampleSearchRequest.builder()
        .donor_unique_id("a")
        .aliquot_id("b")
        .dcc_specimen_type("c")
        .library_strategy("d")
        .build();

    val same = SampleSearchRequest.builder()
        .donor_unique_id("a")
        .aliquot_id("b")
        .dcc_specimen_type("c")
        .library_strategy("d")
        .build();

    val diff = SampleSearchRequest.builder()
        .donor_unique_id("a")
        .aliquot_id("b")
        .dcc_specimen_type("c")
        .library_strategy("zzzz")
        .build();

    assertThat(ref.equals(same)).isTrue();
    assertThat(ref.equals(diff)).isFalse();
    assertThat(ref.hashCode() == same.hashCode()).isTrue();
  }

  @Test
  public void testSampleBeansEquality(){
    val ref = SampleBean.builder()
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .donor_wgs_exclusion_white_gray("Whitelist")
        .submitter_donor_id("096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .icgc_donor_id("DO804")
        .dcc_project_code("BLCA-US")
        .aliquot_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .submitter_specimen_id("27461a27-26eb-4c2c-9c54-e16fbd32c615")
        .icgc_specimen_id("SP1682")
        .submitter_sample_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .icgc_sample_id("SA5237")
        .dcc_specimen_type("Normal - solid tissue")
        .library_strategy("WGS")
        .build();

    val same = SampleBean.builder()
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .donor_wgs_exclusion_white_gray("Whitelist")
        .submitter_donor_id("096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .icgc_donor_id("DO804")
        .dcc_project_code("BLCA-US")
        .aliquot_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .submitter_specimen_id("27461a27-26eb-4c2c-9c54-e16fbd32c615")
        .icgc_specimen_id("SP1682")
        .submitter_sample_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .icgc_sample_id("SA5237")
        .dcc_specimen_type("Normal - solid tissue")
        .library_strategy("WGS")
        .build();

    val diff = SampleBean.builder()
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .donor_wgs_exclusion_white_gray("Whitelist")
        .submitter_donor_id("096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .icgc_donor_id("DO804")
        .dcc_project_code("BLCA-US")
        .aliquot_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .submitter_specimen_id("27461a27-26eb-4c2c-9c54-e16fbd32c615")
        .icgc_specimen_id("SP1682")
        .submitter_sample_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .icgc_sample_id("SA5237")
        .dcc_specimen_type("Normal - solid tissue")
        .library_strategy("WGSSSSSSSSSSSSSSS")
        .build();

    assertThat(ref.equals(same)).isTrue();
    assertThat(ref.equals(diff)).isFalse();
    assertThat(ref.hashCode() == same.hashCode()).isTrue();
  }

  @Test
  @SneakyThrows
  public void testSampleDaoUs(){
    val dao = SampleBeanDao.newSampleBeanDao(getSampleSheetReader());
    val aliquotId = "e0fccaf5-925a-41f9-b87c-cd5ee4aecb59";
    val results = dao.findAliquotId(aliquotId);
    assertThat(results).hasSize(1);
    val bean1 = SampleBean.builder()
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .donor_wgs_exclusion_white_gray("Whitelist")
        .submitter_donor_id("096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .icgc_donor_id("DO804")
        .dcc_project_code("BLCA-US")
        .aliquot_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .submitter_specimen_id("27461a27-26eb-4c2c-9c54-e16fbd32c615")
        .icgc_specimen_id("SP1682")
        .submitter_sample_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .icgc_sample_id("SA5237")
        .dcc_specimen_type("Normal - solid tissue")
        .library_strategy("WGS")
        .build();
    assertThat(results.get(0)).isEqualTo(bean1);
  }

  @Test
  @SneakyThrows
  public void testSampleDaoNonUs(){
    val dao = SampleBeanDao.newSampleBeanDao(getSampleSheetReader());
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
  public void testRequestWildCardAliquotId() {
    val request = SampleSearchRequest.builder()
        .aliquot_id("*")
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .dcc_specimen_type("normal")
        .library_strategy("WGS")
        .build();
    val dao = SampleBeanDao.newSampleBeanDao(getSampleSheetReader());
    val results = dao.find(request);
    log.info("Results: {}", results.stream().map(SampleBean::toString).collect(Collectors.joining("\n")));
    assertThat(results).hasSize(2);
    val bean1 = SampleBean.builder()
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .donor_wgs_exclusion_white_gray("Whitelist")
        .submitter_donor_id("096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .icgc_donor_id("DO804")
        .dcc_project_code("BLCA-US")
        .aliquot_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .submitter_specimen_id("27461a27-26eb-4c2c-9c54-e16fbd32c615")
        .icgc_specimen_id("SP1682")
        .submitter_sample_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb59")
        .icgc_sample_id("SA5237")
        .dcc_specimen_type("Normal - solid tissue")
        .library_strategy("WGS")
        .build();
    assertThat(results.get(0)).isEqualTo(bean1);

    val bean2 = SampleBean.builder()
        .donor_unique_id("BLCA-US::096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .donor_wgs_exclusion_white_gray("Whitelist")
        .submitter_donor_id("096b4f32-10c1-4737-a0dd-cae04c54ee33")
        .icgc_donor_id("DO804")
        .dcc_project_code("BLCA-US")
        .aliquot_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb58")
        .submitter_specimen_id("27461a27-26eb-4c2c-9c54-e16fbd32c614")
        .icgc_specimen_id("SP1681")
        .submitter_sample_id("e0fccaf5-925a-41f9-b87c-cd5ee4aecb58")
        .icgc_sample_id("SA5236")
        .dcc_specimen_type("Normal - solid tissue")
        .library_strategy("WGS")
        .build();
    assertThat(results.get(1)).isEqualTo(bean2);
  }

  @Test
  @SneakyThrows
  public void testBarcodeDao(){
    val dao = BarcodeBeanDao.newBarcodeBeanDao(getBarcodeSheetReader());
    val uuid = "0304b12d-7640-4150-a581-2eea2b1f2ad5";
    val expected = newBarcodeBean(
    "TCGA-ACC", "donor",
        "0304b12d-7640-4150-a581-2eea2b1f2ad5", "TCGA-OR-A5LL");

    val results = dao.find(newBarcodeRequest(uuid));
    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isEqualTo(expected);
  }

  @Test
  @SneakyThrows
  public void testCapsBarcodeDao(){
    val dao = BarcodeBeanDao.newBarcodeBeanDao(getBarcodeSheetReader());
    val uuid = "075dbfd0-9cf4-4877-884f-ae858902c79e";
    val expected = newBarcodeBean(
    "TCGA-ACC","donor",
        "075dbfd0-9cf4-4877-884f-ae858902c79e", "TCGA-OR-A5J7");
    val results = dao.find(newBarcodeRequest(uuid));
    assertThat(results).hasSize(1);
    assertThat(results.get(0)).isEqualTo(expected);

  }


  @SneakyThrows
  public static File getTestFile(String filename){
    val url = Resources.getResource(filename);
    return new File(url.toURI());
  }


}
