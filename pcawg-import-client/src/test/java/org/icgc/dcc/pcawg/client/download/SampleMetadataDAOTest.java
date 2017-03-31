package org.icgc.dcc.pcawg.client.download;

import com.google.common.base.Stopwatch;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Sets;
import org.icgc.dcc.pcawg.client.core.Factory;
import org.icgc.dcc.pcawg.client.data.IcgcFileIdDao;
import org.icgc.dcc.pcawg.client.data.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.factory.impl.BarcodeBeanDaoFactory;
import org.icgc.dcc.pcawg.client.data.factory.impl.SampleBeanDaoFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.core.Factory.newFileSampleMetadataBeanDAOAndDownload;
import static org.icgc.dcc.pcawg.client.core.Factory.newFileSampleMetadataDAOOldAndDownload;
import static org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename.newPortalFilename;

@Slf4j
public class SampleMetadataDAOTest {


  @SneakyThrows
  private void runQuery(SampleMetadataDAO sampleMetadataDAO){
    val nonUsFilename = "10cb8ac6-c622-11e3-bf01-24c6515278c0.dkfz-copyNumberEstimation_1-0-189-hpc-fix.1508271624.somatic.cnv.vcf.gz";
    val usFilename = "9c70688d-6e43-4520-9262-eaae4e4d597d.broad-snowman.20150827.somatic.sv.vcf.gz";
    val nonUsId = newPortalFilename(nonUsFilename);
    val usId = newPortalFilename(usFilename);

    //Non-US
    val nonUsProjectData = sampleMetadataDAO.fetchSampleMetadata(nonUsId);
    assertThat(nonUsProjectData.getDccProjectCode()).isEqualTo("LIRI-JP");
    assertThat(nonUsProjectData.getAnalyzedSampleId()).isEqualTo("RK001_C01");
    assertThat(nonUsProjectData.getMatchedSampleId()).isEqualTo("RK001_B01");

    //US
    val usProjectData = sampleMetadataDAO.fetchSampleMetadata(usId);
    assertThat(usProjectData.getDccProjectCode()).isEqualTo("BRCA-US");
    assertThat(usProjectData.getAnalyzedSampleId()).isEqualTo("TCGA-BH-A18R-01A-11D-A19H-09");
    assertThat(usProjectData.getMatchedSampleId()).isEqualTo("TCGA-BH-A18R-11A-42D-A19H-09");
  }

  @Test
  @SneakyThrows
  public void testOldFetchSampleMetadata(){
    runQuery(newFileSampleMetadataDAOOldAndDownload());
  }

  @Test
  @SneakyThrows
  public void testFastFetchSampleMetadata(){
    runQuery(Factory.newFastFileSampleMetadataBeanDAOAndDownload());
  }

  @Test
  @SneakyThrows
  @Ignore("Very slow")
  public void testBeanFetchSampleMetadata(){
    runQuery(newFileSampleMetadataBeanDAOAndDownload());
  }

  @Test
  @SneakyThrows
  @Ignore("Not ready yet")
  public void testIcgcFileIdDao(){
//    val sampleMetadataBeanDao = newFastFileSampleMetadataBeanDAOAndDownload();
    val sampleMetadataBeanDao = Factory.newFileSampleMetadataBeanDAOAndDownload();
    val sampleDao = sampleMetadataBeanDao.getSampleDao();
    val barcodeDao = sampleMetadataBeanDao.getBarcodeDao();

    val icgcDao = IcgcFileIdDao.newIcgcFileIdDao("icgcFileIdDao.dat", sampleDao, barcodeDao);
    log.info("Output[{}]: {}" );


  }


  @Test
  @SneakyThrows
  @Ignore("Test takes too long, and only long the first time its run")
  public void testSampleBeanDaoFactory(){
    val inputFilename = "test.sample.tsv";
    val persistFilename= "test.sample.dat";
    Files.deleteIfExists(Paths.get(inputFilename));
    Files.deleteIfExists(Paths.get(persistFilename));
    Storage.downloadFileByURL(SAMPLE_SHEET_TSV_URL,inputFilename);
    val factory = new SampleBeanDaoFactory(SAMPLE_SHEET_TSV_URL,inputFilename, persistFilename);
    val watch = Stopwatch.createStarted();
    val barcodeBeanDao = factory.getObject();
    watch.stop();
    log.info("First Time: {}", watch.elapsed(TimeUnit.MILLISECONDS));
    watch.reset();

    watch.start();
    val factory2 = new SampleBeanDaoFactory(BARCODE_SHEET_TSV_URL,inputFilename, persistFilename);
    val barcodeBeanDao2 = factory2.getObject();
    watch.stop();
    log.info("Second Time: {}", watch.elapsed(TimeUnit.MILLISECONDS));
    assertThat(barcodeBeanDao.findAll()).containsAll(barcodeBeanDao2.findAll());

  }

  @Test
  @SneakyThrows
  @Ignore("Test takes too long, and only long the first time its run")
  public void testBarcodeBeanDaoFactory(){
    val inputFilename = "test.barcode.tsv";
    val persistFilename= "test.barcode.dat";
    Files.deleteIfExists(Paths.get(inputFilename));
    Files.deleteIfExists(Paths.get(persistFilename));
    Storage.downloadFileByURL(BARCODE_SHEET_TSV_URL,inputFilename);
    val factory = new BarcodeBeanDaoFactory(BARCODE_SHEET_TSV_URL,inputFilename, persistFilename);
    val watch = Stopwatch.createStarted();
    val barcodeBeanDao = factory.getObject();
    watch.stop();
    log.info("First Time: {}", watch.elapsed(TimeUnit.MILLISECONDS));
    watch.reset();

    watch.start();
    val factory2 = new BarcodeBeanDaoFactory(BARCODE_SHEET_TSV_URL,inputFilename, persistFilename);
    val barcodeBeanDao2 = factory2.getObject();
    watch.stop();
    log.info("Second Time: {}", watch.elapsed(TimeUnit.MILLISECONDS));
    val set1 = Sets.newHashSet(barcodeBeanDao.findAll());
    val set2 = Sets.newHashSet(barcodeBeanDao2.findAll());
    assertThat(set1.containsAll(set2));
    assertThat(set2.containsAll(set1));

  }
}
