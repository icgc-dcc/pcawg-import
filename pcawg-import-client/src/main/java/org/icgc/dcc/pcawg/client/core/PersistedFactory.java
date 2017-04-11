package org.icgc.dcc.pcawg.client.core;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetBeanDao;
import org.icgc.dcc.pcawg.client.data.factory.impl.BarcodeBeanDaoFactory;
import org.icgc.dcc.pcawg.client.data.factory.impl.SampleBeanDaoFactory;
import org.icgc.dcc.pcawg.client.data.icgc.FileIdDao;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.metadata.impl.FileSampleMetadataBeanDAO;
import org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetBeanDao;
import org.icgc.dcc.pcawg.client.download.MetadataContainer;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorer;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_BEAN_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_HAS_HEADER;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_TSV_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.ICGC_FILE_ID_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.METADATA_CONTAINER_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PERSISTANCE_DIR;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_BEAN_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_HAS_HEADER;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.core.Factory.newPortal;
import static org.icgc.dcc.pcawg.client.data.factory.impl.SampleBeanDaoFactory.newSampleBeanDaoFactory;
import static org.icgc.dcc.pcawg.client.data.icgc.FileIdDao.newFileIdDao;
import static org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory.newFileRestorerFactory;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@RequiredArgsConstructor
@Slf4j
public class PersistedFactory {

  public static PersistedFactory newPersistedFactory(boolean useFast){
    val persistedDir = Paths.get(PERSISTANCE_DIR);
    return newPersistedFactory(newFileRestorerFactory(persistedDir), useFast);
  }

  public static PersistedFactory newPersistedFactory(Path persistedDir, boolean useFast){
    return newPersistedFactory(newFileRestorerFactory(persistedDir), useFast);
  }

  @SneakyThrows
  public static PersistedFactory newPersistedFactory(LocalFileRestorerFactory localFileRestorerFactory, boolean useFast){
    val sampleBeanDao= buildSampleBeanDaoFactory(localFileRestorerFactory, useFast).getObject();
    val barcodeBeanDao= buildBarcodeBeanDaoFactory(localFileRestorerFactory, useFast).getObject();
    return new PersistedFactory(localFileRestorerFactory, sampleBeanDao, barcodeBeanDao);
  }

  private static SampleBeanDaoFactory buildSampleBeanDaoFactory(LocalFileRestorerFactory localFileRestorerFactory,
      boolean useFast){
    val sampleBeanDaoRestorer = (LocalFileRestorer<SampleSheetBeanDao>)localFileRestorerFactory.<SampleSheetBeanDao>createFileRestorer(
        SAMPLE_BEAN_DAO_PERSISTANCE_FILENAME);
    return newSampleBeanDaoFactory(SAMPLE_SHEET_TSV_URL,SAMPLE_SHEET_TSV_FILENAME,
        sampleBeanDaoRestorer,useFast, SAMPLE_SHEET_HAS_HEADER);
  }

  private static BarcodeBeanDaoFactory buildBarcodeBeanDaoFactory(LocalFileRestorerFactory localFileRestorerFactory,
      boolean useFast){
    val sampleBeanDaoRestorer = (LocalFileRestorer<BarcodeSheetBeanDao>)localFileRestorerFactory.<BarcodeSheetBeanDao>createFileRestorer(
        BARCODE_BEAN_DAO_PERSISTANCE_FILENAME);
    return BarcodeBeanDaoFactory.newBarcodeBeanDaoFactory(BARCODE_SHEET_TSV_URL,BARCODE_SHEET_TSV_FILENAME,
        sampleBeanDaoRestorer,useFast, BARCODE_SHEET_HAS_HEADER);
  }


  /**
   * Dependencies
   */

  @NonNull @Getter private final LocalFileRestorerFactory localFileRestorerFactory;
  @NonNull @Getter private final SampleSheetBeanDao sampleSheetBeanDao;
  @NonNull @Getter private final BarcodeSheetBeanDao barcodeSheetBeanDao;

  /**
   * State
   */
  private FileIdDao fileIdDao;

  @SneakyThrows
  public MetadataContainer newMetadataContainer(){
    val restorer = localFileRestorerFactory.<MetadataContainer>createFileRestorer(METADATA_CONTAINER_PERSISTANCE_FILENAME);
    if (restorer.isPersisted()){
      return restorer.restore();
    } else {
      val container = buildMetadataContainer();
      restorer.store(container);
      return container;
    }
  }

  @SneakyThrows
  public SampleMetadataDAO newSampleMetadataDAO(){
    fileIdDao =  buildFileId();
    log.info("Done initialized SampleDao, BarcodeDao, and FileIdDao, ... creating FileSampleMetadataBeanDAO");
    return new FileSampleMetadataBeanDAO(sampleSheetBeanDao, barcodeSheetBeanDao, fileIdDao);
  }

  public FileIdDao buildFileId(){
    return newFileIdDao(ICGC_FILE_ID_DAO_PERSISTANCE_FILENAME, sampleSheetBeanDao, barcodeSheetBeanDao);
  }

  private MetadataContainer buildMetadataContainer(){
    log.info("Creating Portal instance for [{}]", CONSENSUS);
    val portal = newPortal(CONSENSUS);

    log.info("Retreiving portal filemeta data...");
    val portalMetadataSet = portal.getFileMetas()
        .stream()
        .map(PortalMetadata::buildPortalMetadata)
        .collect(toImmutableSet());

    log.info("Creating base sampleMetadata DAO...");
    val sampleMetadataDao = newSampleMetadataDAO();

    log.info("Instantiating MetadataContainer");
    return new MetadataContainer(sampleMetadataDao, portalMetadataSet);
  }

}
