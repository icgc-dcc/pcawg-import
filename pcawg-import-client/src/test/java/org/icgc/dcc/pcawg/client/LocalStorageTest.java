package org.icgc.dcc.pcawg.client;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Maps;
import org.assertj.core.util.Sets;
import org.icgc.dcc.pcawg.client.core.PersistedFactory;
import org.icgc.dcc.pcawg.client.core.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.core.types.WorkflowTypes;
import org.icgc.dcc.pcawg.client.data.factory.PortalMetadataDaoFactory;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.util.Lists.newArrayList;
import static org.icgc.dcc.pcawg.client.core.model.portal.PortalFilename.newPortalFilename;
import static org.icgc.dcc.pcawg.client.storage.impl.LocalStorage.newLocalStorage;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.VCFStreamFilter.newVCFStreamFilter;

@Slf4j
public class LocalStorageTest {

  private static final String LOCAL_STORAGE_DIR_PATH = "fixtures/localStorage";

  @SneakyThrows
  private static Path getPath(String filename){
    val url = Resources.getResource(filename);
    return new File(url.toURI()).toPath();
  }

  private PortalMetadata createPortalMetadata(Path vcfPathname, String md5Checksum){
    val filename = vcfPathname.getFileName().toString();
    val portalFilename = newPortalFilename(filename);
    return PortalMetadata.builder()
        .portalFilename(portalFilename)
        .dataType("myDataType")
        .donorId("myDonorId")
        .fileId("myFileId")
        .fileMd5sum(md5Checksum)
        .fileSize(0)
        .genomeBuild("myGenomeBuild")
        .objectId("myObjectId")
        .referenceName("myReferenceName")
        .sampleId("mySampleId")
        .build();
  }

  private Map<Path, PortalMetadata> buildTestMap(){
    val map = Maps.<Path, PortalMetadata>newHashMap();
    val path1 = Paths.get("f221c897-6ad0-0df9-e040-11ac0c4813ef.consensus.20160830.somatic.indel.vcf.gz");
    val pm1 = createPortalMetadata(path1,"5149d403009a139c7e085405ef762e1a" );
    map.put(path1, pm1);

    val path2 = Paths.get("f82d213f-9843-28eb-e040-11ac0d483e48.consensus.20160830.somatic.snv_mnv.vcf.gz");
    val pm2 = createPortalMetadata(path2, "3d709e89c8ce201e3c928eb917989aef");
    map.put(path2, pm2);

    val path3 = Paths.get("f82d213f-bc06-5b51-e040-11ac0c48687e.consensus.20160830.somatic.indel.vcf.gz");
    val pm3 = createPortalMetadata(path3,"60b91f1875424d3b4322b0fdd0529d5d");
    map.put(path3, pm3);
    return map;
  }

  @Test
  public void testSanityMd5Check(){
    val dirpath = getPath(LOCAL_STORAGE_DIR_PATH);
    val testMap = buildTestMap();
    val bypassMd5Check = false;
    val storage = newLocalStorage(dirpath,bypassMd5Check);
    for (val path : testMap.keySet()){
      val portalMetadata = testMap.get(path);
      val file = storage.getFile(portalMetadata);
    }

  }

  private void runMismatchChecksumTest(boolean bypassMd5Check){
    val dirpath = getPath(LOCAL_STORAGE_DIR_PATH);
    val path = Paths.get("f221c897-6ad0-0df9-e040-11ac0c4813ef.consensus.20160830.somatic.indel.vcf.gz");
    val portalMetadata = createPortalMetadata(path,"2234234incorrectChecksum22838" );
    val storage = newLocalStorage(dirpath,bypassMd5Check);
    val file = storage.getFile(portalMetadata);
  }

  @Test(expected = IllegalStateException.class)
  public void testMismatchMd5Check(){
    val bypassMd5Check = false;
    runMismatchChecksumTest(bypassMd5Check);
  }

  @Test
  public void testMismatchAndBypassMd5Check(){
    val bypassMd5Check = true;
    runMismatchChecksumTest(bypassMd5Check);
  }


  @Test
  @SneakyThrows
  @Ignore("used to verify number in portal")
  public void testConsensusCount(){
    val dirpath = Paths.get("/Users/rtisma/Documents/oicr/pcawgConsensusVCFs/pcawg_consensus_vcf");
    val bypassMd5Check = false;
    val storage = newLocalStorage(dirpath,bypassMd5Check);
    val f = LocalFileRestorerFactory.newFileRestorerFactory("rob.persistence");
    val portalF = PortalMetadataDaoFactory.newAllPortalMetadataDaoFactory(f);
    val portalMetadataDao = portalF.createPortalMetadataDao();
    val variantFilterFactory = VariantFilterFactory.newVariantFilterFactory(false, false);
    val persistedFactory = PersistedFactory.newPersistedFactory(f, true);
    val sampleMetadataDao = persistedFactory.newSampleMetadataDAO();
    long total = 0;
    long afterFilterTotal = 0;
    long fileCount = 0;

    val US_POS = 0;
    val NONUS_POS = 1;
    val totalVariantsCountArray = new long[]{0,0};
    val afterQualityFilterCountArray = new long[]{0,0};
    val afterTcgaFilterCountArray = new long[]{0,0};
    val totalFiles = new long[]{0,0};
    val projects = newArrayList(Sets.<String>newHashSet(), Sets.<String>newHashSet());

    val totaltotalFiles = portalMetadataDao.findAll().size();
    for (val p : portalMetadataDao.findAll()){
      val vcfFile = storage.getFile(p);
      val workflow = WorkflowTypes.parseMatch(p.getPortalFilename().getWorkflow(), false);
      if (workflow == WorkflowTypes.CONSENSUS){
        val portalFilename = p.getPortalFilename();
        val sampleMetadata = sampleMetadataDao.fetchSampleMetadata(portalFilename);
        val isUsProject = sampleMetadata.isUsProject();
        val dccProjectCode = sampleMetadata.getDccProjectCode();

        val vcfStreamFilter = newVCFStreamFilter(vcfFile.toPath(),sampleMetadata, variantFilterFactory);
        vcfStreamFilter.streamFilteredVariants().count();
        val pos = isUsProject ? US_POS : NONUS_POS;
          totalVariantsCountArray[pos] += vcfStreamFilter.getTotalVariantCounter().getCount();
          afterQualityFilterCountArray[pos] += vcfStreamFilter.getAfterQualityFilterCounter().getCount();
          afterTcgaFilterCountArray[pos] += vcfStreamFilter.getAfterTCGSFilterCounter().getCount();
          totalFiles[pos]++;
          projects.get(pos).add(dccProjectCode);
        log.info("[{}]: Processed File ({} / {}) -- isUsProject: {}",dccProjectCode, ++fileCount, totaltotalFiles, isUsProject );
      }
    }
    log.info("US Projects Summary:\n\ttotalProjects: {}\n\ttotalFiles: {}\n\ttotalVariantsCount: {}\n\tafterQualityFilterCount: {}\n\tafterTCGAFilterCount: {}",
        projects.get(US_POS).size(),
        totalFiles[US_POS],
        totalVariantsCountArray[US_POS],
        afterQualityFilterCountArray[US_POS],
        afterTcgaFilterCountArray[US_POS]);

    log.info("NON-US Projects Summary:\n\ttotalProjects: {}\n\ttotalFiles: {}\n\ttotalVariantsCount: {}\n\tafterQualityFilterCount: {}\n\tafterTCGAFilterCount: {}",
        projects.get(NONUS_POS).size(),
        totalFiles[NONUS_POS],
        totalVariantsCountArray[NONUS_POS],
        afterQualityFilterCountArray[NONUS_POS],
        afterTcgaFilterCountArray[NONUS_POS]);
  }

}
