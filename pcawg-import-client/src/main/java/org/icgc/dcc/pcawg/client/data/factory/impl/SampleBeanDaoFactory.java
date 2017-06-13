package org.icgc.dcc.pcawg.client.data.factory.impl;

import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.factory.AbstractDaoFactory;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetBeanDao;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorer;

import static org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetBeanDao.newSampleSheetBeanDao;
import static org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetFastDao.newSampleSheetFastDao;

// TODO: Refactor this. Throw away abstract inheritance, causing alot of headache and dirty
public class SampleBeanDaoFactory extends AbstractDaoFactory<SampleSheetBean, SampleSheetSearchRequest, SampleSheetBeanDao> {

  @SneakyThrows
  public static SampleSheetBeanDao buildSampleSheetBeanDao(String downloadUrl, String inputFilename,
      LocalFileRestorer<SampleSheetBeanDao> localFileRestorer, boolean useFast, boolean hasHeader) {
    return newSampleBeanDaoFactory(downloadUrl, inputFilename, localFileRestorer, useFast, hasHeader).getObject();
  }

  public static SampleBeanDaoFactory newSampleBeanDaoFactory(String downloadUrl, String inputFilename,
      LocalFileRestorer<SampleSheetBeanDao> localFileRestorer, boolean useFast, boolean hasHeader) {
    return new SampleBeanDaoFactory(downloadUrl, inputFilename, localFileRestorer, useFast, hasHeader);
  }

  private final boolean useFast;
  private final boolean hasHeader;

  public SampleBeanDaoFactory(String downloadUrl, String inputFilename, LocalFileRestorer<SampleSheetBeanDao> localFileRestorer,
      boolean useFast, boolean hasHeader) {
    super(downloadUrl, inputFilename, localFileRestorer);
    this.useFast = useFast;
    this.hasHeader = hasHeader;
  }

  @Override
  protected SampleSheetBeanDao newObject(String filename) {
    if (useFast){
      val fastSampleSheetDao = newSampleSheetFastDao(filename, hasHeader);
      return newSampleSheetBeanDao(fastSampleSheetDao.findAll());
    }else {
      return newSampleSheetBeanDao(filename);
    }
  }

}
