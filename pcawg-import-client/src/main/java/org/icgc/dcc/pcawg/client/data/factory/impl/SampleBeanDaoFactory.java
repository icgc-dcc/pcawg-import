package org.icgc.dcc.pcawg.client.data.factory.impl;

import lombok.SneakyThrows;
import org.icgc.dcc.pcawg.client.data.factory.AbstractDaoFactory;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;

import static org.icgc.dcc.pcawg.client.utils.FileRestorer.newFileRestorer;

public class SampleBeanDaoFactory extends AbstractDaoFactory<SampleSheetBean, SampleSheetSearchRequest, SampleSheetBeanDao> {

  @SneakyThrows
  public static SampleSheetBeanDao buildSampleSheetBeanDao(String downloadUrl, String inputFilename, String persistedFilename) {
    return new SampleBeanDaoFactory(downloadUrl, inputFilename, persistedFilename).getObject();
  }

  public SampleBeanDaoFactory(String downloadUrl, String inputFilename, String persistedFilename) {
    super(downloadUrl, inputFilename, newFileRestorer(persistedFilename));
  }

  @Override
  protected SampleSheetBeanDao newObject(String filename) {
    return SampleSheetBeanDao.newSampleSheetBeanDao(filename);
  }

}
