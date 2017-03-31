package org.icgc.dcc.pcawg.client.data.factory.impl;

import lombok.SneakyThrows;
import org.icgc.dcc.pcawg.client.data.factory.AbstractDaoFactory;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;

import static org.icgc.dcc.pcawg.client.data.factory.FileRestorer.newFileRestorer;
import static org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao.newSampleBeanDao;

public class SampleBeanDaoFactory extends AbstractDaoFactory<SampleBean, SampleSearchRequest, SampleBeanDao> {

  @SneakyThrows
  public static SampleBeanDao buildSampleBeanDao (String downloadUrl, String inputFilename, String persistedFilename) {
    return new SampleBeanDaoFactory(downloadUrl, inputFilename, persistedFilename).getObject();
  }

  public SampleBeanDaoFactory(String downloadUrl, String inputFilename, String persistedFilename) {
    super(downloadUrl, inputFilename, newFileRestorer(persistedFilename));
  }

  @Override
  protected SampleBeanDao newObject(String filename) {
    return newSampleBeanDao(filename);
  }

}
