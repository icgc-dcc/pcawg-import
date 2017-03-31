package org.icgc.dcc.pcawg.client.data.factory.impl;

import lombok.SneakyThrows;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBeanDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.factory.AbstractDaoFactory;

import static org.icgc.dcc.pcawg.client.data.factory.FileRestorer.newFileRestorer;

public class BarcodeBeanDaoFactory extends AbstractDaoFactory<BarcodeBean, BarcodeSearchRequest, BarcodeBeanDao> {

  @SneakyThrows
  public static BarcodeBeanDao buildBarcodeBeanDao (String downloadUrl, String inputFilename, String persistedFilename) {
    return new BarcodeBeanDaoFactory(downloadUrl, inputFilename, persistedFilename).getObject();
  }

    public BarcodeBeanDaoFactory(String downloadUrl, String inputFilename, String persistedFilename) {
      super(downloadUrl, inputFilename, newFileRestorer(persistedFilename));
    }

    @Override
    protected BarcodeBeanDao newObject(String filename) {
      return BarcodeBeanDao.newBarcodeBeanDao(filename);
    }

}
