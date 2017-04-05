package org.icgc.dcc.pcawg.client.data.factory.impl;

import lombok.SneakyThrows;
import org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetBeanDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.factory.AbstractDaoFactory;

import static org.icgc.dcc.pcawg.client.utils.FileRestorer.newFileRestorer;

public class BarcodeBeanDaoFactory extends AbstractDaoFactory<BarcodeSheetBean, BarcodeSearchRequest, BarcodeSheetBeanDao> {

  @SneakyThrows
  public static BarcodeSheetBeanDao buildBarcodeSheetBeanDao(String downloadUrl, String inputFilename, String persistedFilename) {
    return new BarcodeBeanDaoFactory(downloadUrl, inputFilename, persistedFilename).getObject();
  }

  public BarcodeBeanDaoFactory(String downloadUrl, String inputFilename, String persistedFilename) {
    super(downloadUrl, inputFilename, newFileRestorer(persistedFilename));
  }

  @Override
  protected BarcodeSheetBeanDao newObject(String filename) {
    return BarcodeSheetBeanDao.newBarcodeSheetBeanDao(filename);
  }

}
