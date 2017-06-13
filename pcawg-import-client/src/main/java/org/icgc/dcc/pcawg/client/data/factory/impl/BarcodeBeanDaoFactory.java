package org.icgc.dcc.pcawg.client.data.factory.impl;

import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetBeanDao;
import org.icgc.dcc.pcawg.client.data.factory.AbstractDaoFactory;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorer;

import static org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetBeanDao.newBarcodeSheetBeanDao;
import static org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetFastDao.newBarcodeSheetFastDao;

public class BarcodeBeanDaoFactory extends AbstractDaoFactory<BarcodeSheetBean, BarcodeSearchRequest, BarcodeSheetBeanDao> {

  @SneakyThrows
  public static BarcodeSheetBeanDao buildBarcodeSheetBeanDao(String downloadUrl, String inputFilename,
      LocalFileRestorer<BarcodeSheetBeanDao> localFileRestorer, boolean useFast, boolean hasHeader) {
    return new BarcodeBeanDaoFactory(downloadUrl, inputFilename, localFileRestorer, useFast, hasHeader).getObject();
  }

  public static BarcodeBeanDaoFactory newBarcodeBeanDaoFactory(String downloadUrl, String inputFilename,
      LocalFileRestorer<BarcodeSheetBeanDao> localFileRestorer, boolean useFast, boolean hasHeader) {
    return new BarcodeBeanDaoFactory(downloadUrl, inputFilename, localFileRestorer, useFast, hasHeader);
  }

  private final boolean useFast;
  private final boolean hasHeader;

  public BarcodeBeanDaoFactory(String downloadUrl, String inputFilename, LocalFileRestorer<BarcodeSheetBeanDao> localFileRestorer,
      boolean useFast, boolean hasHeader) {
    super(downloadUrl, inputFilename, localFileRestorer);
    this.useFast = useFast;
    this.hasHeader = hasHeader;
  }

  @Override
  protected BarcodeSheetBeanDao newObject(String filename) {
    if (useFast){
      val fastBarcodeSheetDao = newBarcodeSheetFastDao(filename, hasHeader);
      return newBarcodeSheetBeanDao(fastBarcodeSheetDao.findAll());
    }else {
      return newBarcodeSheetBeanDao(filename);
    }
  }

}
