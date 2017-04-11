package org.icgc.dcc.pcawg.client.data.barcode.impl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.utils.persistance.ObjectPersistance;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.List;

import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;

@Slf4j
public class BarcodeSheetBeanDao extends AbstractFileDao<BarcodeSheetBean, BarcodeSearchRequest> implements Serializable,
    BarcodeSheetDao<BarcodeSheetBean, BarcodeSearchRequest> {

  public static final long serialVersionUID = 1490628681L;


  private BarcodeSheetBeanDao(String inputFilename) {
    super(inputFilename);
    makeUuidsLowercase(getBeans());
  }

  private BarcodeSheetBeanDao(Reader reader) {
    super(reader);
    makeUuidsLowercase(getBeans());
  }

  private BarcodeSheetBeanDao(List<BarcodeSheetBean> beans) {
    super(beans);
    makeUuidsLowercase(getBeans());
  }

  private static void makeUuidsLowercase(List<BarcodeSheetBean> beans){
    log.info("Making uuids for BarcodeBeans lowercase...");
    for (val b : beans){
      b.setUuid(b.getUuid().toLowerCase());
    }
  }

  public static BarcodeSheetBeanDao newBarcodeSheetBeanDao(String inputFilename){
    return new BarcodeSheetBeanDao(inputFilename);
  }

  public static BarcodeSheetBeanDao newBarcodeSheetBeanDao(Reader reader){
    return new BarcodeSheetBeanDao(reader);
  }

  public static BarcodeSheetBeanDao newBarcodeSheetBeanDao(List<BarcodeSheetBean> beans){
    return new BarcodeSheetBeanDao(beans);
  }

  @SneakyThrows
  public static BarcodeSheetBeanDao restoreBarcodeBeanDao(String storedBarcodeDaoFilename){
    return (BarcodeSheetBeanDao) ObjectPersistance.restore(storedBarcodeDaoFilename);
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }

  @Override
  public Class<BarcodeSheetBean> getBeanClass() {
    return BarcodeSheetBean.class;
  }

  @Override protected BarcodeSearchRequest createRequestFromBean(BarcodeSheetBean bean) {
    return newBarcodeRequest(bean.getUuid());
  }
}
