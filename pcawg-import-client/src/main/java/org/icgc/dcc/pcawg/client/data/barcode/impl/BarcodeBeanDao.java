package org.icgc.dcc.pcawg.client.data.barcode.impl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.model.beans.BarcodeBean;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.List;

import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;

@Slf4j
public class BarcodeBeanDao extends AbstractFileDao<BarcodeBean, BarcodeSearchRequest> implements Serializable,
    BarcodeDao<BarcodeBean, BarcodeSearchRequest> {

  public static final long serialVersionUID = 1490628681L;


  private BarcodeBeanDao(String inputFilename) {
    super(inputFilename);
    makeUuidsLowercase(getBeans());
  }

  private BarcodeBeanDao(Reader reader) {
    super(reader);
    makeUuidsLowercase(getBeans());
  }

  private BarcodeBeanDao(List<BarcodeBean> beans) {
    super(beans);
    makeUuidsLowercase(getBeans());
  }

  private static void makeUuidsLowercase(List<BarcodeBean> beans){
    log.info("Making uuids for BarcodeBeans lowercase...");
    for (val b : beans){
      b.setUuid(b.getUuid().toLowerCase());
    }
  }

  public static BarcodeBeanDao newBarcodeBeanDao(String inputFilename){
    return new BarcodeBeanDao(inputFilename);
  }

  public static BarcodeBeanDao newBarcodeBeanDao(Reader reader){
    return new BarcodeBeanDao(reader);
  }

  public static BarcodeBeanDao newBarcodeBeanDao(List<BarcodeBean> beans){
    return new BarcodeBeanDao(beans);
  }

  @SneakyThrows
  public static BarcodeBeanDao restoreBarcodeBeanDao(String storedBarcodeDaoFilename){
    return (BarcodeBeanDao) ObjectPersistance.restore(storedBarcodeDaoFilename);
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }

  @Override
  public Class<BarcodeBean> getBeanClass() {
    return BarcodeBean.class;
  }

  @Override protected BarcodeSearchRequest createRequestFromBean(BarcodeBean bean) {
    return newBarcodeRequest(bean.getUuid());
  }
}
