package org.icgc.dcc.pcawg.client.data.exp;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.List;

@Slf4j
public class BarcodeBeanDao2 extends AbstractFileDao<BarcodeBean, BarcodeSearchRequest2> implements Serializable,
    BarcodeDao<BarcodeBean, BarcodeSearchRequest2> {

  public static final long serialVersionUID = 1490628681L;


  private BarcodeBeanDao2(String inputFilename) {
    super(inputFilename);
//    denormalizeBeans(getBeans());
  }

  private BarcodeBeanDao2(Reader reader) {
    super(reader);
//    denormalizeBeans(getBeans());
  }

  private BarcodeBeanDao2(List<BarcodeBean> beans) {
    super(beans);
//    denormalizeBeans(getBeans());
  }

  private static void denormalizeBeans(List<BarcodeBean> beans){
    log.info("Denormalizing BarcodeBeans to lowercase uuids");
    for (val b : beans){
      b.setUuid(b.getUuid().toLowerCase());
    }
  }

  public static BarcodeBeanDao2 newBarcodeBeanDao(String inputFilename){
    return new BarcodeBeanDao2(inputFilename);
  }

  public static BarcodeBeanDao2 newBarcodeBeanDao(Reader reader){
    return new BarcodeBeanDao2(reader);
  }

  public static BarcodeBeanDao2 newBarcodeBeanDao(List<BarcodeBean> beans){
    return new BarcodeBeanDao2(beans);
  }

  @SneakyThrows
  public static BarcodeBeanDao2 restoreBarcodeBeanDao(String storedBarcodeDaoFilename){
    return (BarcodeBeanDao2) ObjectPersistance.restore(storedBarcodeDaoFilename);
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }

  @Override
  public Class<BarcodeBean> getBeanClass() {
    return BarcodeBean.class;
  }

  @Override protected BarcodeSearchRequest2 createRequestFromBean(BarcodeBean bean) {
    return BarcodeSearchRequest2.newBarcodeRequest(bean.getUuid());
  }
}
