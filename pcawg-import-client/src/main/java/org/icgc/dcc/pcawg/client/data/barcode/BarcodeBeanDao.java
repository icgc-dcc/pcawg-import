package org.icgc.dcc.pcawg.client.data.barcode;

import lombok.SneakyThrows;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.List;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

public class BarcodeBeanDao extends AbstractFileDao<BarcodeBean, String> implements Serializable, BarcodeDao<BarcodeBean, String> {

  public static final long serialVersionUID = 1490628681L;

  private BarcodeBeanDao(String inputFilename) {
    super(inputFilename);
  }

  private BarcodeBeanDao(Reader reader) {
    super(reader);
  }

  public static BarcodeBeanDao newBarcodeBeanDao(String inputFilename){
    return new BarcodeBeanDao(inputFilename);
  }

  public static BarcodeBeanDao newBarcodeBeanDao(Reader reader){
    return new BarcodeBeanDao(reader);
  }

  @SneakyThrows
  public static BarcodeBeanDao restoreBarcodeBeanDao(String storedBarcodeDaoFilename){
    return (BarcodeBeanDao) ObjectPersistance.restore(storedBarcodeDaoFilename);
  }

  @Override
  public List<BarcodeBean> find(String uuid){
    return getData().stream()
        .filter(b -> b.getUuid().equals(uuid))
        .collect(toImmutableList());
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }

  @Override
  public Class<BarcodeBean> getBeanClass() {
    return BarcodeBean.class;
  }


}
