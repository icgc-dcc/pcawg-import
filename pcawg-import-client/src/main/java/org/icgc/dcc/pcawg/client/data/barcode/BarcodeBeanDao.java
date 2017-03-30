package org.icgc.dcc.pcawg.client.data.barcode;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.util.List;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@Slf4j
public class BarcodeBeanDao extends AbstractFileDao<BarcodeBean, String> implements Serializable, BarcodeDao<BarcodeBean, String> {

  public static final long serialVersionUID = 1490628681L;

  private BarcodeBeanDao(String inputFilename) {
    super(inputFilename);
    denormalizeBeans(getBeans());
  }

  private BarcodeBeanDao(Reader reader) {
    super(reader);
    denormalizeBeans(getBeans());
  }

  private BarcodeBeanDao(List<BarcodeBean> beans) {
    super(beans);
    denormalizeBeans(getBeans());
  }

  private static void denormalizeBeans(List<BarcodeBean> beans){
    log.info("Denormalizing BarcodeBeans to lowercase uuids");
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


  @Override
  public List<BarcodeBean> find(String uuid){
    return getBeans().stream()
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

  @Override public List<BarcodeBean> findAll() {
    return ImmutableList.copyOf(getBeans());
  }
}
