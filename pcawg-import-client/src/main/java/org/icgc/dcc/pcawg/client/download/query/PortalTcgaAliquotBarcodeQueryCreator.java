package org.icgc.dcc.pcawg.client.download.query;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.ObjectNodeConverter;
import org.icgc.dcc.pcawg.client.utils.PartitioningIterator;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.pcawg.client.utils.Strings.toStringArray;

@RequiredArgsConstructor(access = PRIVATE)
@Value
@Slf4j
public class PortalTcgaAliquotBarcodeQueryCreator implements ObjectNodeConverter, Iterable<PortalTcgaAliquotBarcodeQueryCreator> {

  private static final int DEFAULT_FETCH_SIZE = 100;

  public static final PortalTcgaAliquotBarcodeQueryCreator newTcgaAliquotBarcodeQueryCreator(Set<String> tcgaAliquotBarcodes){
    return new PortalTcgaAliquotBarcodeQueryCreator(tcgaAliquotBarcodes, DEFAULT_FETCH_SIZE);
  }

  public static final PortalTcgaAliquotBarcodeQueryCreator newTcgaAliquotBarcodeQueryCreator(Set<String> tcgaAliquotBarcodes, final int fetchSize){
    return new PortalTcgaAliquotBarcodeQueryCreator(tcgaAliquotBarcodes, fetchSize);
  }

  @NonNull
  private final Set<String> tcgaAliquotBarcodes;

  private final int fetchSize;

  @Override
  public ObjectNode toObjectNode(){
    return object()
        .with("file",
            object()
                .with("tcgaAliquotBarcode", createIs(toStringArray(tcgaAliquotBarcodes)))
                .with("study", createIs("PCAWG"))
                .with("experimentalStrategy", createIs("WGS"))
                .with("fileFormat", createIs("BAM"))
            )
        .end();
  }

  @Override
  public Iterator<PortalTcgaAliquotBarcodeQueryCreator> iterator() {
    val internalIterator = new PartitioningIterator<String>(newArrayList(tcgaAliquotBarcodes), fetchSize);
    return new QueryIterator<PortalTcgaAliquotBarcodeQueryCreator,String>( internalIterator,
        list -> newTcgaAliquotBarcodeQueryCreator(newHashSet(list)));
  }

}
