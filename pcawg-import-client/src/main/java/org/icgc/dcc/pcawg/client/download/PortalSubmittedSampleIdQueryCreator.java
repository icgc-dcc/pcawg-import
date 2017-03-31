package org.icgc.dcc.pcawg.client.download;

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
public class PortalSubmittedSampleIdQueryCreator
    implements ObjectNodeConverter, Iterable<PortalSubmittedSampleIdQueryCreator> {

  private static final int DEFAULT_FETCH_SIZE = 100;

  public static final PortalSubmittedSampleIdQueryCreator newSubmitterSampleIdQueryCreator(Set<String> submittedSampleIds){
    return new PortalSubmittedSampleIdQueryCreator(submittedSampleIds, DEFAULT_FETCH_SIZE);
  }
  public static final PortalSubmittedSampleIdQueryCreator newSubmitterSampleIdQueryCreator(Set<String> submittedSampleIds,
      final int fetchSize){
    return new PortalSubmittedSampleIdQueryCreator(submittedSampleIds, fetchSize);
  }

  @NonNull
  private final Set<String> submitterSampleIds;

  private final int fetchSize;

  @Override
  public Iterator<PortalSubmittedSampleIdQueryCreator> iterator() {
    val internalIterator = new PartitioningIterator<String>(newArrayList(submitterSampleIds), fetchSize);
    return new QueryIterator<PortalSubmittedSampleIdQueryCreator, String>(
        internalIterator, list -> newSubmitterSampleIdQueryCreator(newHashSet(list)) );
  }

  @Override
  public ObjectNode toObjectNode(){
    return object()
        .with("file",
            object()
                .with("submittedSampleId", createIs(toStringArray(submitterSampleIds)))
                .with("fileFormat", createIs("BAM"))
                .with("study", createIs("PCAWG"))
                .with("experimentalStrategy", createIs("WGS"))
            )
        .end();
  }

}
