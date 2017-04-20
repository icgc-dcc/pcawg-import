package org.icgc.dcc.pcawg.client.core.transformer.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.core.transformer.Transformer;
import org.icgc.dcc.pcawg.client.core.writer.impl.LocalWriterContext;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.BaseTransformer.newBaseTransformer;

/**
 * This class creates a pool of Writers (lazily) and records which one was actually writen to.
 * If a writer was not writen to at all, then when close is called, those unwritten files (which will be touched to the filesystem) are deleted
 * The life span of this object is fully dependant on dccProjectCode. Since we are iterating through each project code 1by1, all the writers
 * for a specific dccProjectCode are opened upon call, and closed when this object is closed.
 * @param <T>
 */
@RequiredArgsConstructor(staticName = "newDccTransformer")
@ToString
@EqualsAndHashCode
public final class DccTransformer<T> implements Transformer<DccTransformerContext<T>> {

  private static <T> Map<WorkflowTypes, Transformer<T>> createEmptyTransformerMap(){
    return Maps.newEnumMap(WorkflowTypes.class);
  }

  /**
   * Dependencies
   */
  @NonNull private final FsController<Path> fsController;
  @NonNull private final TSVConverter<T> tsvConverter;

  /**
   * Configuration
   */
  @NonNull private final Path   outputDirectory;
  @NonNull private final String dccProjectCode;
  @NonNull private final String fileNamePrefix;
  @NonNull private final String fileExtension;
  private final boolean append;

  /**
   * State
   */
  private Map<WorkflowTypes, Transformer<T>> transformerMap = Maps.newEnumMap(WorkflowTypes.class);
  private Map<WorkflowTypes, Boolean> hasBeenWritenMap= Maps.newEnumMap(WorkflowTypes.class);

  public List<Path> getWrittenPaths(){
    val out = ImmutableList.<Path>builder();
    for (val entry : hasBeenWritenMap.entrySet()){
      val w = entry.getKey();
      val written = entry.getValue();
      if (written){
        out.add(getOutputPath(w));
      }
    }
    return out.build();
  }

  @Override
  public void transform(DccTransformerContext<T> ctx) throws IOException {
    val ssmClassification = ctx.getSSMPrimaryClassification();
    transform(ssmClassification.getWorkflowType(), ctx.getObject());
  }

  public void transform(WorkflowTypes workflowType, T t) throws IOException {
    val transformer = getTransformer(workflowType);
    transformer.transform(t);
    recordWriteTo(workflowType);
  }

  private void recordWriteTo(WorkflowTypes workflowType){
    if (hasBeenWritenMap == null){
      hasBeenWritenMap = Maps.newEnumMap(WorkflowTypes.class);
    }
    hasBeenWritenMap.put(workflowType, true);
  }

  @SneakyThrows
  private Transformer<T> createNewTransformer(WorkflowTypes workflowType){
    val outputPath = getOutputPath(workflowType);
    boolean fileExists = fsController.exists(outputPath);
    val writeHeader = !append || !fileExists ;
    val writer = createNewFileWriter(workflowType);
    return newBaseTransformer(tsvConverter,writer, writeHeader);
  }

  private Path getOutputPath(WorkflowTypes workflowType){
    return Paths.get(createOutputFilename(workflowType, dccProjectCode));
  }

  @SneakyThrows
  private Writer createNewFileWriter(WorkflowTypes workflowType){
    val outputPath = getOutputPath(workflowType);
    val writerContext = LocalWriterContext.builder()
        .path(outputPath)
        .append(append)
        .build();
    return fsController.createWriter(writerContext);
  }

  private String createOutputFilename(WorkflowTypes workflowType, String dccProjectCode){
    return PATH.join(outputDirectory, dccProjectCode, createOutputTsvFilename(workflowType));

  }
  private String createOutputTsvFilename( WorkflowTypes workflowType){
    return DOT.join(fileNamePrefix, workflowType.getName(), fileExtension);
  }


  private Transformer<T> getTransformer(WorkflowTypes workflowType){
    if (transformerMap == null){
      transformerMap = createEmptyTransformerMap();
    }

    if (!transformerMap.containsKey(workflowType)){
      transformerMap.put(workflowType, createNewTransformer(workflowType));
    }
    return transformerMap.get(workflowType);
  }

  @Override
  public void close() throws IOException {
    // Close all connections
    for(val transformer : transformerMap.values()){
      if (transformer != null){
        transformer.close();
      }
    }
    this.transformerMap = createEmptyTransformerMap();

    //Delete any files that were opened, but never written to
    for (val workflowType : hasBeenWritenMap.keySet()){
      val hasBeenWritten = hasBeenWritenMap.getOrDefault(workflowType, false);
      if (!hasBeenWritten) {
        val outputPath = getOutputPath(workflowType);
        fsController.deleteIfExists(outputPath);
        hasBeenWritenMap.remove(workflowType);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    for(val transformer : transformerMap.values()){
      if (transformer != null){
        transformer.flush();
      }
    }
  }

}
