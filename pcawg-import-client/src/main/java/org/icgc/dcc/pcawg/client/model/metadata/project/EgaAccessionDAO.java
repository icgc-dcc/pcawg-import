package org.icgc.dcc.pcawg.client.model.metadata.project;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;

import java.io.File;
import java.io.FileReader;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

public class EgaAccessionDAO {

  private static final char SEPERATOR = '\t';
  @Getter private final String inputDirname;
  @Getter private final String globPattern;

  /**
   * State
   */
  @Getter
  private boolean initialized = false;

  private File inputDirHandle;
  private Map<EgaSearchRequest, List<EgaBean>> lookupMap;


  public EgaAccessionDAO(@NonNull String inputDirname, @NonNull String globPattern){
    this.inputDirname = inputDirname;
    this.globPattern = globPattern;
    this.inputDirHandle = Paths.get(inputDirname).toFile();
    checkState(inputDirHandle.exists(),"The inputDirname [%s] does not exist", inputDirHandle.getAbsolutePath());
    checkState(inputDirHandle.isDirectory(),"The inputDirname [%s] is not a directory", inputDirHandle.getAbsolutePath());
  }

  public void init(){
    this.lookupMap = getPaths().stream()
        .map(EgaAccessionDAO::process)
        .flatMap(Collection::stream)
        .collect(groupingBy(EgaSearchRequest::fromEgaBean));
    initialized = true;
  }

  public Optional<EgaBean> search(EgaSearchRequest request){
    checkState(lookupMap.containsKey(request), "The lookup table does not contain the search request [%s]", request);
    val list = lookupMap.get(request).stream()
        .filter(x -> x.getFile().endsWith(".bam.bai")).collect(
        toImmutableList());
    val isValidSize = list.size() <2 ;
    checkState(isValidSize, "The SearchRequest [%s] is not unique (i.e there is more than 1 result for this request): %s ", request,
        list.stream()
            .map(EgaBean::toString)
            .collect(joining("\n")) );
    return list.size() == 0 ? Optional.empty() : Optional.of(list.get(0));
  }

  @SneakyThrows
  private static List<EgaBean> process(Path path){
    val file = path.toFile();
    val reader = new CSVReader(new FileReader(file), SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<EgaBean>();
    strategy.setType(EgaBean.class);
    val csvToBean = new CsvToBean<EgaBean>();
    return csvToBean.parse(strategy, reader);
  }

  public List<EgaBean> getAll(){
    return lookupMap.values().stream()
        .flatMap(List::stream)
        .collect(toImmutableList());
  }


  @SneakyThrows
  private List<Path> getPaths(){
    val fileVisitor = SearchFileVisitor.newSearchFileVisitor(globPattern);
    Files.walkFileTree(inputDirHandle.toPath() , fileVisitor);
    return fileVisitor.getMatchingPaths();
  }


  private static class SearchFileVisitor extends SimpleFileVisitor<Path>{

    public static SearchFileVisitor newSearchFileVisitor(String globPattern){
      return new SearchFileVisitor(globPattern);
    }
    private final PathMatcher pathMatcher;

    private List<Path> matchingPaths = Lists.newArrayList();

    private SearchFileVisitor(String globPattern) {
      this.pathMatcher = FileSystems.getDefault().getPathMatcher(
          "glob:" + globPattern);
    }

    @Override
    public FileVisitResult visitFile(Path filePath,
        BasicFileAttributes basicFileAttrib) {
      if (pathMatcher.matches(filePath.getFileName())) {
        matchingPaths.add(filePath);
      }
      return FileVisitResult.CONTINUE;
    }

    public List<Path> getMatchingPaths(){
      return ImmutableList.copyOf(matchingPaths);
    }

  }

  @Value
  @Builder
  public static class EgaSearchRequest {

    public static EgaSearchRequest fromEgaBean(EgaBean b){
      return new EgaSearchRequest(b.getIcgc_project_code(),
          b.getAliquot_id(),
          b.getSubmitter_sample_id(),
          b.getIcgc_sample_id());
    }
    @NonNull private final String dccProjectCode;
    @NonNull private final String aliquotId;
    @NonNull private final String submitterSampleId;
    @NonNull private final String icgcSampleId;

  }

}
