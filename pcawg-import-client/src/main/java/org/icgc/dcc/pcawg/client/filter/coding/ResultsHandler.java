package org.icgc.dcc.pcawg.client.filter.coding;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.release.job.annotate.model.ConsequenceType;
import org.icgc.dcc.release.job.annotate.parser.SnpEffectParser;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.release.job.annotate.model.ConsequenceType.*;

@Slf4j
public class ResultsHandler implements Runnable {

  private static final Pattern SKIP_ANNOTATION_PATTERN = compile("^#|Reading cancer samples pedigree from VCF header");
  private static final List<ConsequenceType> CODING_TYPES = ImmutableList.of(
      FRAMESHIFT_VARIANT,
      MISSENSE_VARIANT,
      INITIATOR_CODON_VARIANT,
      STOP_GAINED,
      STOP_LOST,
      RARE_AMINO_ACID_VARIANT,
      CODING_SEQUENCE_VARIANT,
      NON_CANONICAL_START_CODON,
      DISRUPTIVE_INFRAME_DELETION,
      INFRAME_DELETION,
      DISRUPTIVE_INFRAME_INSERTION,
      INFRAME_INSERTION,
      SYNONYMOUS_VARIANT,
      STOP_RETAINED_VARIANT
  );

  /**
   * Constants
   */
  private static String INFO_EFF_FIELD = "EFF=";

  /**
   * Dependencies.
   */
  @NonNull
  private final InputStream input;

  /**
   * State.
   */
  @NonNull
  private final BlockingQueue<Boolean> queue;

  public ResultsHandler(@NonNull InputStream input, @NonNull BlockingQueue<Boolean> queue, String geneBuildVersion) {
    this.input = input;
    this.queue = queue;
  }

  @Override
  @SneakyThrows
  public void run() {
    val reader = new BufferedReader(new InputStreamReader(input, UTF_8));
    String line;
    while ((line = reader.readLine()) != null) {
      if (isSkipLine(line)) {
        continue;
      }

      queue.put(isCoding(line));
    }
  }

  private boolean isCoding(String line) {
    String[] lineSplit = line.split(INFO_EFF_FIELD);
    if (lineSplit.length < 2) {
      return false;
    }

    val effField = lineSplit[1];
    val effects = stream(effField.split(","))
        .map(SnpEffectParser::parse)
        .flatMap(Collection::stream)
        .filter(e -> CODING_TYPES.contains(e.getConsequenceType()))
        .collect(toImmutableList());

    return effects.size() > 0;
  }

  private static boolean isSkipLine(String line) {
    val matcher = SKIP_ANNOTATION_PATTERN.matcher(line);

    return matcher.find();
  }

}
