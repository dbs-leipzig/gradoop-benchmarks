/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmarks.tpgm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsHDFSReader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated program to benchmark the query operator on temporal data.
 * The benchmark is expected to be executed on the LDBC data set.
 */
public class PatternMatchingBenchmark extends BaseTpgmBenchmark {

  private static final String OPTION_STATISTICS_PATH = "s";
  private static final String OPTION_INPUT_FORMAT = "f";
  static String STATISTICS_PATH;
  static String INPUT_FORMAT;

  static {
    OPTIONS.addOption(OPTION_STATISTICS_PATH, "statistics", true, "Path to statistics directory.");
    OPTIONS.addRequiredOption(OPTION_INPUT_FORMAT, "format", true, "Input graph format (csv, indexed).");
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, PatternMatchingBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    readBaseCMDArguments(cmd);

    STATISTICS_PATH = cmd.getOptionValue(OPTION_STATISTICS_PATH);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT);

    TemporalGraphCollection results;
    GraphStatistics statistics;

    TemporalGraph graph = readTemporalGraph(INPUT_PATH, INPUT_FORMAT);

    ExecutionEnvironment env = graph.getConfig().getExecutionEnvironment();

    String query = "MATCH (p:person)-[l:likes]->(c:comment), (c)-[r:replyOf]->(po:post) " +
      "WHERE l.val_from.after(Timestamp(2012-06-01)) AND " +
      "      l.val_from.before(Timestamp(2012-06-02)) AND " +
      "      c.val_from.after(Timestamp(2012-05-30)) AND " +
      "      c.val_from.before(Timestamp(2012-06-02)) AND " +
      "      po.val_from.after(Timestamp(2012-05-30)) AND " +
      "      po.val_from.before(Timestamp(2012-06-02))";


    if (STATISTICS_PATH != null) {
      statistics = GraphStatisticsHDFSReader.read(STATISTICS_PATH, new Configuration());
    } else {
      statistics = new GraphStatistics(1L, 1L, 1L, 1L);
    }

    results = graph.query(query, statistics);

    // only count the results and write it to a csv file
    DataSet<Tuple2<String, Long>> sum = results.getGraphHeads()
      .map(g -> new Tuple2<>("G", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {})
      // group by the element type (V or E)
      .groupBy(0)
      // sum the values
      .sum(1);

    sum.writeAsCsv(appendSeparator(OUTPUT_PATH) + "count.csv", FileSystem.WriteMode.OVERWRITE);

    env.execute(PatternMatchingBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String.format("%s|%s|%s|%s|%s", "Parallelism", "dataset", "format", "stats", "Runtime(s)");

    String tail = String.format("%s|%s|%s|%s|%s", env.getParallelism(), INPUT_PATH, INPUT_FORMAT,
      STATISTICS_PATH != null ? "yes" : "no",
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    writeToCSVFile(head, tail);
  }
}
