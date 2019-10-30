/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.benchmarks.AbstractRunner;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;

import java.io.File;
import java.io.IOException;

/**
 * Base class for the TPGM benchmarks.
 */
abstract class BaseTpgmBenchmark extends AbstractRunner {
  /**
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare output path to csv file with execution results
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to count the result sets instead of writing them
   */
  static final String OPTION_COUNT_RESULT = "n";

  /**
   * Used input path
   */
  static String INPUT_PATH;
  /**
   * Used output path
   */
  static String OUTPUT_PATH;
  /**
   * Used csv path
   */
  static String CSV_PATH;
  /**
   * Used count only flag. The graph elements will be counted only if this is set to true.
   */
  static boolean COUNT_RESULT;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true, "Path to source files.");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true, "Path to output file.");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv result file (will be created if not available).");
    OPTIONS.addOption(OPTION_COUNT_RESULT, "count", false, "Only count the results instead of writing them.");
  }

  /**
   * A function to output the results. There are two ways according to the flag COUNT_RESULT.
   * <p>
   * If it is set to TRUE, the vertices and edges of the graph will be counted and the result will be written
   * in a file named 'count.csv' inside the given output directory.
   * <p>
   * If it it set to FALSE, the whole graph will be written in the output directory by the
   * {@link TemporalCSVDataSink}.
   *
   * @param temporalGraph the temporal graph to write
   * @param conf the temporal gradoop config
   * @throws IOException if writing the result fails
   */
  static void writeOrCountGraph(TemporalGraph temporalGraph, GradoopFlinkConfig conf) throws IOException {
    if (COUNT_RESULT) {
      // only count the results and write it to a csv file
      DataSet<Tuple2<String, Long>> sum = temporalGraph.getVertices()
        .map(v -> new Tuple2<>("V", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {})
        .union(temporalGraph.getEdges()
          .map(e -> new Tuple2<>("E", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {}))
        // group by the element type (V or E)
        .groupBy(0)
        // sum the values
        .sum(1);

      sum.writeAsCsv(getPath(OUTPUT_PATH) + "count.csv", FileSystem.WriteMode.OVERWRITE);
    } else {
      // write graph to sink
      TemporalDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, conf);
      sink.write(temporalGraph, true);
    }
  }

  /**
   * Reads main arguments (input path, output path, csv path and count flag) from command line.
   *
   * @param cmd command line
   */
  static void readBaseCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH     = cmd.getOptionValue(OPTION_CSV_PATH);
    COUNT_RESULT = cmd.hasOption(OPTION_COUNT_RESULT);
  }

  /**
   * Get the path with a separator char at the end.
   *
   * @param path the path to append the separator if it do not have it
   * @return the path as string with a separator at the end
   */
  private static String getPath(String path) {
    return path.endsWith(File.separator) ? path : path + File.separator;
  }

}
