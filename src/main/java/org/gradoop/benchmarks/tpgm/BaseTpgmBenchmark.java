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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.benchmarks.AbstractRunner;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.impl.TemporalGraph;

import java.io.File;
import java.io.IOException;

/**
 * Base class for all TPGM benchmarks.
 */
abstract class BaseTpgmBenchmark extends AbstractRunner {
  /**
   * Option to declare path to indexed input graph
   */
  static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare output path to statistics csv file
   */
  static final String OPTION_CSV_PATH = "c";
  /**
   * Option to count the result sets instead of writing it
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
   * Used count only flag
   */
  static boolean COUNT_RESULT;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true, "Path to source files.");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true, "Path to output file.");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv statistics file (will be created if not available).");
    OPTIONS.addOption(OPTION_COUNT_RESULT, "count", false, "Only count result instead of writing.");
  }

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

      OUTPUT_PATH = getPath(OUTPUT_PATH);

      sum.writeAsCsv(OUTPUT_PATH + "/count.csv", FileSystem.WriteMode.OVERWRITE);
    } else {
      // write graph to sink
      // We want to reuse the metadata file because the properties won't change
      String metadataFile = getPath(INPUT_PATH) + "metadata.csv";
      TemporalCSVDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, metadataFile, conf);
      sink.write(temporalGraph, true);
    }
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
