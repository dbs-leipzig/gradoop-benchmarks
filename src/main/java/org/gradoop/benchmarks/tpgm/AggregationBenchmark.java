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
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.impl.operators.tpgm.aggregation.MaxEdgeTime;
import org.gradoop.flink.model.impl.operators.tpgm.aggregation.MaxTime;
import org.gradoop.flink.model.impl.operators.tpgm.aggregation.MaxVertexTime;
import org.gradoop.flink.model.impl.operators.tpgm.aggregation.MinEdgeTime;
import org.gradoop.flink.model.impl.operators.tpgm.aggregation.MinTime;
import org.gradoop.flink.model.impl.operators.tpgm.aggregation.MinVertexTime;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized TPGM aggregation benchmark.
 */
public class AggregationBenchmark extends BaseTpgmBenchmark {

  /**
   * Main program to run the benchmark. Arguments are the available options.
   * Example: {@code /path/to/flink run -c org.gradoop.benchmark.tpgm.SnapshotBenchmark
   * path/to/gradoop-examples.jar -i hdfs:///graph -o hdfs:///output -c results.csv}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, AggregationBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read graph
    DataSource source = new CSVDataSource(INPUT_PATH, conf);
    TemporalGraph graph = source.getTemporalGraph();

    // we consider valid time only
    TemporalAttribute valid = TemporalAttribute.VALID_TIME;

    // get the diff
    TemporalGraph aggregate = graph.aggregate(
      new MinVertexTime("minVertexValidFrom", valid, TemporalAttribute.Field.FROM),
      new MinEdgeTime("minEdgeValidFrom", valid, TemporalAttribute.Field.FROM),
      new MaxTime("maxValidFrom", valid, TemporalAttribute.Field.FROM),
      new MaxVertexTime("maxVertexValidTo", valid, TemporalAttribute.Field.TO),
      new MaxEdgeTime("maxEdgeValidTo", valid, TemporalAttribute.Field.TO),
      new MinTime("minValidTo", valid, TemporalAttribute.Field.TO));
    // todo: add 3 average aggregations here

    // write graph
    writeOrCountGraph(aggregate, conf);

    // execute and write job statistics
    env.execute(AggregationBenchmark.class.getSimpleName() + "P:" + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH     = cmd.getOptionValue(OPTION_CSV_PATH);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s%n",
        "Parallelism",
        "dataset",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s%n",
        env.getParallelism(),
        INPUT_PATH,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }
}
