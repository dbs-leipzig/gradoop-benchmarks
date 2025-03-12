/*
 * Copyright © 2014 - 2024 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.benchmarks.utils.GradoopFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized TPGM single source earliest arrival benchmark.
 */
public class SingleSourceEarliestArrivalBenchmark extends BaseTpgmBenchmark {

  /**
   * Option to declare source id
   */
  private static String OPTION_SRC_ID = "s";

  /**
   * Option to declare maximum iterations
   */
  private static  String OPTION_MAX_ITERATIONS = "m";

  /**
   * Option to declare vertex property key
   */
  private static  String OPTION_VERTEX_PROPERTY= "v";

  /**
   * Option to declare overlap
   */
  private static  String OPTION_OVERLAP = "l";

  /**
   * Option to declare starttime
   */
  private static  String OPTION_STARTTIME= "t";

  /**
   * Option to declare interval
   */
  private static String OPTION_INTERVAL = "iv";

  /**
   * ID of the source vertex
   */
  private static GradoopId SRC_ID;

  /**
   * number of maximum iterations
   */
  private static int MAX_ITERATIONS;

  /**
   * vertex property to store earliest arrival time at vertices
   */
  private static String VERTEX_PROPERTY;

  /**
   * allow temporal overlap of edges
   */
  private static boolean OVERLAP;

  /**
   * valid time at edge is interval or timestamp
   */
  private static boolean INTERVAL;

  /**
   * starttime at start vertex
   */
  private static long STARTTIME;

  static {
    OPTIONS.addRequiredOption(OPTION_SRC_ID, "SourceVertex", true, "VertexID to start from");
    OPTIONS.addRequiredOption(OPTION_MAX_ITERATIONS, "maxIterations", true, "Maximum Iterations on algorithm");
    OPTIONS.addRequiredOption(OPTION_VERTEX_PROPERTY, "VertexPorperty", true, "Property for output");
    OPTIONS.addOption(OPTION_OVERLAP, "Overlap", true, "Time overlap between Edges (TRUE or FALSE)");
    OPTIONS.addRequiredOption(OPTION_STARTTIME, "Starttime", true, "Timestamp at start of algorithm");
    OPTIONS.addRequiredOption(OPTION_INTERVAL, "Interval", true, "(TRUE) Interval or (FALSE) Timestamp");
  }

  /**
   * Run the Benchmarks
   * @param args program arguments
   * @throws Exception in case of an error
   */
  public static void main(String[] args) throws Exception {

    CommandLine cmd = parseArguments(args, SingleSourceEarliestArrivalBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read cmd arguments
    readBaseCMDArguments(cmd);
    readCMDArguments(cmd);

    // read graph
    TemporalGraph graph = readTemporalGraph(INPUT_PATH, GradoopFormat.getByName(INPUT_FORMAT));

    //get SSEA
    TemporalGraph ssea = graph.singleSourceEarliestArrival(SRC_ID, MAX_ITERATIONS, VERTEX_PROPERTY, INTERVAL, STARTTIME, OVERLAP);

    // create gradoop config
    TemporalGradoopConfig conf = ssea.getConfig();
    ExecutionEnvironment env = conf.getExecutionEnvironment();

    // write graph
    writeOrCountGraph(ssea, conf);

    // execute and write job statistics
    env.execute(SingleSourceEarliestArrivalBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    SRC_ID = GradoopId.fromString(cmd.getOptionValue(OPTION_SRC_ID));
    MAX_ITERATIONS = Integer.parseInt(cmd.getOptionValue(OPTION_MAX_ITERATIONS));
    VERTEX_PROPERTY = cmd.getOptionValue(OPTION_VERTEX_PROPERTY);
    OVERLAP =Boolean.parseBoolean(cmd.getOptionValue(OPTION_OVERLAP));
    STARTTIME = Long.parseLong(cmd.getOptionValue(OPTION_STARTTIME));
    INTERVAL = Boolean.parseBoolean(cmd.getOptionValue(OPTION_INTERVAL));
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String
            .format("%s|%s|%s|%s|%s|%s|%s|%s|%s",
                    "Parallelism",
                    "dataset",
                    "srcVertexId",
                    "maxIterations",
                    "vertexProperty",
                    "interval",
                    "overlap",
                    "starttime",
                    "Runtime(s)");

    String tail = String
            .format("%s|%s|%s|%s|%s|%s|%s|%s|%s",
                    env.getParallelism(),
                    INPUT_PATH,
                    SRC_ID,
                    MAX_ITERATIONS,
                    VERTEX_PROPERTY,
                    INTERVAL,
                    OVERLAP,
                    STARTTIME,
                    env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    writeToCSVFile(head, tail);
  }

}
