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
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.predicates.All;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.model.impl.functions.predicates.Between;
import org.gradoop.temporal.model.impl.functions.predicates.ContainedIn;
import org.gradoop.temporal.model.impl.functions.predicates.CreatedIn;
import org.gradoop.temporal.model.impl.functions.predicates.DeletedIn;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;
import org.gradoop.temporal.model.impl.functions.predicates.ValidDuring;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized TPGM snapshot benchmark.
 */
public class SnapshotBenchmark extends BaseTpgmBenchmark {
  /**
   * String representation of the query type {@link All}.
   */
  private static final String TYPE_ALL = "all";
  /**
   * String representation of the query type {@link AsOf}.
   */
  private static final String TYPE_AS_OF = "asof";
  /**
   * String representation of the query type {@link Between}.
   */
  private static final String TYPE_BETWEEN = "between";
  /**
   * String representation of the query type {@link ContainedIn}.
   */
  private static final String TYPE_CONTAINED_IN = "containedin";
  /**
   * String representation of the query type {@link CreatedIn}.
   */
  private static final String TYPE_CREATED_IN = "createdin";
  /**
   * String representation of the query type {@link DeletedIn}.
   */
  private static final String TYPE_DELETED_IN = "deletedin";
  /**
   * String representation of the query type {@link FromTo}.
   */
  private static final String TYPE_FROM_TO = "fromto";
  /**
   * String representation of the query type {@link ValidDuring}.
   */
  private static final String TYPE_VALID_DURING = "validduring";

  /**
   * Option to declare verification
   */
  private static final String OPTION_VERIFICATION = "v";
  /**
   * Option to declare query from timestamp
   */
  private static final String OPTION_QUERY_FROM = "f";
  /**
   * Option to declare query to timestamp
   */
  private static final String OPTION_QUERY_TO = "t";
  /**
   * Option to declare query type
   */
  private static final String OPTION_QUERY_TYPE = "y";

  /**
   * Used verification flag
   */
  private static boolean VERIFICATION;
  /**
   * Used from timestamp in milliseconds
   */
  private static Long QUERY_FROM;
  /**
   * Used to timestamp in milliseconds
   */
  private static Long QUERY_TO;
  /**
   * Used query type
   */
  private static String QUERY_TYPE;

  static {
    OPTIONS.addRequiredOption(OPTION_QUERY_TYPE, "type", true, "Used query type");
    OPTIONS.addOption(OPTION_VERIFICATION, "verification", false, "Verify Snapshot with join.");
    OPTIONS.addOption(OPTION_QUERY_FROM, "from", true, "Used query from timestamp [ms]");
    OPTIONS.addOption(OPTION_QUERY_TO, "to", true, "Used query to timestamp [ms]");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   * Example: {@code /path/to/flink run -c org.gradoop.benchmarks.tpgm.SnapshotBenchmark
   * path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv
   * -f 1287000000000 -y asof}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SnapshotBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readBaseCMDArguments(cmd);
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    TemporalGradoopConfig conf = TemporalGradoopConfig.createConfig(env);

    // read graph
    TemporalDataSource source = new TemporalCSVDataSource(INPUT_PATH, conf);
    TemporalGraph graph = source.getTemporalGraph();

    // get temporal predicate
    TemporalPredicate temporalPredicate;

    switch (QUERY_TYPE) {
    case TYPE_AS_OF:
      temporalPredicate = new AsOf(QUERY_FROM);
      break;
    case TYPE_BETWEEN:
      temporalPredicate = new Between(QUERY_FROM, QUERY_TO);
      break;
    case TYPE_CONTAINED_IN:
      temporalPredicate = new ContainedIn(QUERY_FROM, QUERY_TO);
      break;
    case TYPE_CREATED_IN:
      temporalPredicate = new CreatedIn(QUERY_FROM, QUERY_TO);
      break;
    case TYPE_DELETED_IN:
      temporalPredicate = new DeletedIn(QUERY_FROM, QUERY_TO);
      break;
    case TYPE_FROM_TO:
      temporalPredicate = new FromTo(QUERY_FROM, QUERY_TO);
      break;
    case TYPE_VALID_DURING:
      temporalPredicate = new ValidDuring(QUERY_FROM, QUERY_TO);
      break;
    case TYPE_ALL:
      temporalPredicate = new All();
      break;
    default:
      throw new IllegalArgumentException("The given query type '" + QUERY_TYPE + "' is not supported.");
    }

    // get the snapshot
    TemporalGraph snapshot = graph.snapshot(temporalPredicate);

    // apply optional verification
    if (VERIFICATION) {
      snapshot = snapshot.verify();
    }

    // write graph
    writeOrCountGraph(snapshot, conf);

    // execute and write job statistics
    env.execute(SnapshotBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());

    writeCSV(env);
  }

  /**
   * Checks if the necessary arguments are provided for the given query type.
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    switch (cmd.getOptionValue(OPTION_QUERY_TYPE)) {
    case TYPE_BETWEEN:
    case TYPE_CONTAINED_IN:
    case TYPE_CREATED_IN:
    case TYPE_DELETED_IN:
    case TYPE_FROM_TO:
    case TYPE_VALID_DURING:
      if (cmd.getOptionValue(OPTION_QUERY_TO) == null) {
        throw new IllegalArgumentException("The used query type needs the parameter '" +
          OPTION_QUERY_TO + "' to define the query to timestamp.");
      }
      // fall through
    case TYPE_AS_OF:
      if (cmd.getOptionValue(OPTION_QUERY_FROM) == null) {
        throw new IllegalArgumentException("The used query type needs the parameter '" +
          OPTION_QUERY_FROM + "' to define the query from timestamp.");
      }
      break;
    default:
      break;
    }
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    String queryFrom = cmd.getOptionValue(OPTION_QUERY_FROM);
    QUERY_FROM   = queryFrom == null ? null : Long.valueOf(queryFrom);

    String queryTo = cmd.getOptionValue(OPTION_QUERY_TO);
    QUERY_TO     = queryTo == null ? null : Long.valueOf(queryTo);

    QUERY_TYPE   = cmd.getOptionValue(OPTION_QUERY_TYPE);
    VERIFICATION = cmd.hasOption(OPTION_VERIFICATION);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s",
        "Parallelism",
        "dataset",
        "query-type",
        "from(ms)",
        "to(ms)",
        "verify",
        "count-only",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s",
        env.getParallelism(),
        INPUT_PATH,
        QUERY_TYPE,
        QUERY_FROM,
        QUERY_TO,
        VERIFICATION,
        COUNT_RESULT,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    writeToCSVFile(head, tail);
  }
}
