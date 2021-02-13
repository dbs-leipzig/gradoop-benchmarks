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
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated program to benchmark a complex dataflow that contains numerous TPGM related transformations.
 * The benchmark is expected to be executed on the Citibike data set.
 */
public class CitibikeBenchmark extends BaseTpgmBenchmark {


  /**
   * Main program to run the benchmark.
   * <p>
   * Example: {@code $ /path/to/flink run -c org.gradoop.benchmarks.tpgm.CitibikeBenchmark
   * /path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, DiffBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    readBaseCMDArguments(cmd);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    TemporalGradoopConfig cfg = TemporalGradoopConfig.createConfig(env);

    TemporalCSVDataSource source = new TemporalCSVDataSource(INPUT_PATH, cfg);

   /* final GraphStatistics statistics =
      GraphStatisticsHDFSReader.read(INPUT_PATH + "_statistics", new Configuration());
*/
    TemporalGraph temporalGraph = source.getTemporalGraph();

    TemporalGraphCollection collection =
      temporalGraph.query("MATCH (s:Station)-[t1:Trip]->(m:Station)-[t2:Trip]->(e:Station)" +
      " WHERE s <> m AND m <> e AND t1.bike_id = t2.bike_id ");

    long matchCount = collection.getGraphHeads().count();

// only count the results and write it to a csv file
    DataSet<Tuple2<String, Long>> sum = collection.getGraphHeads()
      .map(v -> new Tuple2<>("G", 1L)).returns(new TypeHint<Tuple2<String, Long>>() {})
      // group by the element type (V or E)
      .groupBy(0)
      // sum the values
      .sum(1);

    sum.writeAsCsv(appendSeparator(OUTPUT_PATH) + "count.csv", FileSystem.WriteMode.OVERWRITE);

      // Reduce collection to graph
      /*.reduce(new ReduceCombination<>())
      // Grouping
      .callForGraph(
        new KeyedGrouping<>(
          // Vertex grouping key functions
          Arrays.asList(
            GroupingKeys.label(),
            GroupingKeys.property("name"),
            GroupingKeys.property("cellId")),
          // Vertex aggregates
          null,
          // Edge grouping key functions
          Arrays.asList(
            GroupingKeys.label(),
            TemporalGroupingKeys.timeStamp(VALID_TIME, TimeDimension.Field.FROM, ChronoField.ALIGNED_WEEK_OF_YEAR)),
          // Edge aggregates
          Arrays.asList(
            new Count("countTripsOfWeek"),
            new AverageDuration("avgTripDurationOfWeek", VALID_TIME))))
      // Subgraph
      .subgraph(
        v -> true,
        e -> e.getPropertyValue("countTripsOfWeek").getLong() > 0);*/

    //writeOrCountGraph(citibikeGraph, cfg);

    env.execute(CitibikeBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String.format("%s|%s|%s", "Parallelism", "dataset", "Runtime(s)");

    String tail = String.format("%s|%s|%s", env.getParallelism(), INPUT_PATH,
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    writeToCSVFile(head, tail);
  }

}
