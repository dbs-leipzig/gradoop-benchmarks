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
package org.gradoop.benchmarks.dimspan.data_source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString;
import org.gradoop.flink.io.impl.tlf.inputformats.TLFInputFormat;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Objects;

/**
 * Lightweight data source for TLF formatted string-labeled graphs.
 * NOTE, no consistency check, inconsistent data will cause errors!
 */
public class DIMSpanTLFSource {
  /**
   * Gradoop configuration
   */
  private final GradoopFlinkConfig config;
  /**
   * input file path
   */
  private final String filePath;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param filePath input file path
   * @param config Gradoop configuration
   */
  public DIMSpanTLFSource(String filePath, GradoopFlinkConfig config) {
    this.filePath = Objects.requireNonNull(filePath);
    this.config = Objects.requireNonNull(config);
  }

  /**
   * Reads the input as dataset of TLFGraphs.
   *
   * @return io graphs
   * @throws IOException on failure
   */
  public DataSet<LabeledGraphStringString> getGraphs() throws IOException {
    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    return env.createInput(HadoopInputs
      .readHadoopFile(new TLFInputFormat(), LongWritable.class, Text.class, getFilePath()))
      .map(new DIMSpanGraphFromText());
  }

  /**
   * Get the gradoop config.
   *
   * @return the gradoop config
   */
  private GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * Get the file path
   *
   * @return the path as string
   */
  private String getFilePath() {
    return filePath;
  }
}
