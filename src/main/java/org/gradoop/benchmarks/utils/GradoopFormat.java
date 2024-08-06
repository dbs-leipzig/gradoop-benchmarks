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
package org.gradoop.benchmarks.utils;

public enum GradoopFormat {
  CSV("csv"),
  INDEXED_CSV("indexed"),
  PARQUET("parquet"),
  PARQUET_PROTOBUF("protobuf");

  public final String name;

  private GradoopFormat(String name) {
    this.name = name;
  }

  public static GradoopFormat getByName(String name) {
    for (GradoopFormat format : values()) {
      if (format.name.equals(name)) {
        return format;
      }
    }
    return null;
  }
}
