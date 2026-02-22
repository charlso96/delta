/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.defaults;

import java.io.UncheckedIOException;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.delta.kernel.defaults.internal.parquet.ParquetStatsReader.readDataFileStatistics;
import static java.util.Collections.emptyMap;


public class S3ParquetStats {
  private S3ParquetStats() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(S3ParquetStats.class);

  public static DataFileStatus extractStats(FileIO fileIO, String path, StructType dataSchema, long numRows,
                                      List<Column> statsColumns) {
    try {
      // Get the FileStatus to figure out the file size and modification time
      FileStatus fileStatus = fileIO.getFileStatus(path);
      String resolvedPath = fileIO.resolvePath(path);

      DataFileStatistics stats;
      if (statsColumns.isEmpty()) {
        stats =
                new DataFileStatistics(
                        numRows,
                        emptyMap() /* minValues */,
                        emptyMap() /* maxValues */,
                        emptyMap() /* nullCount */);
      } else {
        stats =
                readDataFileStatistics(
                        fileIO.newInputFile(resolvedPath, fileStatus.getSize()), dataSchema, statsColumns);
      }

      return new DataFileStatus(
              resolvedPath,
              fileStatus.getSize(),
              fileStatus.getModificationTime(),
              Optional.ofNullable(stats));
    } catch (IOException ioe) {
      throw new UncheckedIOException("Failed to read the stats for: " + path, ioe);
    }
  }

}
