/*
 * Copyright (C) 2017-2021 Dremio Corporation
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

package com.adhoc.flight.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilitary class for helping queries out with cross-cutting concerns, such as
 * printing and saving resources to a file.
 */
public final class QueryUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(QueryUtils.class);

  private QueryUtils() {
    // Prevent instantiation.
  }

  /**
   * FIXME 
   * 
   * Writes the binary representation of the provided {@link VectorSchemaRoot} to
   * the provided {@link File}.
   *
   * @param vectorSchemaRoot the {@code VectorSchemaRoot} to be converted.
   * @param file             the file to write to.
   * @throws IOException If an error occurs when trying to write to the file.
   */
  public static void writeToBinaryFile(VectorSchemaRoot vectorSchemaRoot, FlightStream stream, File file)
      throws IOException {

    try (ArrowStreamWriter arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null,
        new FileOutputStream(file))) {

      int batchNumber = 1;
      int rowCount = vectorSchemaRoot.getRowCount();
      int totalRows = rowCount;
      arrowStreamWriter.start();
      arrowStreamWriter.writeBatch();

      boolean hasMore = true;
      while (hasMore) {
        hasMore = stream.next();
        if (hasMore) {
          arrowStreamWriter.writeBatch();
          rowCount = vectorSchemaRoot.getRowCount();
          batchNumber++;
          totalRows = totalRows + rowCount;
          LOGGER.info("Wrote batch#{} rowCount={}", batchNumber, rowCount);
        } else {
          LOGGER.info("Total write batches {} Total rows {}", batchNumber, totalRows);
        }

      }

      arrowStreamWriter.end();
    }
  }



  /*
   * This prints out the results, each value in its corresponding row and column,
   * with different columns separated by tabs and properly aligned with their
   * field names.
   *
   * For more information on this, please refer to the documentation:
   * <https://arrow.apache.org/docs/java/ipc.html>
  */
  public static void printFromBinaryFile(File file) {

    try (ArrowStreamReader reader = new ArrowStreamReader(new FileInputStream(file),
        new RootAllocator(Long.MAX_VALUE))) {

      // Instantiating a new VectorSchemaRoot based on the binary info.
      VectorSchemaRoot batchRead = reader.getVectorSchemaRoot();

      boolean hadBatch = true;
      int batchNum = 1;
      int rowCount = 0;
      while (hadBatch) {
        // Updating the data inside the VectorSchemaRoot.
        rowCount = batchRead.getRowCount();

        LOGGER.info("Retrieved: {} rows batch #{}", rowCount, batchNum);
        // System.out.println(batchRead.contentToTSVString());
        hadBatch = reader.loadNextBatch();
        if (hadBatch)
          batchNum++;
      }
      LOGGER.info("Total Retrieved: {} rows in {} batches", rowCount, batchNum);
    } catch (Exception e) {
      LOGGER.error("Failed to read from binary file", e);
    }
  }

}
