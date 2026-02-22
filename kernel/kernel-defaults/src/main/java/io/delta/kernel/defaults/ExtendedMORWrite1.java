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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.CatalogOuterClass.FileObject;
import io.delta.kernel.defaults.CatalogOuterClass.TableObject;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class ExtendedMORWrite1 {
  // 1. Add this private constructor
  private ExtendedMORWrite1() {}

  public static class MetricsExporter {
    public static void exportMetricsToLog(String outputFile) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.registerModule(new JavaTimeModule());
      mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

      // IMPORTANT: Disable pretty printing.
      // NDJSON requires each object to be on exactly one line.
      // If you pretty print, the newlines inside the object will break the format.
      try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), UTF_8)) {
        int size = LOAD_TABLE_TIMES.size();

        for (int i = 0; i < size; i++) {
          ObjectNode row = mapper.createObjectNode();

          // 1. Build the object
          addTimeBlock(mapper, row, "load_table", LOAD_TABLE_TIMES.get(i));
          addTimeBlock(mapper, row, "insert_file", INSERT_FILE_TIMES.get(i));
          addTimeBlock(mapper, row, "commit", COMMIT_TIMES.get(i));

          if (i < COMPACT_TIMES.size()) {
            addTimeBlock(mapper, row, "compact", COMPACT_TIMES.get(i));
          }

          ArrayNode filesArray = mapper.createArrayNode();
          List<File2> fileList = ADDED_FILES.get(i);
          if (fileList != null) {
            for (File2 file : fileList) {
              ObjectNode fileObj = mapper.createObjectNode();
              fileObj.put("type", file.fileType().name().toLowerCase(Locale.getDefault()));
              fileObj.put("path", file.path());
              fileObj.put("tag", file.tag());
              fileObj.put("size", getFileSize(file.path()));
              filesArray.add(fileObj);
            }
          }
          row.set("files", filesArray);

          // 2. Write as single-line JSON string
          String jsonLine = mapper.writeValueAsString(row);

          // 3. Write to file + Newline
          writer.write(jsonLine);
          writer.newLine(); // This makes it valid NDJSON
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    public static void appendSummaryToJson(String outputFile) {
      ObjectMapper mapper = new ObjectMapper();

      // 1. Calculate num_txn
      int numTxn = LOAD_TABLE_TIMES.size();

      // 2. Calculate num_files (Aggregating all files of type ADD)
      long numFiles = 0;
      for (List<File2> fileList : ADDED_FILES) {
        if (fileList != null) {
          numFiles += fileList.stream()
                  .filter(f -> f.fileType() == File2.File2Type.ADD)
                  .count();
        }
      }

      // 3. Calculate avg_latency
      double totalLatencyMillis = 0;

      for (int i = 0; i < numTxn; i++) {
        // Start: Always LOAD_TABLE start
        Instant start = LOAD_TABLE_TIMES.get(i).start;

        // End: COMPACT end if available, otherwise COMMIT end
        Instant end;
        if (i < COMPACT_TIMES.size()) {
          end = COMPACT_TIMES.get(i).end;
        } else {
          // Fallback if compaction data is missing for this index
          end = COMMIT_TIMES.get(i).end;
        }

        // Calculate duration in milliseconds
        long duration = Duration.between(start, end).toMillis();
        totalLatencyMillis += duration;
      }

      double avgLatency = (numTxn == 0) ? 0 : (totalLatencyMillis / numTxn);

      // 4. Construct the JSON Object
      ObjectNode summaryNode = mapper.createObjectNode();
      summaryNode.put("exp_name", "ExtendedMORWrite1");
      summaryNode.put("exp_type", expType);
      summaryNode.put("txn_per_compaction", txnPerCompaction);
      summaryNode.put("duration", durationStr);
      summaryNode.put("num_txn", numTxn);
      summaryNode.put("num_files", numFiles);
      summaryNode.put("avg_latency", avgLatency);

      // 5. Append to file
      // 'true' in FileWriter constructor enables append mode
      try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), UTF_8, CREATE, APPEND)) {
        String jsonLine = mapper.writeValueAsString(summaryNode);
        writer.write(jsonLine);
        writer.newLine(); // Add newline so it remains valid NDJSON
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private static void addTimeBlock(ObjectMapper mapper, ObjectNode parent, String fieldName, TimePair timePair) {
      if (timePair == null) return;
      ObjectNode timeNode = mapper.createObjectNode();
      timeNode.put("start", timePair.start.toString());
      timeNode.put("end", timePair.end.toString());
      parent.set(fieldName, timeNode);
    }

  }

  public static class TimePair {
    public final Instant start;
    public final Instant end;

    public TimePair(Instant start, Instant end) {
      this.start = start;
      this.end = end;
    }

    public long getDurationMillis() {
      return end.toEpochMilli() - start.toEpochMilli();
    }

    // Getters, equals(), hashCode(), and toString() would go here
  }

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static String expType;
  private static String durationStr;
  private static String workspaceName;
  private static String dbName;
  private static String tableName;
  private static int numRowsPerFile;
  private static int txnPerCompaction;
  private static String warehouseLocation;
  private static String s3Secret;
  private static String s3KeyId;
  private static String s3Region;
  private static String ravenAddress;
  private static final StructType SCHEMA = TPCDSSchema.STORE_SALES;
  private static Configuration hadoopConf;

  private static RavenCatalog ravenCatalog;
  private static Engine engine;
  private static Table table;
  private static FileIO s3;

  // data structures for measurements
  private static final List<TimePair> LOAD_TABLE_TIMES = Lists.newArrayList();
  private static final List<TimePair> INSERT_FILE_TIMES = Lists.newArrayList();
  private static final List<TimePair> COMMIT_TIMES = Lists.newArrayList();
  private static final List<TimePair> COMPACT_TIMES = Lists.newArrayList();
  private static final List<List<File2>> ADDED_FILES = Lists.newArrayList();

  // thread pool for s3 operations
  private static ExecutorService s3Executors;

  public static Map<String, String> parseJsonToMap(String jsonFilePath) throws IOException {
    File file = new File(jsonFilePath);

    // TypeReference is essential to tell Jackson the specific
    // Map implementation and generic types to use.
    return JSON_MAPPER.readValue(file, new TypeReference<Map<String, String>>() {});
  }

  public static void initS3() throws Exception {
    hadoopConf = new Configuration();
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // (Optional but recommended): Alias the abstract filesystem as well for newer APIs
    hadoopConf.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");

    // 2. SET CREDENTIALS: You still use the "s3a" prefix for the credential properties
    hadoopConf.set("fs.s3a.access.key", s3KeyId);
    hadoopConf.set("fs.s3a.secret.key", s3Secret);
    hadoopConf.set("fs.s3a.endpoint", "s3." + s3Region + ".amazonaws.com");
    s3 = new HadoopFileIO(hadoopConf);
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> expConfigs = parseJsonToMap(args[0]);
    expType = expConfigs.get("exp_type");
    durationStr = expConfigs.get("duration");
    workspaceName = expConfigs.get("workspace_name");
    dbName = expConfigs.get("db_name");
    tableName = expConfigs.get("table_name");
    numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
    warehouseLocation = expConfigs.get("warehouse_location");
    txnPerCompaction = Integer.parseInt(expConfigs.get("txn_per_compaction"));
    s3Secret = expConfigs.get("s3_secret");
    s3KeyId = expConfigs.get("s3_key_id");
    s3Region = expConfigs.get("s3_region");
    ravenAddress = expConfigs.get("raven_address");

    initS3();
    engine = DefaultEngine.create(hadoopConf);
    s3Executors = Executors.newFixedThreadPool(txnPerCompaction + 1);
    if (expType.equals("raven")) {
      runRavenExp(expConfigs);
    } else {
      runVanillaExp(expConfigs);
    }

    s3Executors.shutdown();

  }

  private static void runRavenExp(Map<String, String> expConfigs) {
    String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    table = Table.forPath(engine, location);
    TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, SCHEMA);
    Transaction txn = txnBuilder.build(engine);
    TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());

    ravenCatalog = new RavenCatalog(ravenAddress);
    LocalTime duration = LocalTime.parse(durationStr);
    runRavenExpImpl(duration);

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/extendedmorwrite1-delta-raven-%d-%d-log.json",
            expResultDir, txnPerCompaction, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runVanillaExp(Map<String, String> expConfigs) {
    String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    System.out.println("REACHED 1");
    table = Table.forPath(engine, location);
    System.out.println("REACHED 2");
    TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, SCHEMA);
    Transaction txn = txnBuilder.build(engine);
    System.out.println("REACHED 3");
    TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());

    System.out.println("REACHED 4");
    LocalTime duration = LocalTime.parse(durationStr);
    runVanillaExpImpl(duration);

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/extendedmorwrite1-delta-vanilla-%d-%d-log.json",
            expResultDir, txnPerCompaction, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runRavenExpImpl(LocalTime duration) {
    runTaskForDuration(running -> {
      // Keep running until flag change
      int i = 0;
      while (running.get()) {
        List<File2> fileLogs = Lists.newArrayList();
        Instant beforeLoadTable = Instant.now();

        // Load table. Load from both delta and raven
        TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.WRITE);
//        txnBuilder = txnBuilder.withSchema(engine, SCHEMA);
        Transaction txn = txnBuilder.build(engine);
        Row txnState = txn.getTransactionState(engine);

        TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, tableName);

        Instant afterLoadTable = Instant.now();

        CloseableIterator<FilteredColumnarBatch> data =
                generateLogicalData(i * numRowsPerFile + 1, (i + 1) * numRowsPerFile);
        i += 1;

        // turn logical data to physical data
        CloseableIterator<FilteredColumnarBatch> physicalData = Transaction.transformLogicalData(engine, txnState,
                data, Collections.emptyMap());

        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, Collections.emptyMap());

        // Now write the physical data to Parquet files
        CloseableIterator<DataFileStatus> dataFiles = null;
        try {
          dataFiles = engine.getParquetHandler().writeParquetFiles(
                  writeContext.getTargetDirectory(), physicalData, writeContext.getStatisticsColumns());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        List<FileObject> newFilesList = Lists.newArrayList();
        while (dataFiles.hasNext()) {
          DataFileStatus dataFile = dataFiles.next();
          FileObject newFileObject = FileObject.newBuilder().setPath(dataFile.getPath())
                  .setSize((int) dataFile.getSize())
                  .setFormat("parquet").setTag("newdata").build();
          newFilesList.add(newFileObject);
        }

        Instant afterInsertFile = Instant.now();

        ravenCatalog.finalAppendFiles(tableObject, newFilesList);

        Instant afterCommit = Instant.now();
        Instant afterCompact = afterCommit;

        // perform compaction
        // 1. Get the newdata files, using ExecQuery
        // 2. Read the newdata files, collecting statistics information, using DuckDB.
        // 3. Commit the newdata files to Iceberg and get the metadata file, manifestlist file, and manifest file.
        // 4. Replace the newdata files with different tags, Replace the metadata file, manifestlist file, and manifest file.
        if (tableObject.getSnapshotVid() % txnPerCompaction == 0) {
          tableObject = ravenCatalog.loadTable(workspaceName, dbName, tableName);
          String query = String.format(Locale.getDefault(),
                  "SELECT file_path, file_size, format FROM FILELIST SNAPSHOT TableSnapshot(%d, %d) WHERE tag = 'newdata'",
                  tableObject.getSnapshotObjId(), tableObject.getSnapshotVid());

          byte[] resultSet = ravenCatalog.execQuery(query);
          // extract the newdata files from the result set buffer
          List<FileObject> newDataFiles = Lists.newArrayList();
          RavenCatalog.BufIterator bufIter = new RavenCatalog.BufIterator(resultSet);
          while (bufIter.valid()) {
            String path = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
            bufIter.next();
            String size = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
            bufIter.next();
            String format = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
            bufIter.next();
            // change tag to data
            FileObject file = FileObject.newBuilder().setPath(path).setSize(Integer.parseInt(size))
                    .setFormat(format).setTag("data").build();
            newDataFiles.add(file);
          }

          List<Callable<DataFileStatus>> s3Tasks = Lists.newArrayList();
          for (FileObject file : newDataFiles) {
            s3Tasks.add(() -> S3ParquetStats.extractStats(s3, file.getPath(), SCHEMA, numRowsPerFile,
                    writeContext.getStatisticsColumns()));
          }

          List<DataFileStatus> newDataFilesList = Lists.newArrayList();
          // stats extraction is performed in parallel for performance
          try {
            List<Future<DataFileStatus>> stats = s3Executors.invokeAll(s3Tasks);
            for (Future<DataFileStatus> future : stats) {
              newDataFilesList.add(future.get());
            }
          }
          catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
          }

          CloseableIterator<Row> dataActions = Transaction.generateAppendActions(engine, txnState,
                  toCloseableIterator(newDataFilesList.iterator()), writeContext);

          CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);

          TransactionCommitResult commitResult = txn.commit2(engine, dataActionsIterable, fileLogs);

          // 4. Replace the newdata files with different tags, add the delta log file (and checkpoint file).
          List<String> filesToReplace = Lists.newArrayList();

          // newdata files to replace (changing the tags to 'data')
          for (FileObject file : newDataFiles) {
            filesToReplace.add(file.getPath());
          }

          // metadata files (log file and checkpoint file) to add
          for (File2 file : fileLogs) {
            switch (file.fileType()) {
              case ADD:
                FileObject newMetadataFileObject = FileObject.newBuilder()
                        .setTag(file.tag()).setPath(file.path()).build();
                // reusing newDataFiles list to hold all new files to add
                newDataFiles.add(newMetadataFileObject);
                break;
              case DELETE:
                filesToReplace.add(file.path());
                break;
              default:
                break;
            }
          }

          ravenCatalog.finalRewriteFiles(tableObject, filesToReplace, newDataFiles);

          afterCompact = Instant.now();
        }

        LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTable, afterLoadTable));
        INSERT_FILE_TIMES.add(new TimePair(afterLoadTable, afterInsertFile));
        COMMIT_TIMES.add(new TimePair(afterInsertFile, afterCommit));
        COMPACT_TIMES.add(new TimePair(afterCommit, afterCompact));
        for (FileObject fileObject : newFilesList) {
          fileLogs.add(new File2(fileObject.getPath(), File2.File2Type.ADD, "data"));
        }
        ADDED_FILES.add(fileLogs);

      }
    }, duration.toSecondOfDay(), TimeUnit.SECONDS);

  }

  private static void runVanillaExpImpl(LocalTime duration) {
    runTaskForDuration(running -> {
      // Keep running until flag change
      int i = 0;
      while (running.get()) {
        List<File2> fileLogs = Lists.newArrayList();

        Instant beforeLoadTable = Instant.now();

        // Load table. Load from both delta and raven
        TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.WRITE);
        Transaction txn = txnBuilder.build(engine);

        Row txnState = txn.getTransactionState(engine);

        Instant afterLoadTable = Instant.now();

        CloseableIterator<FilteredColumnarBatch> data =
                generateLogicalData(i * numRowsPerFile + 1, (i + 1) * numRowsPerFile);
        i += 1;

        // turn logical data to physical data
        CloseableIterator<FilteredColumnarBatch> physicalData = Transaction.transformLogicalData(engine, txnState,
                data, Collections.emptyMap());

        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, Collections.emptyMap());

        // Now write the physical data to Parquet files
        CloseableIterator<DataFileStatus> dataFiles = null;
        try {
          dataFiles = engine.getParquetHandler().writeParquetFiles(
                  writeContext.getTargetDirectory(), physicalData, writeContext.getStatisticsColumns());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        List<DataFileStatus> dataFilesList = dataFiles.toInMemoryList();

        Instant afterInsertFile = Instant.now();

        CloseableIterator<Row> dataActions = Transaction.generateAppendActions(engine, txnState,
                toCloseableIterator(dataFilesList.iterator()), writeContext);

        CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);

        TransactionCommitResult commitResult = txn.commit2(engine, dataActionsIterable, fileLogs);

        Instant afterCommit = Instant.now();

        LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTable, afterLoadTable));
        INSERT_FILE_TIMES.add(new TimePair(afterLoadTable, afterInsertFile));
        COMMIT_TIMES.add(new TimePair(afterInsertFile, afterCommit));
        for (DataFileStatus dataFile : dataFilesList) {
          fileLogs.add(new File2(dataFile.getPath(), File2.File2Type.ADD, "data"));
        }
        ADDED_FILES.add(fileLogs);

      }
    }, duration.toSecondOfDay(), TimeUnit.SECONDS);

  }

  public static void runTaskForDuration(Consumer<AtomicBoolean> task, long duration, TimeUnit unit) {
    AtomicBoolean running = new AtomicBoolean(true); // The "Green Light"
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // Submit the task, passing the control flag 'running' to it
    @SuppressWarnings("unused")
    Future<?> future = executor.submit(() -> task.accept(running));

    // Schedule the "Stop Signal"
    @SuppressWarnings("unused")
    Future<?> future2 = scheduler.schedule(() -> {
      running.set(false); // Flip the switch to Red
      scheduler.shutdown();
    }, duration, unit);

    // Shutdown executor safely
    executor.shutdown();
    try {
      // Wait for the task to finish its LAST iteration naturally.
      // We add a buffer (e.g., duration * 2) to ensure we don't kill it mid-process.
      // Since you prefer accuracy trade-offs over exceptions, we wait longer.
      if (!executor.awaitTermination(duration + 10, unit)) {
        executor.shutdownNow(); // Force kill only if it's genuinely stuck forever
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static CloseableIterator<FilteredColumnarBatch> generateLogicalData(int start, int end) {
    ColumnVector[] vectors = new ColumnVector[SCHEMA.length()];
    List<StructField> fields = SCHEMA.fields();

    // generate int data
    List<Integer> intData = Lists.newArrayList();
    for (int i = start; i <= end; i++) {
      intData.add(i);
    }

    List<String> stringData = Lists.newArrayList();
    for (Integer i : intData) {
      stringData.add(i.toString());
    }

    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).getDataType() == IntegerType.INTEGER) {
        vectors[i] = ExpColumnVector.intVector(intData);
      }
      else if (fields.get(i).getDataType() == StringType.STRING) {
        vectors[i] = ExpColumnVector.stringVector(stringData);
      }
    }

    ColumnarBatch batch = new DefaultColumnarBatch(end - start + 1, SCHEMA, vectors);
    return toCloseableIterator(Arrays.asList(new FilteredColumnarBatch(batch, Optional.empty())).iterator());
  }

  private static long getFileSize(String path) {
    long fileSize = 0;
    try {
      fileSize = s3.getFileStatus(path).getSize();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return fileSize;
  }


}
