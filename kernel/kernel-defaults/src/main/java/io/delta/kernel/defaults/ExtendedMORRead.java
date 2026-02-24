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
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;


import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class ExtendedMORRead {
  // 1. Add this private constructor
  private ExtendedMORRead() {}

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
          addTimeBlock(mapper, row, "list_files", LIST_FILES_TIMES.get(i));
          addTimeBlock(mapper, row, "select", SELECT_TIMES.get(i));

          ArrayNode filesArray = mapper.createArrayNode();
          List<String> fileList = SELECTED_FILES.get(i);
          List<Long> fileSizes = SELECTED_FILES_SIZE.get(i);
          if (fileList != null && fileSizes != null) {
            for (int j = 0; j < fileList.size(); j++) {
              ObjectNode fileObj = mapper.createObjectNode();
              fileObj.put("path", fileList.get(j));
              fileObj.put("size", fileSizes.get(j));
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
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }

    public static void appendSummaryToJson(String outputFile) {
      ObjectMapper mapper = new ObjectMapper();

      // 3. Calculate avg_latency
      double totalLatencyMillis = 0;

      for (int i = 0; i < LOAD_TABLE_TIMES.size(); i++) {
        Instant start = LOAD_TABLE_TIMES.get(i).start;

        Instant end = SELECT_TIMES.get(i).end;

        // Calculate duration in milliseconds
        long duration = Duration.between(start, end).toMillis();
        totalLatencyMillis += duration;
      }

      double avgLatency = LOAD_TABLE_TIMES.isEmpty() ? 0 : (totalLatencyMillis / LOAD_TABLE_TIMES.size());

      // 4. Construct the JSON Object
      ObjectNode summaryNode = mapper.createObjectNode();
      summaryNode.put("exp_name", "ExtendedMORRead");
      summaryNode.put("exp_type", expType);
      summaryNode.put("txn_per_compaction", txnPerCompaction);
      summaryNode.put("selectivity", selectivity);
      summaryNode.put("num_rows_per_file", numRowsPerFile);
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
  private static String workspaceName;
  private static String dbName;
  private static String tableName;
  // number of data files
  private static int numDataFiles;
  private static int numMeasures;
  // selectivity is expressed in percentage points
  private static int selectivity;
  private static int numRowsPerFile;
  // the total number of rows,
  private static int totalNumRows;
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
  private static final List<TimePair> LIST_FILES_TIMES = Lists.newArrayList();
  private static final List<TimePair> SELECT_TIMES = Lists.newArrayList();
  private static final List<List<String>> SELECTED_FILES = Lists.newArrayList();
  private static final List<List<Long>> SELECTED_FILES_SIZE = Lists.newArrayList();

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
    numDataFiles = Integer.parseInt(expConfigs.get("num_data_files"));
    numMeasures = Integer.parseInt(expConfigs.get("num_measures"));
    selectivity = Integer.parseInt(expConfigs.get("selectivity"));
    workspaceName = expConfigs.get("workspace_name");
    dbName = expConfigs.get("db_name");
    tableName = expConfigs.get("table_name");
    numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
    totalNumRows = numDataFiles * numRowsPerFile;
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
    List<PostCommitHook> postCommitHooks = commitResult.getPostCommitHooks();
    for (PostCommitHook postCommitHook : postCommitHooks) {
      try {
        postCommitHook.threadSafeInvoke(engine);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    ravenCatalog = new RavenCatalog(ravenAddress);
    populateRavenExp();
    try {
      runRavenExpImpl();
    } catch (Exception e) {
      e.printStackTrace();
    }

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/extendedmorread-delta-raven-%d-%d-%d-log.json",
            expResultDir, selectivity, txnPerCompaction, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runVanillaExp(Map<String, String> expConfigs) {
    String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
    table = Table.forPath(engine, location);
    TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, SCHEMA);
    Transaction txn = txnBuilder.build(engine);
    TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());
    List<PostCommitHook> postCommitHooks = commitResult.getPostCommitHooks();
    for (PostCommitHook postCommitHook : postCommitHooks) {
      try {
        postCommitHook.threadSafeInvoke(engine);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    populateVanillaExp();
    try {
      runVanillaExpImpl();
    } catch (Exception e) {
      e.printStackTrace();
    }

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/extendedmorread-delta-vanilla-%d-%d-%d-log.json",
            expResultDir, selectivity, txnPerCompaction, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void populateRavenExp() {
    // Keep running for given number of txns
    for (int i = 0; i < numDataFiles; i++) {
      List<File2> fileLogs = Lists.newArrayList();

      // Load table. Load from both delta and raven
      TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.WRITE);
      Transaction txn = txnBuilder.build(engine);
      Row txnState = txn.getTransactionState(engine);

      TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, tableName);

      CloseableIterator<FilteredColumnarBatch> data =
              generateLogicalData(i * numRowsPerFile + 1, (i + 1) * numRowsPerFile);

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

      ravenCatalog.finalAppendFiles(tableObject, newFilesList);

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
        List<PostCommitHook> postCommitHooks = commitResult.getPostCommitHooks();
        for (PostCommitHook postCommitHook : postCommitHooks) {
          try {
            postCommitHook.threadSafeInvoke(engine);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

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
      }

    }

  }

  private static void runRavenExpImpl() throws IOException {
    for (int i = 0; i < numMeasures; i++) {
      Instant beforeLoadTable = Instant.now();

      TableObject tableObject = ravenCatalog.loadTable(workspaceName, dbName, tableName);
      Snapshot snapshot = table.getLatestSnapshot(engine);

      Instant afterLoadTable = Instant.now();

      // randomly generate lowerBound
      int lowerBound = ThreadLocalRandom.current().nextInt(1, totalNumRows -
              (int) ((float) selectivity * totalNumRows/ 100));
      int upperBound = lowerBound + (int) ((float) selectivity * totalNumRows/ 100);

      Predicate predicate =
              new And(new Predicate(">=", new Column("ss_sold_date_sk"), Literal.ofInt(lowerBound)),
                      new Predicate("<", new Column("ss_sold_date_sk"), Literal.ofInt(upperBound)));



      // retrieve newdata files to merge from Raven
      List<FileStatus> filesToMerge = Lists.newArrayList();
      String query = String.format(Locale.getDefault(),
              "SELECT file_path, file_size FROM FILELIST SNAPSHOT TableSnapshot(%d, %d) WHERE tag = 'newdata'",
              tableObject.getSnapshotObjId(), tableObject.getSnapshotVid());
      byte[] resultSet = ravenCatalog.execQuery(query);
      RavenCatalog.BufIterator bufIter = new RavenCatalog.BufIterator(resultSet);
      while (bufIter.valid()) {
        String path = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
        bufIter.next();
        String size = new String(resultSet, bufIter.dataIdx(), bufIter.elemSize(), UTF_8);
        bufIter.next();

        filesToMerge.add(FileStatus.of(path, Long.parseLong(size), 0L));
      }

      Scan scan = snapshot.getScanBuilder().withReadSchema(SCHEMA).withFilter(predicate).build();
      Row scanState = scan.getScanState(engine);

      CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine);

      Instant afterListFiles = Instant.now();

      List<String> filesToScan = Lists.newArrayList();

      try {
        StructType physicalReadSchema =
                ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
        while (scanFileIter.hasNext()) {
          FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
          try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
            while (scanFileRows.hasNext()) {
              Row scanFileRow = scanFileRows.next();

              FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
              filesToScan.add(fileStatus.getPath());

              CloseableIterator<ColumnarBatch> physicalDataIter =
                      engine.getParquetHandler().readParquetFiles(
                              singletonCloseableIterator(fileStatus),
                              physicalReadSchema,
                              Optional.of(predicate));
              try (CloseableIterator<FilteredColumnarBatch> transformedData =
                           Scan.transformPhysicalData(
                                   engine,
                                   scanState,
                                   scanFileRow,
                                   physicalDataIter)) {
                while (transformedData.hasNext()) {
                  FilteredColumnarBatch filteredData = transformedData.next();
                }
              }
            }
          }
        }

        for (FileStatus fileStatus : filesToMerge) {
          filesToScan.add(fileStatus.getPath());

          CloseableIterator<ColumnarBatch> physicalDataIter =
                  engine.getParquetHandler().readParquetFiles(
                          singletonCloseableIterator(fileStatus),
                          physicalReadSchema,
                          Optional.of(predicate));
          // rather hacky way to make the default engine scan work... Just using a random scanFileRow. Should work
          // since we are not using any deletion vector or partitioning.
          try (CloseableIterator<FilteredColumnarBatch> transformedData =
                       Scan.transformPhysicalData2(
                               engine,
                               scanState,
                               physicalDataIter)) {
            while (transformedData.hasNext()) {
              FilteredColumnarBatch filteredData = transformedData.next();
            }
          }
        }
      } finally {
        scanFileIter.close();
      }

      Instant afterSelect = Instant.now();

      LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTable, afterLoadTable));
      LIST_FILES_TIMES.add(new TimePair(afterLoadTable, afterListFiles));
      SELECT_TIMES.add(new TimePair(afterListFiles, afterSelect));
      SELECTED_FILES.add(filesToScan);
      List<Long> fileSizes = Lists.newArrayList();
      for (String path : filesToScan) {
        fileSizes.add(getFileSize(path));
      }
      SELECTED_FILES_SIZE.add(fileSizes);

    }

  }

  private static void populateVanillaExp() {
    for (int i = 0; i < numDataFiles; i++) {
      List<File2> fileLogs = Lists.newArrayList();

      // Load table. Load from both delta and raven
      TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.WRITE);
      Transaction txn = txnBuilder.build(engine);

      Row txnState = txn.getTransactionState(engine);

      CloseableIterator<FilteredColumnarBatch> data =
              generateLogicalData(i * numRowsPerFile + 1, (i + 1) * numRowsPerFile);

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

      CloseableIterator<Row> dataActions = Transaction.generateAppendActions(engine, txnState,
              toCloseableIterator(dataFilesList.iterator()), writeContext);

      CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);

      TransactionCommitResult commitResult = txn.commit2(engine, dataActionsIterable, fileLogs);
      List<PostCommitHook> postCommitHooks = commitResult.getPostCommitHooks();
      for (PostCommitHook postCommitHook : postCommitHooks) {
        try {
          postCommitHook.threadSafeInvoke(engine);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    }
  }

  private static void runVanillaExpImpl() throws IOException {
    for (int i = 0; i < numMeasures; i++) {
      Instant beforeLoadTable = Instant.now();

      Snapshot snapshot = table.getLatestSnapshot(engine);

      Instant afterLoadTable = Instant.now();

      // randomly generate lowerBound
      int lowerBound = ThreadLocalRandom.current().nextInt(1, totalNumRows -
              (int) ((float) selectivity * totalNumRows/ 100));
      int upperBound = lowerBound + (int) ((float) selectivity * totalNumRows/ 100);

      Predicate predicate =
              new And(new Predicate(">=", new Column("ss_sold_date_sk"), Literal.ofInt(lowerBound)),
                      new Predicate("<", new Column("ss_sold_date_sk"), Literal.ofInt(upperBound)));

      Scan scan = snapshot.getScanBuilder().withReadSchema(SCHEMA).withFilter(predicate).build();
      Row scanState = scan.getScanState(engine);

      CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine);

      Instant afterListFiles = Instant.now();
      List<String> filesToScan = Lists.newArrayList();
      try {
        StructType physicalReadSchema =
                ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
        while (scanFileIter.hasNext()) {
          FilteredColumnarBatch scanFilesBatch = scanFileIter.next();
          try (CloseableIterator<Row> scanFileRows = scanFilesBatch.getRows()) {
            while (scanFileRows.hasNext()) {
              Row scanFileRow = scanFileRows.next();

              FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
              filesToScan.add(fileStatus.getPath());

              CloseableIterator<ColumnarBatch> physicalDataIter =
                      engine.getParquetHandler().readParquetFiles(
                              singletonCloseableIterator(fileStatus),
                              physicalReadSchema,
                              Optional.of(predicate));
              try (CloseableIterator<FilteredColumnarBatch> transformedData =
                              Scan.transformPhysicalData(
                                      engine,
                                      scanState,
                                      scanFileRow,
                                      physicalDataIter)) {
                while (transformedData.hasNext()) {
                  FilteredColumnarBatch filteredData = transformedData.next();
                }
              }
            }
          }
        }
      } finally {
        scanFileIter.close();
      }

      Instant afterSelect = Instant.now();

      LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTable, afterLoadTable));
      LIST_FILES_TIMES.add(new TimePair(afterLoadTable, afterListFiles));
      SELECT_TIMES.add(new TimePair(afterListFiles, afterSelect));
      SELECTED_FILES.add(filesToScan);
      List<Long> fileSizes = Lists.newArrayList();
      for (String path : filesToScan) {
        fileSizes.add(getFileSize(path));
      }
      SELECTED_FILES_SIZE.add(fileSizes);

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
