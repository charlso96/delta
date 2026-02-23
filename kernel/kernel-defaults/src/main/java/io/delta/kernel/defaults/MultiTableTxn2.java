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
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class MultiTableTxn2 {
  // 1. Add this private constructor
  private MultiTableTxn2() {}

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
        Instant end = COMMIT_TIMES.get(i).end;

        // Calculate duration in milliseconds
        long duration = Duration.between(start, end).toMillis();
        totalLatencyMillis += duration;
      }

      double avgLatency = (numTxn == 0) ? 0 : (totalLatencyMillis / numTxn);

      // 4. Construct the JSON Object
      ObjectNode summaryNode = mapper.createObjectNode();
      summaryNode.put("exp_name", "MultiTableTxn2");
      summaryNode.put("exp_type", expType);
      summaryNode.put("num_tables", numTables);
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
  private static String workspaceName;
  private static String dbName;
  private static int numTxn;
  private static int numTables;
  private static int numRowsPerFile;
  private static String warehouseLocation;
  private static String s3Secret;
  private static String s3KeyId;
  private static String s3Region;
  private static String ravenAddress;
  private static Configuration hadoopConf;

  private static RavenCatalog ravenCatalog;
  private static Engine engine;
  private static FileIO s3;

  // data structures for measurements
  private static final List<TimePair> LOAD_TABLE_TIMES = Lists.newArrayList();
  private static final List<TimePair> INSERT_FILE_TIMES = Lists.newArrayList();
  private static final List<TimePair> COMMIT_TIMES = Lists.newArrayList();
  private static final List<List<File2>> ADDED_FILES = Lists.newArrayList();

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
    workspaceName = expConfigs.get("workspace_name");
    dbName = expConfigs.get("db_name");
    numTxn = Integer.parseInt(expConfigs.get("num_txn"));
    numTables = Integer.parseInt(expConfigs.get("num_tables"));
    numRowsPerFile = Integer.parseInt(expConfigs.get("num_rows_per_file"));
    warehouseLocation = expConfigs.get("warehouse_location");
    s3Secret = expConfigs.get("s3_secret");
    s3KeyId = expConfigs.get("s3_key_id");
    s3Region = expConfigs.get("s3_region");
    ravenAddress = expConfigs.get("raven_address");

    initS3();
    engine = DefaultEngine.create(hadoopConf);
    if (expType.equals("raven")) {
      runRavenExp(expConfigs);
    } else {
      runVanillaExp(expConfigs);
    }

  }

  private static void runRavenExp(Map<String, String> expConfigs) {
    for (int i = 0; i < TPCDSSchema.SCHEMA_LIST.size(); i++) {
      StructType schema = TPCDSSchema.SCHEMA_LIST.get(i);
      String tableName = TPCDSSchema.SCHEMA_NAMES.get(i);
      String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
      Table table = Table.forPath(engine, location);
      TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.CREATE_TABLE);
      txnBuilder = txnBuilder.withSchema(engine, schema);
      Transaction txn = txnBuilder.build(engine);
      TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());
    }

    ravenCatalog = new RavenCatalog(ravenAddress);
    runRavenExpImpl();

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/multitabletxn2-delta-raven-%d-%d-log.json",
            expResultDir, numTables, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runVanillaExp(Map<String, String> expConfigs) {
    for (int i = 0; i < TPCDSSchema.SCHEMA_LIST.size(); i++) {
      StructType schema = TPCDSSchema.SCHEMA_LIST.get(i);
      String tableName = TPCDSSchema.SCHEMA_NAMES.get(i);
      String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
      Table table = Table.forPath(engine, location);
      TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.CREATE_TABLE);
      txnBuilder = txnBuilder.withSchema(engine, schema);
      Transaction txn = txnBuilder.build(engine);
      TransactionCommitResult commitResult = txn.commit(engine, CloseableIterable.emptyIterable());
    }

    runVanillaExpImpl();

    String expResultDir = expConfigs.get("exp_result_dir");
    String logFileName = String.format(Locale.getDefault(),"%s/multitabletxn2-delta-vanilla-%d-%d-log.json",
            expResultDir, numTables, numRowsPerFile);
    String summaryFileName = String.format("%s/summary.json", expResultDir);
    MetricsExporter.exportMetricsToLog(logFileName);
    MetricsExporter.appendSummaryToJson(summaryFileName);
  }

  private static void runRavenExpImpl() {
    // Keep running until flag change
    for (int i = 0; i < numTxn; i++) {
      List<Integer> tableIdxList = ThreadLocalRandom.current()
              .ints(0, TPCDSSchema.SCHEMA_LIST.size())
              .distinct()      // Filters out duplicates
              .limit(numTables)        // Stops once we have exactly 'n' distinct numbers
              .boxed()         // Converts primitive int to Integer
              .collect(Collectors.toList());

      List<List<File2>> fileLogs = Lists.newArrayList();

      Instant beforeLoadTables = Instant.now();

      List<String> tableNames = Lists.newArrayList();
      for (Integer tableIdx : tableIdxList) {
        tableNames.add(TPCDSSchema.SCHEMA_NAMES.get(tableIdx));
      }

      CatalogOuterClass.StartTransactionResponse ravenTxn = ravenCatalog.startTransaction(
              workspaceName, dbName, tableNames);

      long txnId = ravenTxn.getTxnId();
      List<TableObject> unsortedTableObjects = ravenTxn.getTablesList();

      List<TableObject> tableObjects = Lists.newArrayList();
      List<Table> tables = Lists.newArrayList();
      List<Transaction> txns = Lists.newArrayList();
      List<Row> txnStates = Lists.newArrayList();
      for (Integer tableIdx : tableIdxList) {
        String tableName = TPCDSSchema.SCHEMA_NAMES.get(tableIdx);
        String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
        Table table = Table.forPath(engine, location);
        TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.WRITE);
        Transaction txn = txnBuilder.build(engine);
        Row txnState = txn.getTransactionState(engine);
        // brute force way to sort table objects. Should not take too long
        Optional<TableObject> tableObject = unsortedTableObjects.stream()
                .filter(t -> t.getTableName().equals(tableName)) // The Predicate
                .findFirst();
        tableObjects.add(tableObject.get());
        tables.add(table);
        txns.add(txn);
        txnStates.add(txnState);
      }

      Instant afterLoadTables = Instant.now();

      List<CloseableIterable<Row>> dataActionsIterables = Lists.newArrayList();
      for (int j = 0; j < tableIdxList.size(); j++) {
        StructType schema = TPCDSSchema.SCHEMA_LIST.get(tableIdxList.get(j));
        CloseableIterator<FilteredColumnarBatch> data = generateLogicalData(schema, 1, numRowsPerFile);
        // turn logical data to physical data
        CloseableIterator<FilteredColumnarBatch> physicalData = Transaction.transformLogicalData(engine,
                txnStates.get(j), data, Collections.emptyMap());
        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(engine, txnStates.get(j), Collections.emptyMap());

        try {
          // Now write the physical data to Parquet files
          List <DataFileStatus> dataFiles = engine.getParquetHandler().writeParquetFiles(
                  writeContext.getTargetDirectory(), physicalData, writeContext.getStatisticsColumns()).toInMemoryList();
          // add the filelogs
          List<File2> newFileLogs = Lists.newArrayList();
          fileLogs.add(newFileLogs);
          for (DataFileStatus dataFileStatus : dataFiles) {
            fileLogs.get(j).add(new File2(dataFileStatus.getPath(), File2.File2Type.ADD, "data"));
          }

          CloseableIterator<Row> dataActions = Transaction.generateAppendActions(engine, txnStates.get(j),
                  toCloseableIterator(dataFiles.iterator()), writeContext);

          CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);
          dataActionsIterables.add(dataActionsIterable);

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      Instant afterInsertFiles = Instant.now();

      for (int j = 0 ; j < tableIdxList.size(); j++) {
        List<File2> curFileLogs = fileLogs.get(j);

        TransactionCommitResult commitResult = txns.get(j).commit2(engine, dataActionsIterables.get(j),
                curFileLogs);

        List<FileObject> newDataFiles = Lists.newArrayList();
        List<String> filesToReplace = Lists.newArrayList();

        for (File2 file : curFileLogs) {
          switch (file.fileType()) {
            case ADD:
              FileObject newFileObject = FileObject.newBuilder()
                      .setTag(file.tag()).setPath(file.path()).build();
              newDataFiles.add(newFileObject);
              break;
            case DELETE:
              filesToReplace.add(file.path());
              break;
            default:
              break;
          }
        }

        ravenCatalog.finalRewriteFiles(tableObjects.get(j), filesToReplace, newDataFiles, txnId);

      }

      ravenCatalog.commit(txnId);

      Instant afterCommit = Instant.now();

      LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTables, afterLoadTables));
      INSERT_FILE_TIMES.add(new TimePair(afterLoadTables, afterInsertFiles));
      COMMIT_TIMES.add(new TimePair(afterInsertFiles, afterCommit));
      // flatten the fileLogs
      ADDED_FILES.add(fileLogs.stream()
              .flatMap(List::stream) // or simply Collection::stream
              .collect(Collectors.toList()));

    }

  }

  private static void runVanillaExpImpl() {
    for (int i = 0; i < numTxn; i++) {
      List<Integer> tableIdxList = ThreadLocalRandom.current()
              .ints(0, TPCDSSchema.SCHEMA_LIST.size())
              .distinct()      // Filters out duplicates
              .limit(numTables)        // Stops once we have exactly 'n' distinct numbers
              .boxed()         // Converts primitive int to Integer
              .collect(Collectors.toList());

      List<File2> fileLogs = Lists.newArrayList();

      Instant beforeLoadTables = Instant.now();

      List<Table> tables = Lists.newArrayList();
      List<Transaction> txns = Lists.newArrayList();
      List<Row> txnStates = Lists.newArrayList();
      for (Integer tableIdx : tableIdxList) {
        String tableName = TPCDSSchema.SCHEMA_NAMES.get(tableIdx);
        String location = String.format("%s/%s.db/%s", warehouseLocation, dbName, tableName);
        Table table = Table.forPath(engine, location);
        TransactionBuilder txnBuilder = table.createTransactionBuilder(engine, "Examples", Operation.WRITE);
        Transaction txn = txnBuilder.build(engine);
        Row txnState = txn.getTransactionState(engine);

        tables.add(table);
        txns.add(txn);
        txnStates.add(txnState);
      }

      Instant afterLoadTables = Instant.now();

      List<CloseableIterable<Row>> dataActionsIterables = Lists.newArrayList();
      for (int j = 0; j < tableIdxList.size(); j++) {
        StructType schema = TPCDSSchema.SCHEMA_LIST.get(tableIdxList.get(j));
        CloseableIterator<FilteredColumnarBatch> data = generateLogicalData(schema, 1, numRowsPerFile);
        // turn logical data to physical data
        CloseableIterator<FilteredColumnarBatch> physicalData = Transaction.transformLogicalData(engine,
                txnStates.get(j), data, Collections.emptyMap());
        // Get the write context
        DataWriteContext writeContext = Transaction.getWriteContext(engine, txnStates.get(j), Collections.emptyMap());

        try {
          // Now write the physical data to Parquet files
          List <DataFileStatus> dataFiles = engine.getParquetHandler().writeParquetFiles(
                  writeContext.getTargetDirectory(), physicalData, writeContext.getStatisticsColumns()).toInMemoryList();
          // add to filelogs
          for (DataFileStatus dataFileStatus : dataFiles) {
            fileLogs.add(new File2(dataFileStatus.getPath(), File2.File2Type.ADD, "data"));
          }

          CloseableIterator<Row> dataActions = Transaction.generateAppendActions(engine, txnStates.get(j),
                  toCloseableIterator(dataFiles.iterator()), writeContext);

          CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(dataActions);

          dataActionsIterables.add(dataActionsIterable);

        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      Instant afterInsertFiles = Instant.now();

      for (int j = 0 ; j < tableIdxList.size(); j++) {
        TransactionCommitResult commitResult = txns.get(j).commit2(engine, dataActionsIterables.get(j),
                fileLogs);
      }

      Instant afterCommit = Instant.now();

      LOAD_TABLE_TIMES.add(new TimePair(beforeLoadTables, afterLoadTables));
      INSERT_FILE_TIMES.add(new TimePair(afterLoadTables, afterInsertFiles));
      COMMIT_TIMES.add(new TimePair(afterInsertFiles, afterCommit));
      ADDED_FILES.add(fileLogs);

    }

  }

  private static CloseableIterator<FilteredColumnarBatch> generateLogicalData(StructType schema, int start, int end) {
    ColumnVector[] vectors = new ColumnVector[schema.length()];
    List<StructField> fields = schema.fields();

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

    ColumnarBatch batch = new DefaultColumnarBatch(end - start + 1, schema, vectors);
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
