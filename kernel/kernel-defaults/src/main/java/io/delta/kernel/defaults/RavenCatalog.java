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

import io.delta.kernel.defaults.CatalogOuterClass.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

public class RavenCatalog {
  private final String ravenAddress;
  private final ManagedChannel channel;
  private final CatalogGrpc.CatalogBlockingStub catalogStub;

  public static class BufIterator {
    private final byte[] buf;
    private int elemSize = 0;
    private int dataIdx = 0;
    private int nextOffset = 0;
    private boolean isNull = false;
    private boolean valid = false;

    public BufIterator(byte[] buf) {
      this.buf = buf;
      // Initialize first element logic
      if (buf.length >= Integer.BYTES) {
        this.elemSize = ByteBuffer.wrap(buf, 0, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        this.isNull = (elemSize == -1);
        // Account for null
        if (this.isNull) {
          elemSize = 0;
        }
        this.dataIdx = Integer.BYTES;
        this.nextOffset = Integer.BYTES + this.elemSize;

        if (this.nextOffset <= buf.length) {
          this.valid = true;
        }
      }
    }

    public boolean next() {
      if (!valid) {
        return false;
      } else if (nextOffset + Integer.BYTES >= buf.length) {
        // Logic matches Scala: if remaining bytes are <= 4, mark invalid.
        valid = false;
        return false;
      } else {
        this.elemSize = ByteBuffer.wrap(buf, nextOffset, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        this.dataIdx = nextOffset + Integer.BYTES;
        this.isNull = (elemSize == -1);
        // Account for null
        if (this.isNull) {
          elemSize = 0;
        }
        this.nextOffset = this.dataIdx + this.elemSize;

        if (this.nextOffset > buf.length) {
          valid = false;
          return false;
        } else {
          valid = true;
          return true;
        }
      }
    }

    public boolean valid() {
      return valid;
    }

    public int dataIdx() {
      return dataIdx;
    }

    public int elemSize() {
      return elemSize;
    }
  }



  public RavenCatalog(Map<String, String> conf) {
    ravenAddress = conf.get("raven.address");
    String[] addressPort = ravenAddress.split(":", -1);
    channel = ManagedChannelBuilder.forAddress(addressPort[0], Integer.parseInt(addressPort[1]))
            .usePlaintext().build();
    catalogStub = CatalogGrpc.newBlockingStub(channel);
  }

  public RavenCatalog(String address) {
    this.ravenAddress = address;
    String[] addressPort = ravenAddress.split(":", -1);
    channel = ManagedChannelBuilder.forAddress(addressPort[0], Integer.parseInt(addressPort[1]))
            .usePlaintext().build();
    catalogStub = CatalogGrpc.newBlockingStub(channel);
  }

  public TableObject loadTable(String workspaceName, String dbName, String tableName) {
    GetTableRequest getTableRequest = GetTableRequest.newBuilder()
            .setWorkspaceName(workspaceName).setDbName(dbName).setTableName(tableName).build();

    return catalogStub.getTable(getTableRequest).getTable();
  }

  public TableObject loadTable(String workspaceName, String dbName, String tableName, int vid) {
    GetTableRequest getTableRequest = GetTableRequest.newBuilder()
            .setWorkspaceName(workspaceName).setDbName(dbName).setTableName(tableName).setVid(vid).build();

    return catalogStub.getTable(getTableRequest).getTable();
  }

  public List<FileObject> listFiles(TableObject table) {
    ListFilesRequest listFilesRequest = ListFilesRequest.newBuilder()
            .setSnapshotObjId(table.getSnapshotObjId()).setSnapshotVid(table.getSnapshotVid()).build();

    return catalogStub.listFiles(listFilesRequest).getFilesList();
  }

  public boolean appendFiles(TableObject table, List<FileObject> files) {
    AppendFilesRequest appendFilesRequest = AppendFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(true).addAllFiles(files).build();

    return catalogStub.appendFiles(appendFilesRequest).getSuccess();
  }

  public boolean appendFiles(TableObject table, List<FileObject> files,
                             long txnId) {
    AppendFilesRequest appendFilesRequest = AppendFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(true).setTxnId(txnId).addAllFiles(files).build();

    return catalogStub.appendFiles(appendFilesRequest).getSuccess();
  }

  public boolean finalAppendFiles(TableObject table, List<FileObject> files) {
    AppendFilesRequest appendFilesRequest = AppendFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(false).addAllFiles(files).build();

    return catalogStub.appendFiles(appendFilesRequest).getSuccess();
  }

  public boolean finalAppendFiles(TableObject table, List<FileObject> files,
                                  long txnId) {
    AppendFilesRequest appendFilesRequest = AppendFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(false).setTxnId(txnId).addAllFiles(files).build();

    return catalogStub.appendFiles(appendFilesRequest).getSuccess();
  }

  public boolean rewriteFiles(TableObject table, List<String> filesToReplace,
                              List <FileObject> filesToAdd) {
    RewriteFilesRequest rewriteFilesRequest = RewriteFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(true).addAllFilesToReplace(filesToReplace)
            .addAllFilesToAdd(filesToAdd).build();

    return catalogStub.rewriteFiles(rewriteFilesRequest).getSuccess();
  }

  public boolean rewriteFiles(TableObject table, List<String> filesToReplace,
                              List <FileObject> filesToAdd, long txnId) {
    RewriteFilesRequest rewriteFilesRequest = RewriteFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(true).setTxnId(txnId)
            .addAllFilesToReplace(filesToReplace).addAllFilesToAdd(filesToAdd).build();

    return catalogStub.rewriteFiles(rewriteFilesRequest).getSuccess();
  }

  public boolean finalRewriteFiles(TableObject table, List<String> filesToReplace,
                                   List <FileObject> filesToAdd) {
    RewriteFilesRequest rewriteFilesRequest = RewriteFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(false).addAllFilesToReplace(filesToReplace)
            .addAllFilesToAdd(filesToAdd).build();

    return catalogStub.rewriteFiles(rewriteFilesRequest).getSuccess();
  }

  public boolean finalRewriteFiles(TableObject table, List<String> filesToReplace,
                                   List <FileObject> filesToAdd, long txnId) {
    RewriteFilesRequest rewriteFilesRequest = RewriteFilesRequest
            .newBuilder().setTableObjId(table.getTableObjId()).setSnapshotObjId(table.getSnapshotObjId())
            .setSnapshotVid(table.getSnapshotVid()).setPrepare(false).setTxnId(txnId)
            .addAllFilesToReplace(filesToReplace).addAllFilesToAdd(filesToAdd).build();

    return catalogStub.rewriteFiles(rewriteFilesRequest).getSuccess();
  }

  public byte[] execQuery(String query) {
    ExecQueryRequest execQueryRequest = ExecQueryRequest.newBuilder()
            .setQuery(query).build();

    return catalogStub.execQuery(execQueryRequest).getResultSet().toByteArray();
  }

  public StartTransactionResponse startTransaction(String workspaceName, String dbName,
                                                   List<String> tableNames) {
    StartTransactionRequest startTransactionRequest = StartTransactionRequest
            .newBuilder().setWorkspaceName(workspaceName).setDbName(dbName).addAllTableNames(tableNames).build();

    return catalogStub.startTransaction(startTransactionRequest);
  }

  public boolean commit(long txnId) {
    CommitRequest commitRequest = CommitRequest.newBuilder()
            .setTxnId(txnId).build();

    return catalogStub.commit(commitRequest).getSuccess();
  }

  public boolean expireSnapshot(String workspaceName, String dbName, String tableName, int vid) {
    ExpireSnapshotsRequest expireSnapshotsRequest = ExpireSnapshotsRequest
            .newBuilder().setWorkspaceName(workspaceName).setDbName(dbName).setTableName(tableName)
            .setVid(vid).build();

    return catalogStub.expireSnapshots(expireSnapshotsRequest).getSuccess();
  }

  public boolean expireSnapshotsOlderThan(String workspaceName, String dbName, String tableName, int vid) {
    ExpireSnapshotsRequest expireSnapshotsRequest = ExpireSnapshotsRequest
            .newBuilder().setWorkspaceName(workspaceName).setDbName(dbName).setTableName(tableName)
            .setOlderThanVid(vid).build();

    return catalogStub.expireSnapshots(expireSnapshotsRequest).getSuccess();
  }

  // TODO no server side implementation yet
  public boolean expireSnapshotsRetainLast(String workspaceName, String dbName, String tableName, int numSnapshots) {
    ExpireSnapshotsRequest expireSnapshotsRequest = ExpireSnapshotsRequest
            .newBuilder().setWorkspaceName(workspaceName).setDbName(dbName).setTableName(tableName)
            .setRetainLast(numSnapshots).build();

    return catalogStub.expireSnapshots(expireSnapshotsRequest).getSuccess();
  }

}

