/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.hook;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.File2;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumWriter;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;

import java.io.IOException;
import java.util.List;

/**
 * A post-commit hook that writes a new checksum file at the version committed by the transaction.
 * This hook performs a simple checksum operation without requiring previous checkpoint or log
 * reading.
 */
public class ChecksumSimpleHook2 implements PostCommitHook {

  private final CRCInfo crcInfo;
  private final Path logPath;
  private final List<File2> fileLogs;

  public ChecksumSimpleHook2(CRCInfo crcInfo, Path logPath, List<File2> fileLogs) {
    this.crcInfo = requireNonNull(crcInfo);
    this.logPath = requireNonNull(logPath);
    this.fileLogs = fileLogs;
  }

  @Override
  public void threadSafeInvoke(Engine engine) throws IOException {
    checkArgument(engine != null);
    fileLogs.add(new File2(FileNames.checksumFile(logPath, crcInfo.getVersion()).toString(),
            File2.File2Type.ADD, "crc"));
    new ChecksumWriter(logPath).writeCheckSum(engine, crcInfo);
  }

  @Override
  public PostCommitHookType getType() {
    return PostCommitHookType.CHECKSUM_SIMPLE;
  }
}
