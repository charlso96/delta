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

import io.delta.kernel.File2;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.fs.Path;
import java.io.IOException;
import java.util.List;

/** Write a new checkpoint at the version committed by the txn. */
public class CheckpointHook2 implements PostCommitHook {

  private final Path tablePath;
  private final long checkpointVersion;
  private final List<File2> fileLogs;

  public CheckpointHook2(Path tablePath, long checkpointVersion, List<File2> fileLogs) {
    this.tablePath = tablePath;
    this.checkpointVersion = checkpointVersion;
    this.fileLogs = fileLogs;
  }

  @Override
  public void threadSafeInvoke(Engine engine) throws IOException {
    Table.forPath(engine, tablePath.toString()).checkpoint2(engine, checkpointVersion, fileLogs);
  }

  @Override
  public PostCommitHookType getType() {
    return PostCommitHookType.CHECKPOINT;
  }
}
