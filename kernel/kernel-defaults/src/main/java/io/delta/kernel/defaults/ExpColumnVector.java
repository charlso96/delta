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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;

import java.util.List;

public class ExpColumnVector {
  private ExpColumnVector() {}

  public static ColumnVector intVector(List<Integer> data) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return IntegerType.INTEGER;
      }

      @Override
      public int getSize() {
        return data.size();
      }

      @Override
      public void close() {
      }

      @Override
      public boolean isNullAt(int rowId) {
        return data.get(rowId) == null;
      }

      @Override
      public int getInt(int rowId) {
        return data.get(rowId);
      }
    };
  }

  public static ColumnVector doubleVector(List<Double> data) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return DoubleType.DOUBLE;
      }

      @Override
      public int getSize() {
        return data.size();
      }

      @Override
      public void close() {
      }

      @Override
      public boolean isNullAt(int rowId) {
        return data.get(rowId) == null;
      }

      @Override
      public double getDouble(int rowId) {
        return data.get(rowId);
      }
    };
  }

  public static ColumnVector stringVector(List<String> data) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return StringType.STRING;
      }

      @Override
      public int getSize() {
        return data.size();
      }

      @Override
      public void close() {
      }

      @Override
      public boolean isNullAt(int rowId) {
        return data.get(rowId) == null;
      }

      @Override
      public String getString(int rowId) {
        return data.get(rowId);
      }
    };
  }

  public static ColumnVector stringSingleValueVector(String value, int size) {
    return new ColumnVector() {
      @Override
      public DataType getDataType() {
        return StringType.STRING;
      }

      @Override
      public int getSize() {
        return size;
      }

      @Override
      public void close() {

      }

      @Override
      public boolean isNullAt(int rowId) {
        return value == null;
      }

      @Override
      public String getString(int rowId) {
        return value;
      }
    };
  }
}
