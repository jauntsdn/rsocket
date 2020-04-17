/*
 * Copyright 2015-2018 the original author or authors.
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
package com.jauntsdn.rsocket;

import io.netty.util.collection.IntObjectMap;

final class StreamIdSupplier {
  private static final int MASK = 0x7FFFFFFF;
  static final int MAX_STREAM_ID = Integer.MAX_VALUE;

  long lastStreamId;

  StreamIdSupplier(int streamId) {
    this.lastStreamId = streamId;
  }

  static StreamIdSupplier clientSupplier() {
    return new StreamIdSupplier(-1);
  }

  static StreamIdSupplier serverSupplier() {
    return new StreamIdSupplier(0);
  }

  int nextStreamId(IntObjectMap<?> streamIds) {
    int wrappedStreamId;
    long streamId = lastStreamId;
    do {
      streamId = streamId + 2;
      if (streamId <= MAX_STREAM_ID) {
        lastStreamId = streamId;
        return (int) streamId;
      }
      wrappedStreamId = (int) (streamId & MASK);
    } while (wrappedStreamId == 0 || streamIds.containsKey(wrappedStreamId));
    lastStreamId = streamId;
    return wrappedStreamId;
  }
}
