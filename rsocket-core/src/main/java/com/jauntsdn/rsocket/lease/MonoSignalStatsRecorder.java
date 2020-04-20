/*
 * Copyright 2020 - present Maksym Ostroverkhov.
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

package com.jauntsdn.rsocket.lease;

import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.lease.ResponderLeaseHandler.Impl.SignalImpl;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import reactor.core.publisher.Signal;

public class MonoSignalStatsRecorder implements Consumer<Signal<?>>, Runnable {
  private static final AtomicIntegerFieldUpdater<MonoSignalStatsRecorder> IS_TERMINATED =
      AtomicIntegerFieldUpdater.newUpdater(MonoSignalStatsRecorder.class, "isTerminated");

  private final ResponderLeaseHandler leaseHandler;
  private final FrameType requestType;
  private final Object request;
  private final long requestStartNanos = System.nanoTime();
  private volatile int isTerminated;

  public MonoSignalStatsRecorder(
      ResponderLeaseHandler leaseHandler, FrameType requestType, Object request) {
    this.leaseHandler = leaseHandler;
    this.requestType = requestType;
    this.request = request;
  }

  @Override
  public void accept(Signal<?> signal) {
    switch (signal.getType()) {
      case ON_NEXT:
      case ON_ERROR:
        if (IS_TERMINATED.compareAndSet(this, 0, 1)) {
          leaseHandler.responseStarted(
              requestType, request, signal, System.nanoTime() - requestStartNanos);
          leaseHandler.responseTerminated(requestType, request, signal);
          break;
        }

      default:
        // noop
    }
  }

  /*onCancel callback*/
  @Override
  public void run() {
    if (IS_TERMINATED.compareAndSet(this, 0, 1)) {
      Signal<Object> cancel = SignalImpl.cancel();
      leaseHandler.responseStarted(
          requestType, request, cancel, System.nanoTime() - requestStartNanos);
      leaseHandler.responseTerminated(requestType, request, cancel);
    }
  }
}
