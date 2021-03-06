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

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.lease.ResponderLeaseHandler.Impl.SignalImpl;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import reactor.core.publisher.Signal;

public class FluxSignalStatsRecorder implements Consumer<Signal<Payload>>, Runnable {
  private static final AtomicIntegerFieldUpdater<FluxSignalStatsRecorder> STATE =
      AtomicIntegerFieldUpdater.newUpdater(FluxSignalStatsRecorder.class, "state");
  private static final int STATE_NO_SIGNALS = 0;
  private static final int STATE_FIRST_SIGNAL = 1;
  private static final int STATE_TERMINAL_SIGNAL = 2;

  private final ResponderLeaseHandler leaseHandler;
  private final FrameType requestType;
  private final Object request;
  private final long requestStartNanos = System.nanoTime();
  private volatile int state;

  public FluxSignalStatsRecorder(
      ResponderLeaseHandler leaseHandler, FrameType requestType, Object request) {
    this.leaseHandler = leaseHandler;
    this.requestType = requestType;
    this.request = request;
  }

  @Override
  public void accept(Signal<Payload> signal) {
    switch (signal.getType()) {
      case ON_COMPLETE:
      case ON_ERROR:
        tryTerminate(signal);
        break;
      case ON_NEXT:
        tryNext(signal);
        break;
      default:
        // noop
    }
  }

  /*onCancel callback*/
  @Override
  public void run() {
    tryTerminate(SignalImpl.cancel());
  }

  private void tryNext(Signal<Payload> signal) {
    if (STATE.compareAndSet(this, STATE_NO_SIGNALS, STATE_FIRST_SIGNAL)) {
      leaseHandler.responseStarted(
          requestType, request, signal, System.nanoTime() - requestStartNanos);
    }
  }

  private void tryTerminate(Signal<?> signal) {
    if (STATE.compareAndSet(this, STATE_NO_SIGNALS, STATE_TERMINAL_SIGNAL)) {
      leaseHandler.responseStarted(
          requestType, request, signal, System.nanoTime() - requestStartNanos);
      leaseHandler.responseTerminated(requestType, request, signal);
    } else if (STATE.compareAndSet(this, STATE_FIRST_SIGNAL, STATE_TERMINAL_SIGNAL)) {
      leaseHandler.responseTerminated(requestType, request, signal);
    }
  }
}
