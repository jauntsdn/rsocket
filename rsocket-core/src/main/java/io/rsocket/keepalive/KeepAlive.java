/*
 * Copyright 2019 - present Maksym Ostroverkhov.
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

package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.resume.ResumeStateHolder;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

public abstract class KeepAlive implements Consumer<ByteBuf> {
  final Scheduler scheduler;
  final ByteBufAllocator allocator;
  final long timeoutMillis;
  final Consumer<ByteBuf> onFrameSent;
  ResumeStateHolder resumeStateHolder;
  Runnable onTimeout;

  long lastReceivedMillis;
  Disposable scheduleDisposable;

  private KeepAlive(
      Scheduler scheduler,
      ByteBufAllocator allocator,
      int timeoutMillis,
      Consumer<ByteBuf> onFrameSent) {
    this.scheduler = scheduler;
    this.allocator = allocator;
    this.timeoutMillis = timeoutMillis;
    this.onFrameSent = onFrameSent;
  }

  public KeepAlive onTimeout(Runnable onTimeout) {
    this.onTimeout = onTimeout;
    return this;
  }

  public KeepAlive resumeState(ResumeStateHolder resumeStateHolder) {
    this.resumeStateHolder = resumeStateHolder;
    return this;
  }

  public abstract KeepAlive start();

  public abstract void stop();

  @Override
  public void accept(ByteBuf keepAliveFrame) {
    this.lastReceivedMillis = System.currentTimeMillis();
    final ResumeStateHolder holder = this.resumeStateHolder;
    if (holder != null) {
      long remoteLastReceivedPos = remoteLastReceivedPosition(keepAliveFrame);
      holder.onImpliedPosition(remoteLastReceivedPos);
    }
    if (KeepAliveFrameFlyweight.respondFlag(keepAliveFrame)) {
      long localLastReceivedPos = localLastReceivedPosition(holder);
      send(
          KeepAliveFrameFlyweight.encode(
              allocator,
              false,
              localLastReceivedPos,
              KeepAliveFrameFlyweight.data(keepAliveFrame).retain()));
    }
  }

  void send(ByteBuf frame) {
    Consumer<ByteBuf> onSent = this.onFrameSent;
    if (onSent != null) {
      onSent.accept(frame);
    }
  }

  long localLastReceivedPosition(ResumeStateHolder holder) {
    return holder != null ? holder.impliedPosition() : 0;
  }

  long remoteLastReceivedPosition(ByteBuf keepAliveFrame) {
    return KeepAliveFrameFlyweight.lastPosition(keepAliveFrame);
  }

  void assertNotStarted() {
    if (scheduleDisposable != null) {
      throw new IllegalStateException("keep-alive start() called while started already");
    }
  }

  public static final class ServerKeepAlive extends KeepAlive {

    public ServerKeepAlive(
        Scheduler scheduler,
        ByteBufAllocator allocator,
        int timeoutMillis,
        Consumer<ByteBuf> onFrameSent) {
      super(scheduler, allocator, timeoutMillis, onFrameSent);
    }

    @Override
    public KeepAlive start() {
      assertNotStarted();
      this.lastReceivedMillis = System.currentTimeMillis();
      this.scheduleDisposable =
          scheduler.schedule(this::checkTimeout, timeoutMillis, TimeUnit.MILLISECONDS);
      return this;
    }

    @Override
    public void stop() {
      Disposable d = this.scheduleDisposable;
      if (d != null) {
        d.dispose();
        this.scheduleDisposable = null;
      }
    }

    private void checkTimeout() {
      final long now = System.currentTimeMillis();
      final long timeout = this.timeoutMillis;
      long sinceLastReceived = now - lastReceivedMillis;
      if (sinceLastReceived >= timeout) {
        final Runnable onTimeout = this.onTimeout;
        if (onTimeout != null) {
          onTimeout.run();
        }
        stop();
      } else {
        this.scheduleDisposable =
            scheduler.schedule(
                this::checkTimeout, timeout - sinceLastReceived, TimeUnit.MILLISECONDS);
      }
    }
  }

  public static final class ClientKeepAlive extends KeepAlive {
    private final int intervalMillis;

    public ClientKeepAlive(
        Scheduler scheduler,
        ByteBufAllocator allocator,
        int intervalMillis,
        int timeoutMillis,
        Consumer<ByteBuf> onFrameSent) {
      super(scheduler, allocator, timeoutMillis, onFrameSent);
      this.intervalMillis = intervalMillis;
    }

    @Override
    public KeepAlive start() {
      assertNotStarted();
      this.lastReceivedMillis = System.currentTimeMillis();
      int interval = this.intervalMillis;
      this.scheduleDisposable =
          scheduler.schedulePeriodically(
              this::checkTimeoutAndSendKeepAlive, interval, interval, TimeUnit.MILLISECONDS);

      return this;
    }

    @Override
    public void stop() {
      Disposable d = this.scheduleDisposable;
      if (d != null) {
        d.dispose();
        this.scheduleDisposable = null;
      }
    }

    private void checkTimeoutAndSendKeepAlive() {
      final long now = System.currentTimeMillis();
      final long timeout = this.timeoutMillis;
      final long sinceLastReceived = now - lastReceivedMillis;
      if (sinceLastReceived >= timeout) {
        final Runnable onTimeout = this.onTimeout;
        if (onTimeout != null) {
          onTimeout.run();
        }
        stop();
      } else {
        send(
            KeepAliveFrameFlyweight.encode(
                allocator,
                true,
                localLastReceivedPosition(this.resumeStateHolder),
                Unpooled.EMPTY_BUFFER));
      }
    }
  }
}
