/*
 * Copyright 2015-2019 the original author or authors.
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

package com.jauntsdn.rsocket.lease;

import com.jauntsdn.rsocket.Availability;
import com.jauntsdn.rsocket.Leases;
import com.jauntsdn.rsocket.exceptions.Exceptions;
import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.frame.LeaseFrameFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

public interface ResponderLeaseHandler extends Availability {

  /**
   * Called when first message of request is received
   *
   * @param requestType request type
   * @param metadata request metadata
   * @return allowed request identifier, or RejectException if no leases available
   */
  Signal<?> allowRequest(FrameType requestType, ByteBuf metadata);

  /**
   * Called when first signal of response (next, complete, error, cancel) is received.
   *
   * @param requestType request type
   * @param request request metadata
   * @param signal response first signal (next, complete, error, cancel). Next signal does not
   *     contain payload.
   * @param latencyNanos time between request start (receive) and response first signal
   */
  void responseStarted(FrameType requestType, Object request, Signal<?> signal, long latencyNanos);

  /**
   * Called when response is terminated (complete, error, cancel).
   *
   * @param requestType request type
   * @param request request metadata
   * @param signal response termination signal (complete, error, cancel).
   */
  void responseTerminated(FrameType requestType, Object request, Signal<?> signal);

  Disposable send(Consumer<ByteBuf> leaseFrameSender);

  boolean requireStats();

  @SuppressWarnings({"unchecked", "rawtypes"})
  final class Impl<T extends Leases.StatsRecorder> implements ResponderLeaseHandler {
    private static final Leases.StatsRecorder<?> NOOP_LEASE_STATS_RECORDER =
        new NoopLeaseStatsRecorder();
    private static final Signal<Void> LEASE_EXPIRED_ERROR =
        SignalImpl.error(Exceptions.LEASE_EXPIRED_EXCEPTION);
    private static final Signal<Void> LEASE_EXHAUSTED_ERROR =
        SignalImpl.error(Exceptions.LEASE_EXHAUSTED_EXCEPTION);

    private volatile LeaseImpl currentLease = LeaseImpl.empty();
    private final ByteBufAllocator allocator;
    private final Function<Optional<T>, Flux<Lease>> leaseSender;
    private final Consumer<Throwable> errorConsumer;
    private final Optional<T> leaseStatsOption;
    private final T leaseStats;

    public Impl(
        ByteBufAllocator allocator,
        Function<Optional<T>, Flux<Lease>> leaseSender,
        Consumer<Throwable> errorConsumer,
        Optional<T> leaseStatsOption) {
      this.allocator = allocator;
      this.leaseSender = leaseSender;
      this.errorConsumer = errorConsumer;
      this.leaseStatsOption = leaseStatsOption;
      this.leaseStats = leaseStatsOption.orElse((T) NOOP_LEASE_STATS_RECORDER);
    }

    @Override
    public Signal<?> allowRequest(FrameType requestType, ByteBuf metadata) {
      LeaseImpl lease = this.currentLease;
      T stats = this.leaseStats;
      Object request = stats.onRequestStarted(requestType, metadata);
      if (lease.use()) {
        return SignalImpl.next(request);
      } else {
        Signal<Void> e = lease.isExpired() ? LEASE_EXPIRED_ERROR : LEASE_EXHAUSTED_ERROR;
        stats.onResponseStarted(requestType, request, e, 0);
        stats.onResponseTerminated(requestType, request, e);
        return SignalImpl.error(e.getThrowable());
      }
    }

    @Override
    public void responseStarted(
        FrameType requestType, Object request, Signal<?> firstSignal, long latencyNanos) {
      Signal<?> signal = firstSignal.isOnNext() ? SignalImpl.nextEmpty() : firstSignal;
      leaseStats.onResponseStarted(requestType, request, signal, latencyNanos);
    }

    @Override
    public void responseTerminated(FrameType requestType, Object request, Signal<?> lastSignal) {
      Signal<?> signal = lastSignal.isOnNext() ? SignalImpl.complete() : lastSignal;
      leaseStats.onResponseTerminated(requestType, request, signal);
    }

    @Override
    public Disposable send(Consumer<ByteBuf> leaseFrameSender) {
      return leaseSender
          .apply(leaseStatsOption)
          .doOnCancel(leaseStats::onRSocketClosed)
          .subscribe(
              lease -> {
                currentLease = create(lease);
                leaseFrameSender.accept(createLeaseFrame(lease));
              },
              errorConsumer);
    }

    @Override
    public boolean requireStats() {
      return leaseStats != NOOP_LEASE_STATS_RECORDER;
    }

    @Override
    public double availability() {
      return currentLease.availability();
    }

    private ByteBuf createLeaseFrame(Lease lease) {
      return LeaseFrameFlyweight.encode(
          allocator, lease.getTimeToLiveMillis(), lease.getAllowedRequests(), lease.getMetadata());
    }

    private static LeaseImpl create(Lease lease) {
      if (lease instanceof LeaseImpl) {
        return (LeaseImpl) lease;
      } else {
        return LeaseImpl.create(
            lease.getTimeToLiveMillis(), lease.getAllowedRequests(), lease.getMetadata());
      }
    }

    private static class NoopLeaseStatsRecorder
        implements Leases.StatsRecorder<NoopLeaseStatsRecorder> {

      @Override
      public NoopLeaseStatsRecorder onRequestStarted(FrameType requestType, ByteBuf metadata) {
        return this;
      }

      @Override
      public void onResponseStarted(
          FrameType requestType,
          NoopLeaseStatsRecorder request,
          Signal<Void> firstSignal,
          long latencyNanos) {}

      @Override
      public void onResponseTerminated(
          FrameType requestType, NoopLeaseStatsRecorder request, Signal<Void> lastSignal) {}

      @Override
      public void onRSocketClosed() {}
    }

    static class SignalImpl<T> implements Signal<T> {
      private static final Signal<Void> NEXT_EMPTY = new SignalImpl(null, null, SignalType.ON_NEXT);
      private static final Signal<Void> COMPLETE =
          new SignalImpl(null, null, SignalType.ON_COMPLETE);
      private static final Signal<?> CANCEL = new SignalImpl(null, null, SignalType.CANCEL);

      private final T value;
      private final Throwable err;
      private final SignalType signalType;

      private SignalImpl(@Nullable T value, @Nullable Throwable err, SignalType signalType) {
        this.value = value;
        this.err = err;
        this.signalType = signalType;
      }

      public static <T> Signal<T> complete() {
        return (Signal<T>) COMPLETE;
      }

      public static Signal<Void> nextEmpty() {
        return NEXT_EMPTY;
      }

      public static <T> Signal<T> cancel() {
        return (Signal<T>) CANCEL;
      }

      public static <T> Signal<T> next(T value) {
        return new SignalImpl<>(value, null, SignalType.ON_NEXT);
      }

      public static <T> Signal<T> error(Throwable err) {
        return new SignalImpl<>(null, err, SignalType.ON_ERROR);
      }

      @Override
      public Throwable getThrowable() {
        return err;
      }

      @Override
      public Subscription getSubscription() {
        return null;
      }

      @Override
      public T get() {
        return value;
      }

      @Override
      public SignalType getType() {
        return signalType;
      }

      @Override
      public Context getContext() {
        return Context.empty();
      }

      @Override
      public boolean isOnComplete() {
        return signalType == SignalType.ON_COMPLETE;
      }

      @Override
      public boolean isOnNext() {
        return signalType == SignalType.ON_NEXT;
      }

      @Override
      public boolean isOnError() {
        return signalType == SignalType.ON_ERROR;
      }
    }
  }
}
