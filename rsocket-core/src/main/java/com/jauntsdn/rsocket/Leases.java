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

package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.frame.FrameType;
import com.jauntsdn.rsocket.lease.Lease;
import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;

/**
 * Configures stats recorder and permits sender of requests lease.
 *
 * @param <T> {@link Leases.StatsRecorder} configured by this class
 */
public class Leases<T extends Leases.StatsRecorder<?>> {
  private static final Function<?, Flux<Lease>> noopLeaseSender = leaseStats -> Flux.never();

  private Function<?, Flux<Lease>> leaseSender = noopLeaseSender;
  private Optional<T> statsRecorder = Optional.empty();

  /**
   * Creates new {@link Leases} instance for requests lease configuration
   *
   * @param <T> {@link Leases.StatsRecorder} configured by this class
   * @return new Requests
   */
  public static <T extends StatsRecorder<?>> Leases<T> create() {
    return new Leases<>();
  }

  /**
   * Configures leases sender
   *
   * @param leaseSender function accepting optional {@link Leases.StatsRecorder} returning {@link
   *     Flux} of Leases that should be sent to peer Requester
   * @return this {@link Leases} instance
   */
  public Leases<T> sender(Function<Optional<T>, Flux<Lease>> leaseSender) {
    this.leaseSender = Objects.requireNonNull(leaseSender, "leaseSender");
    return this;
  }

  /**
   * Sets requests stats recorder
   *
   * @param statsRecorder {@link Leases.StatsRecorder} used to record requests statistics, and
   *     presented to {@link #sender(Function)} to calculate Lease permits
   * @return this {@link Leases} instance
   */
  public Leases<T> stats(T statsRecorder) {
    this.statsRecorder = Optional.of(Objects.requireNonNull(statsRecorder, "statsRecorder"));
    return this;
  }

  /** @return leases sender configured by {@link #sender(Function)} */
  @SuppressWarnings("unchecked")
  public Function<Optional<StatsRecorder<?>>, Flux<Lease>> sender() {
    return (Function<Optional<StatsRecorder<?>>, Flux<Lease>>) leaseSender;
  }

  /** @return requests stats configured by {@link #stats(StatsRecorder)} */
  @SuppressWarnings("unchecked")
  public Optional<StatsRecorder<?>> stats() {
    return (Optional<StatsRecorder<?>>) statsRecorder;
  }

  /**
   * Records requests stats presented to Lease sender for allowed requests and time-to-live
   * calculation.
   *
   * @param <T> type of the request as function of RSocket interaction type and request metadata
   */
  public interface StatsRecorder<T> {

    /**
     * Called when Responder receives new request
     *
     * @param requestType RSocket interaction type: one of {@link FrameType#REQUEST_FNF}, {@link
     *     FrameType#REQUEST_RESPONSE}, {@link FrameType#REQUEST_STREAM}, {@link
     *     FrameType#REQUEST_CHANNEL}
     * @param metadata request metadata
     * @return logical name of received request. For example, for RSocket-RPC this can be string of
     *     form "service/method"
     */
    T onRequestStarted(FrameType requestType, ByteBuf metadata);

    /**
     * Called when Responder request handler sends first signal of response - one of ON_NEXT,
     * ON_COMPLETE, ON_ERROR, CANCEL
     *
     * @param requestType RSocket interaction type: one of {@link FrameType#REQUEST_FNF}, {@link
     *     FrameType#REQUEST_RESPONSE}, {@link FrameType#REQUEST_STREAM}, {@link
     *     FrameType#REQUEST_CHANNEL}
     * @param request logical name of received request
     * @param firstSignal first signal of response - one of ON_NEXT, ON_COMPLETE, ON_ERROR, CANCEL
     * @param latencyNanos interval between request is received and first response signal is sent.
     *     Is 0 if responder rejected request due to missing lease.
     */
    void onResponseStarted(
        FrameType requestType, T request, Signal<Void> firstSignal, long latencyNanos);

    /**
     * Called when Responder response is terminated: one of ON_COMPLETE, ON_ERROR, CANCEL
     *
     * @param requestType RSocket interaction type: one of {@link FrameType#REQUEST_FNF}, {@link
     *     FrameType#REQUEST_RESPONSE}, {@link FrameType#REQUEST_STREAM}, {@link
     *     FrameType#REQUEST_CHANNEL}
     * @param request logical name of received request
     * @param lastSignal last signal of response - one of ON_COMPLETE, ON_ERROR, CANCEL
     */
    void onResponseTerminated(FrameType requestType, T request, Signal<Void> lastSignal);

    /** Called when RSocket is terminated */
    void onRSocketClosed();
  }

  /** Configures Client {@link Leases} */
  public interface ClientConfigurer {

    /**
     * @param rtt {@link Flux} emitting round-trip times of frames, in milliseconds
     * @param scheduler {@link Scheduler} associated with this RSocket
     * @return configured {@link Leases} instance
     */
    Leases<?> configure(Flux<Long> rtt, Scheduler scheduler);
  }

  /** Configures Server {@link Leases} */
  public interface ServerConfigurer {

    /**
     * @param scheduler {@link Scheduler} associated with this RSocket
     * @return configured {@link Leases} instance
     */
    Leases<?> configure(Scheduler scheduler);
  }
}
