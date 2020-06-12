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

package com.jauntsdn.rsocket;

import static com.jauntsdn.rsocket.RSocketErrorMappers.*;
import static com.jauntsdn.rsocket.StreamErrorMappers.*;

import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.lease.RequesterLeaseHandler;
import com.jauntsdn.rsocket.lease.ResponderLeaseHandler;
import io.netty.buffer.ByteBufAllocator;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.scheduler.Scheduler;

interface RSocketsFactory {

  RSocketResponder createResponder(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamErrorMapper streamErrorMapper,
      RSocketErrorMapper rSocketErrorMapper,
      int metadataPushLimit,
      Duration metadataPushLimitInterval);

  RSocketRequester createRequester(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamErrorMapper streamErrorMapper,
      RSocketErrorMapper rSocketErrorMapper,
      StreamIdSupplier streamIdSupplier,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler,
      Duration gracefulShutdownTimeout);

  static RSocketsFactory createClient(
      boolean isLease, Scheduler scheduler, Leases.ClientConfigurer leaseConfigurer) {
    if (isLease) {
      EmitterProcessor<Long> rtt = EmitterProcessor.create();
      return new LeaseRSocketsFactory(rtt, leaseConfigurer.configure(rtt, scheduler));
    } else {
      return new DefaultRSocketsFactory();
    }
  }

  static RSocketsFactory createServer(
      boolean isLease, Scheduler scheduler, Leases.ServerConfigurer leaseConfigurer) {
    if (isLease) {
      return new LeaseRSocketsFactory(leaseConfigurer.configure(scheduler));
    } else {
      return new DefaultRSocketsFactory();
    }
  }

  final class LeaseRSocketsFactory implements RSocketsFactory {
    private final LongConsumer rttConsumer;
    private final Leases<?> leases;

    public LeaseRSocketsFactory(@Nullable Subscriber<Long> rttConsumer, Leases<?> leases) {
      this.rttConsumer = rttConsumer == null ? null : rttConsumer::onNext;
      this.leases = leases;
    }

    public LeaseRSocketsFactory(Leases<?> leases) {
      this(null, leases);
    }

    @Override
    public RSocketResponder createResponder(
        ByteBufAllocator allocator,
        DuplexConnection connection,
        RSocket requestHandler,
        PayloadDecoder payloadDecoder,
        Consumer<Throwable> errorConsumer,
        StreamErrorMapper streamErrorMapper,
        RSocketErrorMapper rSocketErrorMapper,
        int metadataPushLimit,
        Duration metadataPushLimitInterval) {

      ResponderLeaseHandler leaseHandler =
          new ResponderLeaseHandler.Impl<>(
              allocator, leases.sender(), errorConsumer, leases.stats());

      return new LeaseRSocketResponder(
          allocator,
          connection,
          requestHandler,
          payloadDecoder,
          errorConsumer,
          streamErrorMapper,
          rSocketErrorMapper,
          leaseHandler,
          metadataPushLimit,
          metadataPushLimitInterval);
    }

    @Override
    public RSocketRequester createRequester(
        ByteBufAllocator allocator,
        DuplexConnection connection,
        PayloadDecoder payloadDecoder,
        Consumer<Throwable> errorConsumer,
        StreamErrorMapper streamErrorMapper,
        RSocketErrorMapper rSocketErrorMapper,
        StreamIdSupplier streamIdSupplier,
        int keepAliveTickPeriod,
        int keepAliveAckTimeout,
        KeepAliveHandler keepAliveHandler,
        Duration gracefulShutdownTimeout) {
      RequesterLeaseHandler.Impl leaseHandler = new RequesterLeaseHandler.Impl();

      return new LeaseRSocketRequester(
          allocator,
          connection,
          payloadDecoder,
          errorConsumer,
          streamErrorMapper,
          rSocketErrorMapper,
          streamIdSupplier,
          keepAliveTickPeriod,
          keepAliveAckTimeout,
          keepAliveHandler,
          gracefulShutdownTimeout,
          leaseHandler,
          rttConsumer);
    }
  }

  final class DefaultRSocketsFactory implements RSocketsFactory {

    @Override
    public RSocketResponder createResponder(
        ByteBufAllocator allocator,
        DuplexConnection connection,
        RSocket requestHandler,
        PayloadDecoder payloadDecoder,
        Consumer<Throwable> errorConsumer,
        StreamErrorMapper streamErrorMapper,
        RSocketErrorMapper rSocketErrorMapper,
        int metadataPushLimit,
        Duration metadataPushLimitInterval) {

      return new RSocketResponder(
          allocator,
          connection,
          requestHandler,
          payloadDecoder,
          errorConsumer,
          streamErrorMapper,
          rSocketErrorMapper,
          metadataPushLimit,
          metadataPushLimitInterval);
    }

    @Override
    public RSocketRequester createRequester(
        ByteBufAllocator allocator,
        DuplexConnection connection,
        PayloadDecoder payloadDecoder,
        Consumer<Throwable> errorConsumer,
        StreamErrorMapper streamErrorMapper,
        RSocketErrorMapper rSocketErrorMapper,
        StreamIdSupplier streamIdSupplier,
        int keepAliveTickPeriod,
        int keepAliveAckTimeout,
        KeepAliveHandler keepAliveHandler,
        Duration gracefulShutdownTimeout) {
      return new RSocketRequester(
          allocator,
          connection,
          payloadDecoder,
          errorConsumer,
          streamErrorMapper,
          rSocketErrorMapper,
          streamIdSupplier,
          keepAliveTickPeriod,
          keepAliveAckTimeout,
          keepAliveHandler,
          gracefulShutdownTimeout);
    }
  }
}
