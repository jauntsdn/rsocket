package com.jauntsdn.rsocket;

import static com.jauntsdn.rsocket.RSocketErrorMappers.*;
import static com.jauntsdn.rsocket.StreamErrorMappers.*;

import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.lease.RequesterLeaseHandler;
import com.jauntsdn.rsocket.lease.ResponderLeaseHandler;
import io.netty.buffer.ByteBufAllocator;
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
      StreamErrorMapper streamErrorMapper);

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
      KeepAliveHandler keepAliveHandler);

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
        StreamErrorMapper streamErrorMapper) {

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
          leaseHandler);
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
        KeepAliveHandler keepAliveHandler) {
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
        StreamErrorMapper streamErrorMapper) {

      return new RSocketResponder(
          allocator, connection, requestHandler, payloadDecoder, errorConsumer, streamErrorMapper);
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
        KeepAliveHandler keepAliveHandler) {
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
          keepAliveHandler);
    }
  }
}
