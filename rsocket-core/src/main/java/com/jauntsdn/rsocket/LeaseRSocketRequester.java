package com.jauntsdn.rsocket;

import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.lease.RequesterLeaseHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import javax.annotation.Nullable;

class LeaseRSocketRequester extends RSocketRequester {
  private final RequesterLeaseHandler leaseHandler;

  LeaseRSocketRequester(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      StreamIdSupplier streamIdSupplier,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler,
      RequesterLeaseHandler leaseHandler,
      @Nullable LongConsumer onRtt) {
    super(
        allocator,
        connection,
        payloadDecoder,
        errorConsumer,
        streamIdSupplier,
        keepAliveTickPeriod,
        keepAliveAckTimeout,
        keepAliveHandler);
    this.leaseHandler = leaseHandler;
    keepAlive().onRtt(onRtt);
  }

  @Override
  Throwable checkAllowed() {
    Throwable err = super.checkAllowed();
    if (err != null) {
      return err;
    }
    leaseHandler.requestStarted();
    return err;
  }

  @Override
  void handleLeaseFrame(ByteBuf frame) {
    leaseHandler.receive(frame);
  }

  @Override
  public double availability() {
    return Math.min(super.availability(), leaseHandler.availability());
  }
}
