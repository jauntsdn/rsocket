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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.time.Duration;
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
      StreamErrorMapper streamErrorMapper,
      RSocketErrorMapper rSocketErrorMapper,
      StreamIdSupplier streamIdSupplier,
      int keepAliveTickPeriod,
      int keepAliveAckTimeout,
      KeepAliveHandler keepAliveHandler,
      Duration gracefulDisposeTimeout,
      boolean validate,
      RequesterLeaseHandler leaseHandler,
      @Nullable LongConsumer onRtt) {
    super(
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
        gracefulDisposeTimeout,
        validate);
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
