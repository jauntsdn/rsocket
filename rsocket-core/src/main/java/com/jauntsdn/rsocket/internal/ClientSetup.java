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

package com.jauntsdn.rsocket.internal;

import static com.jauntsdn.rsocket.keepalive.KeepAliveHandler.*;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.keepalive.KeepAliveHandler;
import com.jauntsdn.rsocket.resume.ClientRSocketSession;
import com.jauntsdn.rsocket.resume.ResumableDuplexConnection;
import com.jauntsdn.rsocket.resume.ResumableFramesStore;
import com.jauntsdn.rsocket.resume.ResumeStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.time.Duration;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public interface ClientSetup {

  DuplexConnection connection();

  KeepAliveHandler keepAliveHandler();

  class DefaultClientSetup implements ClientSetup {
    private final DuplexConnection connection;

    public DefaultClientSetup(DuplexConnection connection) {
      this.connection = connection;
    }

    @Override
    public DuplexConnection connection() {
      return connection;
    }

    @Override
    public KeepAliveHandler keepAliveHandler() {
      return new DefaultKeepAliveHandler(connection);
    }
  }

  class ResumableClientSetup implements ClientSetup {
    private final ResumableDuplexConnection duplexConnection;
    private final ResumableKeepAliveHandler keepAliveHandler;

    public ResumableClientSetup(
        ByteBufAllocator allocator,
        DuplexConnection connection,
        Mono<DuplexConnection> newConnectionFactory,
        ByteBuf resumeToken,
        ResumableFramesStore resumableFramesStore,
        Duration resumeSessionDuration,
        Duration resumeStreamTimeout,
        Supplier<ResumeStrategy> resumeStrategySupplier,
        boolean cleanupStoreOnKeepAlive,
        boolean validate) {

      ClientRSocketSession rSocketSession =
          new ClientRSocketSession(
                  connection,
                  allocator,
                  resumeSessionDuration,
                  resumeStrategySupplier,
                  resumableFramesStore,
                  resumeStreamTimeout,
                  cleanupStoreOnKeepAlive,
                  validate)
              .continueWith(newConnectionFactory)
              .resumeToken(resumeToken);
      this.duplexConnection = rSocketSession.resumableConnection();
      this.keepAliveHandler = new ResumableKeepAliveHandler(duplexConnection);
    }

    @Override
    public DuplexConnection connection() {
      return duplexConnection;
    }

    @Override
    public KeepAliveHandler keepAliveHandler() {
      return keepAliveHandler;
    }
  }
}
