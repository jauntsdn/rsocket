/*
 * Copyright 2015-2018 the original author or authors.
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

package com.jauntsdn.rsocket.transport.netty;

import com.jauntsdn.rsocket.RSocketFactory;
import com.jauntsdn.rsocket.frame.decoder.PayloadDecoder;
import com.jauntsdn.rsocket.test.PingHandler;
import com.jauntsdn.rsocket.transport.netty.server.WebsocketServerTransport;

public final class WebsocketPongServer {

  public static void main(String... args) {
    RSocketFactory.receive()
        .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor(new PingHandler())
        .transport(WebsocketServerTransport.create(7878))
        .start()
        .block()
        .onClose()
        .block();
  }
}
