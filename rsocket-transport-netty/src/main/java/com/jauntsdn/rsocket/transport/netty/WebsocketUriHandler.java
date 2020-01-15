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

import com.jauntsdn.rsocket.transport.ClientTransport;
import com.jauntsdn.rsocket.transport.ServerTransport;
import com.jauntsdn.rsocket.transport.netty.client.WebsocketClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.WebsocketServerTransport;
import com.jauntsdn.rsocket.uri.UriHandler;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link UriHandler} that creates {@link WebsocketClientTransport}s and {@link
 * WebsocketServerTransport}s.
 */
public final class WebsocketUriHandler implements UriHandler {

  private static final List<String> SCHEME = Arrays.asList("ws", "wss", "http", "https");

  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    Objects.requireNonNull(uri, "uri must not be null");

    if (SCHEME.stream().noneMatch(scheme -> scheme.equals(uri.getScheme()))) {
      return Optional.empty();
    }

    return Optional.of(WebsocketClientTransport.create(uri));
  }

  @Override
  public Optional<ServerTransport> buildServer(URI uri) {
    Objects.requireNonNull(uri, "uri must not be null");

    if (SCHEME.stream().noneMatch(scheme -> scheme.equals(uri.getScheme()))) {
      return Optional.empty();
    }

    int port = UriUtils.isSecure(uri) ? UriUtils.getPort(uri, 443) : UriUtils.getPort(uri, 80);

    return Optional.of(WebsocketServerTransport.create(uri.getHost(), port));
  }
}
