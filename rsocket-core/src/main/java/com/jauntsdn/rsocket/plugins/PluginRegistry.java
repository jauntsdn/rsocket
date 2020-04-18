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

package com.jauntsdn.rsocket.plugins;

import com.jauntsdn.rsocket.*;
import java.util.ArrayList;
import java.util.List;

public class PluginRegistry {
  private List<DuplexConnectionInterceptor> connections = new ArrayList<>();
  private List<RSocketInterceptor> requesters = new ArrayList<>();
  private List<RSocketInterceptor> responders = new ArrayList<>();
  private List<ClientAcceptorInterceptor> clientAcceptorInterceptors = new ArrayList<>();
  private List<ServerAcceptorInterceptor> serverAcceptorInterceptors = new ArrayList<>();

  public PluginRegistry() {}

  public PluginRegistry(PluginRegistry defaults) {
    this.connections.addAll(defaults.connections);
    this.requesters.addAll(defaults.requesters);
    this.responders.addAll(defaults.responders);
  }

  public void addConnectionPlugin(DuplexConnectionInterceptor interceptor) {
    connections.add(interceptor);
  }

  public void addRequesterPlugin(RSocketInterceptor interceptor) {
    requesters.add(interceptor);
  }

  public void addResponderPlugin(RSocketInterceptor interceptor) {
    responders.add(interceptor);
  }

  public void addClientAcceptorPlugin(ClientAcceptorInterceptor interceptor) {
    clientAcceptorInterceptors.add(interceptor);
  }

  public void addServerAcceptorPlugin(ServerAcceptorInterceptor interceptor) {
    serverAcceptorInterceptors.add(interceptor);
  }

  public RSocket applyRequester(RSocket rSocket) {
    for (RSocketInterceptor i : requesters) {
      rSocket = i.apply(rSocket);
    }

    return rSocket;
  }

  public RSocket applyResponder(RSocket rSocket) {
    for (RSocketInterceptor i : responders) {
      rSocket = i.apply(rSocket);
    }

    return rSocket;
  }

  public ClientSocketAcceptor applyClientSocketAcceptor(ClientSocketAcceptor acceptor) {
    for (ClientAcceptorInterceptor i : clientAcceptorInterceptors) {
      acceptor = i.apply(acceptor);
    }

    return acceptor;
  }

  public ServerSocketAcceptor applyServerSocketAcceptor(ServerSocketAcceptor acceptor) {
    for (ServerAcceptorInterceptor i : serverAcceptorInterceptors) {
      acceptor = i.apply(acceptor);
    }

    return acceptor;
  }

  public DuplexConnection applyConnection(
      DuplexConnectionInterceptor.Type type, DuplexConnection connection) {
    for (DuplexConnectionInterceptor i : connections) {
      connection = i.apply(type, connection);
    }

    return connection;
  }
}
