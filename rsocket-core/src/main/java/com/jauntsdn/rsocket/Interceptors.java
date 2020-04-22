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

import com.jauntsdn.rsocket.interceptors.ClientAcceptorInterceptor;
import com.jauntsdn.rsocket.interceptors.DuplexConnectionInterceptor;
import com.jauntsdn.rsocket.interceptors.RSocketInterceptor;
import com.jauntsdn.rsocket.interceptors.ServerAcceptorInterceptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import reactor.core.scheduler.Scheduler;

/** Holder for RSocket and connection level interceptors */
public class Interceptors {
  private static final int EXPECTED_INTERCEPTORS_COUNT = 4;

  private static final Interceptors GLOBAL = createGlobal();
  private static final Interceptors NOOP = createNoop();

  private final Interceptors startingInterceptors;
  private List<DuplexConnectionInterceptor> connections = Collections.emptyList();
  private List<RSocketInterceptor> requesters = Collections.emptyList();
  private List<RSocketInterceptor> handlers = Collections.emptyList();
  private List<ClientAcceptorInterceptor> clientAcceptors = Collections.emptyList();
  private List<ServerAcceptorInterceptor> serverAcceptors = Collections.emptyList();

  private Interceptors(@Nullable Interceptors startingInterceptors) {
    this.startingInterceptors = startingInterceptors;
  }

  /** Configures RSocket and connection interceptors */
  public interface Configurer {

    /**
     * @param scheduler transport scheduler
     * @return configured RSocket and connection interceptors
     */
    Interceptors configure(Scheduler scheduler);
  }

  /** @return new holder for RSocket and connection interceptors */
  public static Interceptors create() {
    return create(GLOBAL);
  }

  /** @return holder for JVM-wide RSocket and connection interceptors */
  public static Interceptors global() {
    return GLOBAL;
  }

  static Interceptors noop() {
    return NOOP;
  }

  private static Interceptors createNoop() {
    return create(GLOBAL);
  }

  private static Interceptors createGlobal() {
    return create(null);
  }

  private static Interceptors create(@Nullable Interceptors startingInterceptors) {
    return new Interceptors(startingInterceptors);
  }

  private static <T> List<T> createInterceptorsList() {
    return new ArrayList<>(EXPECTED_INTERCEPTORS_COUNT);
  }

  /**
   * Adds connection (frames) level interceptor
   *
   * @param interceptor connection interceptor
   * @return this {@link Interceptors} instance
   */
  public Interceptors connection(DuplexConnectionInterceptor interceptor) {
    List<DuplexConnectionInterceptor> c = connections;
    if (c.isEmpty()) {
      connections = c = createInterceptorsList();
    }
    c.add(interceptor);
    return this;
  }

  /**
   * Adds RSocket requester interceptor
   *
   * @param interceptor requester interceptor
   * @return this {@link Interceptors} instance
   */
  public Interceptors requester(RSocketInterceptor interceptor) {
    List<RSocketInterceptor> r = requesters;
    if (r.isEmpty()) {
      requesters = r = createInterceptorsList();
    }
    r.add(interceptor);
    return this;
  }

  /**
   * Adds RSocket requests handler interceptor
   *
   * @param interceptor requests handler interceptor
   * @return this {@link Interceptors} instance
   */
  public Interceptors handler(RSocketInterceptor interceptor) {
    List<RSocketInterceptor> h = handlers;
    if (h.isEmpty()) {
      handlers = h = createInterceptorsList();
    }
    h.add(interceptor);
    return this;
  }

  /**
   * Adds client socket acceptor interceptor. Applied to client side of connection only.
   *
   * @param interceptor client acceptor interceptor
   * @return this {@link Interceptors} instance
   */
  public Interceptors clientAcceptor(ClientAcceptorInterceptor interceptor) {
    List<ClientAcceptorInterceptor> a = clientAcceptors;
    if (a.isEmpty()) {
      clientAcceptors = a = createInterceptorsList();
    }
    a.add(interceptor);
    return this;
  }

  /**
   * Adds server socket acceptor interceptor. Applied to server side of connection only.
   *
   * @param interceptor server acceptor interceptor
   * @return this {@link Interceptors} instance
   */
  public Interceptors serverAcceptor(ServerAcceptorInterceptor interceptor) {
    List<ServerAcceptorInterceptor> a = serverAcceptors;
    if (a.isEmpty()) {
      serverAcceptors = a = createInterceptorsList();
    }
    a.add(interceptor);
    return this;
  }

  DuplexConnection interceptConnection(DuplexConnection connection) {
    Interceptors starting = startingInterceptors;
    if (starting != null) {
      connection = starting.interceptConnection(connection);
    }
    return intercept(connection, connections);
  }

  RSocket interceptRequester(RSocket rSocket) {
    Interceptors starting = startingInterceptors;
    if (starting != null) {
      rSocket = starting.interceptRequester(rSocket);
    }
    return intercept(rSocket, requesters);
  }

  RSocket interceptHandler(RSocket rSocket) {
    Interceptors starting = startingInterceptors;
    if (starting != null) {
      rSocket = starting.interceptHandler(rSocket);
    }
    return intercept(rSocket, handlers);
  }

  ClientSocketAcceptor interceptClientAcceptor(ClientSocketAcceptor acceptor) {
    Interceptors starting = startingInterceptors;
    if (starting != null) {
      acceptor = starting.interceptClientAcceptor(acceptor);
    }
    return intercept(acceptor, clientAcceptors);
  }

  ServerSocketAcceptor interceptServerAcceptor(ServerSocketAcceptor acceptor) {
    Interceptors starting = this.startingInterceptors;
    if (starting != null) {
      acceptor = starting.interceptServerAcceptor(acceptor);
    }
    return intercept(acceptor, serverAcceptors);
  }

  private <T> T intercept(T target, List<? extends Function<T, T>> interceptors) {
    for (int i = 0; i < interceptors.size(); i++) {
      Function<T, T> interceptor = interceptors.get(i);
      target = interceptor.apply(target);
    }
    return target;
  }
}
