package com.jauntsdn.rsocket.test.util;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class TestClientTransport implements ClientTransport {
  private final TestDuplexConnection testDuplexConnection;

  public TestClientTransport() {
    this.testDuplexConnection = new TestDuplexConnection();
  }

  public TestClientTransport(Scheduler scheduler) {
    this.testDuplexConnection = new TestDuplexConnection(scheduler);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.just(testDuplexConnection);
  }

  public TestDuplexConnection testConnection() {
    return testDuplexConnection;
  }
}
