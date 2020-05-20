package com.jauntsdn.rsocket.test.util;

import com.jauntsdn.rsocket.Closeable;
import com.jauntsdn.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;

public class TestServerTransport implements ServerTransport<Closeable> {
  private final MonoProcessor<TestDuplexConnection> conn = MonoProcessor.create();
  private final MonoProcessor<Void> onClose = MonoProcessor.create();

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int frameSizeLimit) {
    conn.flatMap(acceptor::apply).subscribe(ignored -> {}, err -> {}, () -> {});
    return Mono.just(
        new Closeable() {
          @Override
          public Mono<Void> onClose() {
            return onClose;
          }

          @Override
          public void dispose() {
            onClose.onComplete();
          }

          @Override
          public boolean isDisposed() {
            return onClose.isTerminated();
          }
        });
  }

  public TestDuplexConnection connect() {
    TestDuplexConnection c = new TestDuplexConnection();
    conn.onNext(c);
    return c;
  }

  public TestDuplexConnection connect(Scheduler scheduler) {
    TestDuplexConnection c = new TestDuplexConnection(scheduler);
    conn.onNext(c);
    return c;
  }
}
