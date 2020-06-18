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

package com.jauntsdn.rsocket.test.util;

import com.jauntsdn.rsocket.DuplexConnection;
import io.netty.buffer.ByteBuf;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * An implementation of {@link DuplexConnection} that provides functionality to modify the behavior
 * dynamically.
 */
public class TestDuplexConnection implements DuplexConnection {

  private static final Logger logger = LoggerFactory.getLogger(TestDuplexConnection.class);

  private final LinkedBlockingQueue<ByteBuf> sent;
  private final DirectProcessor<ByteBuf> sentPublisher;
  private final FluxSink<ByteBuf> sendSink;
  private final DirectProcessor<ByteBuf> received;
  private final FluxSink<ByteBuf> receivedSink;
  private final MonoProcessor<Void> onClose;
  private final ConcurrentLinkedQueue<Subscriber<ByteBuf>> sendSubscribers;
  private volatile double availability = 1;
  private volatile int initialSendRequestN = Integer.MAX_VALUE;
  private final Scheduler scheduler;

  public TestDuplexConnection() {
    this(ConnectionScheduler.DEFAULT);
  }

  public TestDuplexConnection(Scheduler scheduler) {
    this.scheduler = scheduler;
    this.sent = new LinkedBlockingQueue<>();
    this.received = DirectProcessor.create();
    this.receivedSink = received.sink();
    this.sentPublisher = DirectProcessor.create();
    this.sendSink = sentPublisher.sink();
    this.sendSubscribers = new ConcurrentLinkedQueue<>();
    this.onClose = MonoProcessor.create();
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    if (availability <= 0) {
      return Mono.error(
          new IllegalStateException("RSocket not available. Availability: " + availability));
    }
    Subscriber<ByteBuf> subscriber = TestSubscriber.create(initialSendRequestN);
    Flux.from(frames)
        .doOnNext(
            frame -> {
              sent.offer(frame);
              sendSink.next(frame);
            })
        .doOnError(throwable -> logger.error("Error in send stream on test connection.", throwable))
        .subscribe(subscriber);
    sendSubscribers.add(subscriber);
    return Mono.empty();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return received.doFinally(signalType -> dispose());
  }

  @Override
  public double availability() {
    return availability;
  }

  @Override
  public Scheduler scheduler() {
    return scheduler;
  }

  @Override
  public void dispose() {
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  public ByteBuf awaitSend() throws InterruptedException {
    return sent.take();
  }

  public void setAvailability(double availability) {
    this.availability = availability;
  }

  public Collection<ByteBuf> getSent() {
    CountDownLatch latch = new CountDownLatch(1);
    scheduler.schedule(latch::countDown);
    try {
      latch.await();
      return sent;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Publisher<ByteBuf> getSentAsPublisher() {
    return sentPublisher;
  }

  public void addToReceivedBuffer(ByteBuf... received) {
    CountDownLatch latch = new CountDownLatch(1);
    scheduler()
        .schedule(
            () -> {
              for (ByteBuf frame : received) {
                this.receivedSink.next(frame);
              }
              latch.countDown();
            });
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void clearSendReceiveBuffers() {
    sent.clear();
    sendSubscribers.clear();
  }

  public void setInitialSendRequestN(int initialSendRequestN) {
    this.initialSendRequestN = initialSendRequestN;
  }

  public Collection<Subscriber<ByteBuf>> getSendSubscribers() {
    return sendSubscribers;
  }

  private static class ConnectionScheduler {
    static final Scheduler DEFAULT = Schedulers.immediate();
  }
}
