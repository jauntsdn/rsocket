/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.rsocket.internal;

import com.jauntsdn.rsocket.Payload;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * UnicastProcessor derivative.
 *
 * <p>optionally validates there is no more onNext signals than requested; holds some stream state:
 * streamId, streamSender
 */
public final class StreamReceiver extends FluxProcessor<Payload, Payload>
    implements Fuseable.QueueSubscription<Payload>,
        Fuseable,
        CoreSubscriber<Payload>,
        Scannable,
        Subscription {

  public static StreamReceiver create(Disposable onReceiveOverflow) {
    return new StreamReceiver(Queues.<Payload>unbounded().get(), onReceiveOverflow);
  }

  public static StreamReceiver create() {
    return new StreamReceiver(Queues.<Payload>unbounded().get(), null);
  }

  final Disposable onReceiveOverflow;
  final Queue<Payload> queue;
  final Consumer<? super Payload> onOverflow;

  volatile Disposable onTerminate;
  static final AtomicReferenceFieldUpdater<StreamReceiver, Disposable> ON_TERMINATE =
      AtomicReferenceFieldUpdater.newUpdater(StreamReceiver.class, Disposable.class, "onTerminate");

  volatile boolean done;
  Throwable error;

  boolean
      hasDownstream; // important to not loose the downstream too early and miss discard hook, while
  // having relevant hasDownstreams()
  volatile CoreSubscriber<? super Payload> actual;

  volatile boolean cancelled;

  volatile int once;
  static final AtomicIntegerFieldUpdater<StreamReceiver> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(StreamReceiver.class, "once");

  volatile int wip;
  static final AtomicIntegerFieldUpdater<StreamReceiver> WIP =
      AtomicIntegerFieldUpdater.newUpdater(StreamReceiver.class, "wip");

  volatile int discardGuard;
  static final AtomicIntegerFieldUpdater<StreamReceiver> DISCARD_GUARD =
      AtomicIntegerFieldUpdater.newUpdater(StreamReceiver.class, "discardGuard");

  volatile long requested;
  static final AtomicLongFieldUpdater<StreamReceiver> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(StreamReceiver.class, "requested");

  boolean outputFused;

  /*stream state*/
  private volatile int streamId = -1;
  private volatile Subscription streamSender;

  public StreamReceiver(Queue<Payload> queue, @Nullable Disposable onReceiveOverflow) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.onReceiveOverflow = onReceiveOverflow;
    this.onTerminate = null;
    this.onOverflow = null;
  }

  public StreamReceiver(
      Queue<Payload> queue, Disposable onTerminate, @Nullable Disposable onReceiveOverflow) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
    this.onOverflow = null;
    this.onReceiveOverflow = onReceiveOverflow;
  }

  public StreamReceiver(
      Queue<Payload> queue,
      Consumer<? super Payload> onOverflow,
      Disposable onTerminate,
      @Nullable Disposable onReceiveOverflow) {
    this.queue = Objects.requireNonNull(queue, "queue");
    this.onOverflow = Objects.requireNonNull(onOverflow, "onOverflow");
    this.onTerminate = Objects.requireNonNull(onTerminate, "onTerminate");
    this.onReceiveOverflow = Objects.requireNonNull(onReceiveOverflow, "onReceiveOverflow");
  }

  @Override
  public int getBufferSize() {
    return Queues.capacity(this.queue);
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (Attr.BUFFERED == key) return queue.size();
    if (Attr.PREFETCH == key) return Integer.MAX_VALUE;
    return super.scanUnsafe(key);
  }

  public int streamId() {
    return streamId;
  }

  public int streamId(int streamId) {
    this.streamId = streamId;
    return streamId;
  }

  public Subscription streamSender() {
    return streamSender;
  }

  public void streamSender(Subscription streamSender) {
    this.streamSender = streamSender;
  }

  void doTerminate() {
    Disposable r = onTerminate;
    if (r != null && ON_TERMINATE.compareAndSet(this, r, null)) {
      r.dispose();
    }
  }

  void drainRegular(CoreSubscriber<? super Payload> a) {
    int missed = 1;

    final Queue<Payload> q = queue;

    for (; ; ) {

      long r = requested;
      long e = 0L;

      while (r != e) {
        boolean d = done;

        Payload t = q.poll();
        boolean empty = t == null;

        if (checkTerminated(d, empty, a, q, t)) {
          return;
        }

        if (empty) {
          break;
        }

        a.onNext(t);

        e++;
      }

      if (r == e) {
        boolean empty = q.isEmpty();
        if (checkTerminated(done, empty, a, q, null)) {
          return;
        }
        if (!empty) {
          Disposable receiveOverflow = onReceiveOverflow;
          if (receiveOverflow != null) {
            receiveOverflow.dispose();
          }
        }
      }

      if (e != 0 && r != Long.MAX_VALUE) {
        REQUESTED.addAndGet(this, -e);
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  void drainFused(CoreSubscriber<? super Payload> a) {
    int missed = 1;

    final Queue<Payload> q = queue;

    Disposable receiveOverflow = onReceiveOverflow;
    long r;
    if (receiveOverflow != null) {
      r = requested;
    } else {
      r = 0;
    }

    for (; ; ) {

      if (cancelled) {
        // We are the holder of the queue, but we still have to perform discarding under the guarded
        // block
        // to prevent any racing done by downstream
        this.clear();
        hasDownstream = false;
        return;
      }

      boolean d = done;
      if (receiveOverflow != null) {
        if (r == 0) {
          r = requested;
          if (r == 0) {
            receiveOverflow.dispose();
          }
        }
        r--;
      }
      a.onNext(null);

      if (d) {
        hasDownstream = false;

        Throwable ex = error;
        if (ex != null) {
          a.onError(ex);
        } else {
          a.onComplete();
        }
        return;
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  void drain(@Nullable Payload dataSignalOfferedBeforeDrain) {
    if (WIP.getAndIncrement(this) != 0) {
      if (dataSignalOfferedBeforeDrain != null && cancelled) {
        Operators.onDiscard(dataSignalOfferedBeforeDrain, actual.currentContext());
      }
      return;
    }

    int missed = 1;

    for (; ; ) {
      CoreSubscriber<? super Payload> a = actual;
      if (a != null) {

        if (outputFused) {
          drainFused(a);
        } else {
          drainRegular(a);
        }
        return;
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  boolean checkTerminated(
      boolean d,
      boolean empty,
      CoreSubscriber<? super Payload> a,
      Queue<Payload> q,
      @Nullable Payload t) {
    if (cancelled) {
      Operators.onDiscard(t, a.currentContext());
      Operators.onDiscardQueueWithClear(q, a.currentContext(), null);
      hasDownstream = false;
      return true;
    }
    if (d && empty) {
      Throwable e = error;
      hasDownstream = false;
      if (e != null) {
        a.onError(e);
      } else {
        a.onComplete();
      }
      return true;
    }

    return false;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (done || cancelled) {
      s.cancel();
    } else {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public int getPrefetch() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Context currentContext() {
    CoreSubscriber<? super Payload> actual = this.actual;
    return actual != null ? actual.currentContext() : Context.empty();
  }

  @Override
  public void onNext(Payload t) {
    if (done || cancelled) {
      Operators.onNextDropped(t, currentContext());
      return;
    }

    if (!queue.offer(t)) {
      Context ctx = actual.currentContext();
      Throwable ex = Operators.onOperatorError(null, Exceptions.failWithOverflow(), t, ctx);
      if (onOverflow != null) {
        try {
          onOverflow.accept(t);
        } catch (Throwable e) {
          Exceptions.throwIfFatal(e);
          ex.initCause(e);
        }
      }
      Operators.onDiscard(t, ctx);
      onError(ex);
      return;
    }
    drain(t);
  }

  @Override
  public void onError(Throwable t) {
    if (done || cancelled) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    error = t;
    done = true;

    doTerminate();

    drain(null);
  }

  @Override
  public void onComplete() {
    if (done || cancelled) {
      return;
    }

    done = true;

    doTerminate();

    drain(null);
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

      this.hasDownstream = true;
      actual.onSubscribe(this);
      this.actual = actual;
      if (cancelled) {
        this.hasDownstream = false;
      } else {
        drain(null);
      }
    } else {
      Operators.error(
          actual,
          new IllegalStateException("UnicastProcessor " + "allows only a single Subscriber"));
    }
  }

  @Override
  public void request(long n) {
    if (Operators.validate(n)) {
      Operators.addCap(REQUESTED, this, n);
      drain(null);
    }
  }

  @Override
  public void cancel() {
    if (cancelled) {
      return;
    }
    cancelled = true;

    doTerminate();

    if (WIP.getAndIncrement(this) == 0) {
      if (!outputFused) {
        // discard MUST be happening only and only if there is no racing on elements consumption
        // which is guaranteed by the WIP guard here in case non-fused output
        Operators.onDiscardQueueWithClear(queue, currentContext(), null);
      }
      hasDownstream = false;
    }
  }

  @Override
  @Nullable
  public Payload poll() {
    return queue.poll();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public void clear() {
    // use guard on the queue instance as the best way to ensure there is no racing on draining
    // the call to this method must be done only during the ASYNC fusion so all the callers will be
    // waiting
    // this should not be performance costly with the assumption the cancel is rare operation
    if (DISCARD_GUARD.getAndIncrement(this) != 0) {
      return;
    }

    int missed = 1;

    for (; ; ) {
      Operators.onDiscardQueueWithClear(queue, currentContext(), null);

      int dg = discardGuard;
      if (missed == dg) {
        missed = DISCARD_GUARD.addAndGet(this, -missed);
        if (missed == 0) {
          break;
        }
      } else {
        missed = dg;
      }
    }
  }

  @Override
  public int requestFusion(int requestedMode) {
    if ((requestedMode & Fuseable.ASYNC) != 0) {
      outputFused = true;
      return Fuseable.ASYNC;
    }
    return Fuseable.NONE;
  }

  @Override
  public boolean isDisposed() {
    return cancelled || done;
  }

  @Override
  public boolean isTerminated() {
    return done;
  }

  @Override
  @Nullable
  public Throwable getError() {
    return error;
  }

  @Override
  public long downstreamCount() {
    return hasDownstreams() ? 1L : 0L;
  }

  @Override
  public boolean hasDownstreams() {
    return hasDownstream;
  }
}
