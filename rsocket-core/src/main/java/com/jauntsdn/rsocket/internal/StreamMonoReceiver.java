/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 * Copyright (c) 2020-Present Maksym Ostroverkhov, All Rights Reserved.
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
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/** MonoProcessor derivative. Holds some stream state: streamId */
public final class StreamMonoReceiver extends Mono<Payload>
    implements Processor<Payload, Payload>,
        CoreSubscriber<Payload>,
        Disposable,
        Subscription,
        Scannable {

  /**
   * Create a {@link reactor.core.publisher.MonoProcessor} that will eagerly request 1 on {@link
   * #onSubscribe(Subscription)}, cache and emit the eventual result for 1 or N subscribers.
   *
   * @param <T> type of the expected value
   * @return A {@link reactor.core.publisher.MonoProcessor}.
   */
  public static <T> StreamMonoReceiver create() {
    return new StreamMonoReceiver(null);
  }

  volatile NextInner[] subscribers;

  static final AtomicReferenceFieldUpdater<StreamMonoReceiver, StreamMonoReceiver.NextInner[]>
      SUBSCRIBERS =
          AtomicReferenceFieldUpdater.newUpdater(
              StreamMonoReceiver.class, NextInner[].class, "subscribers");

  static final NextInner[] EMPTY = new NextInner[0];

  static final NextInner[] TERMINATED = new NextInner[0];

  static final NextInner[] EMPTY_WITH_SOURCE = new NextInner[0];

  CorePublisher<? extends Payload> source;

  Throwable error;
  Payload value;

  volatile Subscription subscription;
  static final AtomicReferenceFieldUpdater<StreamMonoReceiver, Subscription> UPSTREAM =
      AtomicReferenceFieldUpdater.newUpdater(
          StreamMonoReceiver.class, Subscription.class, "subscription");

  private volatile int streamId = -1;

  StreamMonoReceiver(@Nullable CorePublisher<? extends Payload> source) {
    this.source = source;
    SUBSCRIBERS.lazySet(this, source != null ? EMPTY_WITH_SOURCE : EMPTY);
  }

  public int streamId() {
    return streamId;
  }

  public int streamId(int streamId) {
    this.streamId = streamId;
    return streamId;
  }

  @Override
  public final void cancel() {
    if (isTerminated()) {
      return;
    }

    Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
    if (s == Operators.cancelledSubscription()) {
      return;
    }

    source = null;
    if (s != null) {
      s.cancel();
    }
  }

  @Override
  public void dispose() {
    Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
    if (s == Operators.cancelledSubscription()) {
      return;
    }

    source = null;
    if (s != null) {
      s.cancel();
    }

    NextInner[] a;
    if ((a = SUBSCRIBERS.getAndSet(this, TERMINATED)) != TERMINATED) {
      Exception e = new CancellationException("Disposed");
      error = e;
      value = null;

      for (NextInner as : a) {
        as.onError(e);
      }
    }
  }

  /**
   * Block the calling thread indefinitely, waiting for the completion of this {@code
   * MonoProcessor}. If the {@link reactor.core.publisher.MonoProcessor} is completed with an error
   * a RuntimeException that wraps the error is thrown.
   *
   * @return the value of this {@code MonoProcessor}
   */
  @Override
  @Nullable
  public Payload block() {
    return block(null);
  }

  /**
   * Block the calling thread for the specified time, waiting for the completion of this {@code
   * MonoProcessor}. If the {@link reactor.core.publisher.MonoProcessor} is completed with an error
   * a RuntimeException that wraps the error is thrown.
   *
   * @param timeout the timeout value as a {@link Duration}
   * @return the value of this {@code MonoProcessor} or {@code null} if the timeout is reached and
   *     the {@code MonoProcessor} has not completed
   */
  @Override
  @Nullable
  public Payload block(@Nullable Duration timeout) {
    try {
      if (!isPending()) {
        return peek();
      }

      connect();

      long delay;
      if (null == timeout) {
        delay = 0L;
      } else {
        delay = System.nanoTime() + timeout.toNanos();
      }
      for (; ; ) {
        NextInner[] inners = subscribers;
        if (inners == TERMINATED) {
          if (error != null) {
            RuntimeException re = Exceptions.propagate(error);
            re = Exceptions.addSuppressed(re, new Exception("Mono#block terminated with an error"));
            throw re;
          }
          if (value == null) {
            return null;
          }
          return value;
        }
        if (timeout != null && delay < System.nanoTime()) {
          cancel();
          throw new IllegalStateException("Timeout on Mono blocking read");
        }

        Thread.sleep(1);
      }

    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();

      throw new IllegalStateException("Thread Interruption on Mono blocking read");
    }
  }

  /**
   * Return the produced {@link Throwable} error if any or null
   *
   * @return the produced {@link Throwable} error if any or null
   */
  @Nullable
  public final Throwable getError() {
    return isTerminated() ? error : null;
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been interrupted via cancellation.
   *
   * @return {@code true} if this {@code MonoProcessor} is cancelled, {@code false} otherwise.
   */
  public boolean isCancelled() {
    return subscription == Operators.cancelledSubscription() && !isTerminated();
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been completed with an error.
   *
   * @return {@code true} if this {@code MonoProcessor} was completed with an error, {@code false}
   *     otherwise.
   */
  public final boolean isError() {
    return getError() != null;
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been successfully completed a value.
   *
   * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
   */
  public final boolean isSuccess() {
    return isTerminated() && error == null;
  }

  /**
   * Indicates whether this {@code MonoProcessor} has been terminated by the source producer with a
   * success or an error.
   *
   * @return {@code true} if this {@code MonoProcessor} is successful, {@code false} otherwise.
   */
  public final boolean isTerminated() {
    return subscribers == TERMINATED;
  }

  @Override
  public boolean isDisposed() {
    return isTerminated() || isCancelled();
  }

  @Override
  public final void onComplete() {
    onNext(null);
  }

  @Override
  public final void onError(Throwable cause) {
    Objects.requireNonNull(cause, "onError cannot be null");

    if (UPSTREAM.getAndSet(this, Operators.cancelledSubscription())
        == Operators.cancelledSubscription()) {
      Operators.onErrorDropped(cause, Context.empty());
      return;
    }

    error = cause;
    value = null;
    source = null;

    for (NextInner as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
      as.onError(cause);
    }
  }

  @Override
  public final void onNext(@Nullable Payload value) {
    Subscription s;
    if ((s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription()))
        == Operators.cancelledSubscription()) {
      if (value != null) {
        Operators.onNextDropped(value, Context.empty());
      }
      return;
    }

    this.value = value;
    Publisher<? extends Payload> parent = source;
    source = null;

    NextInner[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);

    if (value == null) {
      for (NextInner as : array) {
        as.onComplete();
      }
    } else {
      if (s != null && !(parent instanceof Mono)) {
        s.cancel();
      }

      for (NextInner as : array) {
        as.complete(value);
      }
    }
  }

  @Override
  public final void onSubscribe(Subscription subscription) {
    if (Operators.setOnce(UPSTREAM, this, subscription)) {
      subscription.request(Long.MAX_VALUE);
    }
  }

  @Override
  public Stream<? extends Scannable> inners() {
    return Stream.of(subscribers);
  }

  /**
   * Returns the value that completed this {@link reactor.core.publisher.MonoProcessor}. Returns
   * {@code null} if the {@link reactor.core.publisher.MonoProcessor} has not been completed. If the
   * {@link reactor.core.publisher.MonoProcessor} is completed with an error a RuntimeException that
   * wraps the error is thrown.
   *
   * @return the value that completed the {@link reactor.core.publisher.MonoProcessor}, or {@code
   *     null} if it has not been completed
   * @throws RuntimeException if the {@link reactor.core.publisher.MonoProcessor} was completed with
   *     an error
   */
  @Nullable
  public Payload peek() {
    if (!isTerminated()) {
      return null;
    }

    if (value != null) {
      return value;
    }

    if (error != null) {
      RuntimeException re = Exceptions.propagate(error);
      re = Exceptions.addSuppressed(re, new Exception("Mono#peek terminated with an error"));
      throw re;
    }

    return null;
  }

  @Override
  public final void request(long n) {
    Operators.validate(n);
  }

  @Override
  public void subscribe(final CoreSubscriber<? super Payload> actual) {
    NextInner as = new NextInner(actual, this);
    actual.onSubscribe(as);
    if (add(as)) {
      if (as.isCancelled()) {
        remove(as);
      }
    } else {
      Throwable ex = error;
      if (ex != null) {
        actual.onError(ex);
      } else {
        Payload v = value;
        if (v != null) {
          as.complete(v);
        } else {
          as.onComplete();
        }
      }
    }
  }

  void connect() {
    Publisher<? extends Payload> parent = source;
    if (parent != null && SUBSCRIBERS.compareAndSet(this, EMPTY_WITH_SOURCE, EMPTY)) {
      parent.subscribe(this);
    }
  }

  @Override
  public Context currentContext() {
    if (subscribers.length > 0) {
      return subscribers[0].actual.currentContext();
    }
    return Context.empty();
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    boolean t = isTerminated();

    if (key == Attr.TERMINATED) return t;
    if (key == Attr.PARENT) return subscription;
    if (key == Attr.ERROR) return error;
    if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
    if (key == Attr.CANCELLED) return isCancelled();
    return null;
  }

  final boolean isPending() {
    return !isTerminated();
  }

  /**
   * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
   *
   * @return the number of active {@link Subscriber} or {@literal -1} if untracked
   */
  public final long downstreamCount() {
    return subscribers.length;
  }

  /**
   * Return true if any {@link Subscriber} is actively subscribed
   *
   * @return true if any {@link Subscriber} is actively subscribed
   */
  public final boolean hasDownstreams() {
    return downstreamCount() != 0;
  }

  boolean add(NextInner ps) {
    for (; ; ) {
      NextInner[] a = subscribers;

      if (a == TERMINATED) {
        return false;
      }

      int n = a.length;
      NextInner[] b = new NextInner[n + 1];
      System.arraycopy(a, 0, b, 0, n);
      b[n] = ps;

      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        Publisher<? extends Payload> parent = source;
        if (parent != null && a == EMPTY_WITH_SOURCE) {
          parent.subscribe(this);
        }
        return true;
      }
    }
  }

  void remove(NextInner ps) {
    for (; ; ) {
      NextInner[] a = subscribers;
      int n = a.length;
      if (n == 0) {
        return;
      }

      int j = -1;
      for (int i = 0; i < n; i++) {
        if (a[i] == ps) {
          j = i;
          break;
        }
      }

      if (j < 0) {
        return;
      }

      NextInner[] b;

      if (n == 1) {
        b = EMPTY;
      } else {
        b = new NextInner[n - 1];
        System.arraycopy(a, 0, b, 0, j);
        System.arraycopy(a, j + 1, b, j, n - j - 1);
      }
      if (SUBSCRIBERS.compareAndSet(this, a, b)) {
        return;
      }
    }
  }

  static final class NextInner extends MonoSubscriber<Payload, Payload> {
    final StreamMonoReceiver parent;

    NextInner(CoreSubscriber<? super Payload> actual, StreamMonoReceiver parent) {
      super(actual);
      this.parent = parent;
    }

    @Override
    public void cancel() {
      if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
        parent.remove(this);
      }
    }

    @Override
    public void onComplete() {
      if (!isCancelled()) {
        actual.onComplete();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (isCancelled()) {
        Operators.onOperatorError(t, currentContext());
      } else {
        actual.onError(t);
      }
    }

    @Override
    public Object scanUnsafe(Attr key) {
      if (key == Attr.PARENT) {
        return parent;
      }
      return super.scanUnsafe(key);
    }
  }

  public static class MonoSubscriber<I, O>
      implements CoreSubscriber<I>,
          Scannable,
          Subscription,
          Fuseable, // for constants only
          Fuseable.QueueSubscription<O> {

    protected final CoreSubscriber<? super O> actual;

    protected O value;
    volatile int state; // see STATE field updater

    public MonoSubscriber(CoreSubscriber<? super O> actual) {
      this.actual = actual;
    }

    @Override
    public void cancel() {
      O v = value;
      value = null;
      STATE.set(this, CANCELLED);
      discard(v);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
      if (key == Attr.CANCELLED) return isCancelled();
      if (key == Attr.TERMINATED)
        return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE;
      if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
      if (key == Attr.ACTUAL) {
        return actual;
      }

      return null;
    }

    @Override
    public final void clear() {
      STATE.lazySet(this, FUSED_CONSUMED);
      this.value = null;
    }

    /**
     * Tries to emit the value and complete the underlying subscriber or stores the value away until
     * there is a request for it.
     *
     * <p>Make sure this method is called at most once
     *
     * @param v the value to emit
     */
    public final void complete(O v) {
      for (; ; ) {
        int state = this.state;
        if (state == FUSED_EMPTY) {
          setValue(v);
          // sync memory since setValue is non volatile
          if (STATE.compareAndSet(this, FUSED_EMPTY, FUSED_READY)) {
            Subscriber<? super O> a = actual;
            a.onNext(v);
            a.onComplete();
            return;
          }
          // refresh state if race occurred so we test if cancelled in the next comparison
          state = this.state;
        }

        // if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
        if ((state & ~HAS_REQUEST_NO_VALUE) != 0) {
          this.value = null;
          discard(v);
          return;
        }

        if (state == HAS_REQUEST_NO_VALUE
            && STATE.compareAndSet(this, HAS_REQUEST_NO_VALUE, HAS_REQUEST_HAS_VALUE)) {
          this.value = null;
          Subscriber<? super O> a = actual;
          a.onNext(v);
          a.onComplete();
          return;
        }
        setValue(v);
        if (state == NO_REQUEST_NO_VALUE
            && STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, NO_REQUEST_HAS_VALUE)) {
          return;
        }
      }
    }

    /**
     * Discard the given value, generally this.value field. Lets derived subscriber with further
     * knowledge about the possible types of the value discard such values in a specific way. Note
     * that fields should generally be nulled out along the discard call.
     *
     * @param v the value to discard
     */
    protected void discard(O v) {
      Operators.onDiscard(v, actual.currentContext());
    }

    /**
     * Returns true if this Subscription has been cancelled.
     *
     * @return true if this Subscription has been cancelled
     */
    public final boolean isCancelled() {
      return state == CANCELLED;
    }

    @Override
    public final boolean isEmpty() {
      return this.state != FUSED_READY;
    }

    @Override
    public void onComplete() {
      actual.onComplete();
    }

    @Override
    public void onError(Throwable t) {
      actual.onError(t);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onNext(I t) {
      setValue((O) t);
    }

    @Override
    public void onSubscribe(Subscription s) {
      // if upstream
    }

    @Override
    @Nullable
    public final O poll() {
      if (STATE.compareAndSet(this, FUSED_READY, FUSED_CONSUMED)) {
        O v = value;
        value = null;
        return v;
      }
      return null;
    }

    @Override
    public void request(long n) {
      if (Operators.validate(n)) {
        for (; ; ) {
          int s = state;
          // if the any bits 1-31 are set, we are either in fusion mode (FUSED_*)
          // or request has been called (HAS_REQUEST_*)
          if ((s & ~NO_REQUEST_HAS_VALUE) != 0) {
            return;
          }
          if (s == NO_REQUEST_HAS_VALUE
              && STATE.compareAndSet(this, NO_REQUEST_HAS_VALUE, HAS_REQUEST_HAS_VALUE)) {
            O v = value;
            if (v != null) {
              value = null;
              Subscriber<? super O> a = actual;
              a.onNext(v);
              a.onComplete();
            }
            return;
          }
          if (STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, HAS_REQUEST_NO_VALUE)) {
            return;
          }
        }
      }
    }

    @Override
    public int requestFusion(int mode) {
      if ((mode & ASYNC) != 0) {
        STATE.lazySet(this, FUSED_EMPTY);
        return ASYNC;
      }
      return NONE;
    }

    /**
     * Set the value internally, without impacting request tracking state.
     *
     * @param value the new value.
     * @see #complete(Object)
     */
    public void setValue(O value) {
      this.value = value;
    }

    @Override
    public int size() {
      return isEmpty() ? 0 : 1;
    }

    /** Indicates this Subscription has no value and not requested yet. */
    static final int NO_REQUEST_NO_VALUE = 0;
    /** Indicates this Subscription has a value but not requested yet. */
    static final int NO_REQUEST_HAS_VALUE = 1;
    /** Indicates this Subscription has been requested but there is no value yet. */
    static final int HAS_REQUEST_NO_VALUE = 2;
    /** Indicates this Subscription has both request and value. */
    static final int HAS_REQUEST_HAS_VALUE = 3;
    /** Indicates the Subscription has been cancelled. */
    static final int CANCELLED = 4;
    /** Indicates this Subscription is in fusion mode and is currently empty. */
    static final int FUSED_EMPTY = 8;
    /** Indicates this Subscription is in fusion mode and has a value. */
    static final int FUSED_READY = 16;
    /** Indicates this Subscription is in fusion mode and its value has been consumed. */
    static final int FUSED_CONSUMED = 32;

    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<MonoSubscriber> STATE =
        AtomicIntegerFieldUpdater.newUpdater(MonoSubscriber.class, "state");
  }
}
