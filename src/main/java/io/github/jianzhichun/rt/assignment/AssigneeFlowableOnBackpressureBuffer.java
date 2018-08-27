package io.github.jianzhichun.rt.assignment;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.annotations.Nullable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.exceptions.MissingBackpressureException;
import io.reactivex.functions.Action;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.fuseable.HasUpstreamPublisher;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.subscriptions.BasicIntQueueSubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zao
 * @date 2018/08/15
 */
public class AssigneeFlowableOnBackpressureBuffer<ASSIGNEE, T extends Assignable<ASSIGNEE>>
    extends Flowable<T> implements HasUpstreamPublisher<T> {

    private final boolean delayError;
    private final Action onOverflow;
    private final AssignmentManager<ASSIGNEE, T> manager;

    protected final Flowable<T> source;

    public AssigneeFlowableOnBackpressureBuffer(
        Flowable<T> source, boolean delayError, Action onOverflow,
        AssignmentManager<ASSIGNEE, T> manager
    ) {

        this.source = ObjectHelper.requireNonNull(source, "source is null");

        this.delayError = delayError;
        this.onOverflow = onOverflow;
        this.manager = manager;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        source.subscribe(
            new BackpressureBufferSubscriber<>(s, delayError, onOverflow, manager)
        );
    }

    @Override
    public Publisher<T> source() {
        return source;
    }

    static class BackpressureBufferSubscriber<ASSIGNEE, T extends Assignable<ASSIGNEE>>
        extends BasicIntQueueSubscription<T> implements FlowableSubscriber<T> {

        final Subscriber<? super T> actual;
        final SimplePlainQueue<T> queue;
        final boolean delayError;
        final Action onOverflow;

        Subscription s;

        volatile boolean cancelled;

        volatile boolean done;
        Throwable error;

        final AtomicLong requested = new AtomicLong();

        boolean outputFused;

        BackpressureBufferSubscriber(
            Subscriber<? super T> actual, boolean delayError, Action onOverflow,
            AssignmentManager<ASSIGNEE, T> manager
        ) {
            this.actual = actual;
            this.onOverflow = onOverflow;
            this.delayError = delayError;
            this.queue = manager;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                actual.onSubscribe(this);
                s.request(Long.MAX_VALUE);
            }
        }

        @Override
        public void onNext(T t) {
            if (!queue.offer(t)) {
                s.cancel();
                MissingBackpressureException ex = new MissingBackpressureException(
                    "Buffer is full");
                try {
                    onOverflow.run();
                } catch (Throwable e) {
                    Exceptions.throwIfFatal(e);
                    ex.initCause(e);
                }
                onError(ex);
                return;
            }
            if (outputFused) {
                actual.onNext(null);
            } else {
                drain();
            }
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            if (outputFused) {
                actual.onError(t);
            } else {
                drain();
            }
        }

        @Override
        public void onComplete() {
            done = true;
            if (outputFused) {
                actual.onComplete();
            } else {
                drain();
            }
        }

        @Override
        public void request(long n) {
            if (!outputFused) {
                if (SubscriptionHelper.validate(n)) {
                    BackpressureHelper.add(requested, n);
                    drain();
                }
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                s.cancel();

                if (getAndIncrement() == 0) {
                    queue.clear();
                }
            }
        }

        void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                final SimplePlainQueue<T> q = queue;
                final Subscriber<? super T> a = actual;
                for (; ; ) {

                    if (checkTerminated(done, q.isEmpty(), a)) {
                        return;
                    }

                    long r = requested.get();

                    long e = 0L;

                    while (e != r) {
                        boolean d = done;
                        T v = q.poll();
                        boolean empty = v == null;

                        if (checkTerminated(d, empty, a)) {
                            return;
                        }

                        if (empty) {
                            break;
                        }

                        a.onNext(v);

                        e++;
                    }

                    if (e == r) {
                        boolean d = done;
                        boolean empty = q.isEmpty();

                        if (checkTerminated(d, empty, a)) {
                            return;
                        }
                    }

                    if (e != 0L) {
                        if (r != Long.MAX_VALUE) {
                            requested.addAndGet(-e);
                        }
                    }

                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }
        }

        boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a) {
            if (cancelled) {
                queue.clear();
                return true;
            }
            if (d) {
                if (delayError) {
                    if (empty) {
                        Throwable e = error;
                        if (e != null) {
                            a.onError(e);
                        } else {
                            a.onComplete();
                        }
                        return true;
                    }
                } else {
                    Throwable e = error;
                    if (e != null) {
                        queue.clear();
                        a.onError(e);
                        return true;
                    } else if (empty) {
                        a.onComplete();
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int requestFusion(int mode) {
            if ((mode & ASYNC) != 0) {
                outputFused = true;
                return ASYNC;
            }
            return NONE;
        }

        @Nullable
        @Override
        public T poll() {
            return queue.poll();
        }

        @Override
        public void clear() {
            queue.clear();
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }
    }
}
