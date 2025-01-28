package com.example.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.resume.ResumableFramesStore;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;


/**
 * A Redis-based store that mimics the concurrency/state approach of InMemoryResumableFramesStore,
 * but persists frames in a Redis list. The concurrency flags and state machine are carried over
 * from InMemoryResumableFramesStore.
 * <p>
 * writes - n (where n is frequent, primary operation) reads - m (where m == KeepAliveFrequency)
 * skip - k -> 0 (where k is the rare operation which happens after disconnection
 */
public class RedisResumableFramesStore extends Flux<ByteBuf>
        implements ResumableFramesStore, Subscription {

    private FramesSubscriber framesSubscriber;
    private static final Logger log = LoggerFactory.getLogger(RedisResumableFramesStore.class);

    final Sinks.Empty<Void> disposed = Sinks.empty();
    final String side;
    final String session;
    // Key prefix in Redis
    private final String framesKey;        // e.g. "rsocket:resume:<side>:<session>:frames"
    private final String impliedKey;       // e.g. "rsocket:resume:<side>:<session>:impliedPos"
    private final String firstAvailKey;    // e.g. "rsocket:resume:<side>:<session>:firstPos"
    private final String remoteImpliedKey; // e.g. "rsocket:resume:<side>:<session>:remotePos"
    private final RedisTemplate<String, byte[]> redisTemplate;

    volatile long impliedPosition;
    static final AtomicLongFieldUpdater<RedisResumableFramesStore> IMPLIED_POSITION =
            AtomicLongFieldUpdater.newUpdater(RedisResumableFramesStore.class, "impliedPosition");

    volatile long firstAvailableFramePosition;
    static final AtomicLongFieldUpdater<RedisResumableFramesStore> FIRST_AVAILABLE_FRAME_POSITION =
            AtomicLongFieldUpdater.newUpdater(
                    RedisResumableFramesStore.class, "firstAvailableFramePosition");

    volatile long remoteImpliedPosition;
    static final AtomicLongFieldUpdater<RedisResumableFramesStore> REMOTE_IMPLIED_POSITION =
            AtomicLongFieldUpdater.newUpdater(RedisResumableFramesStore.class, "remoteImpliedPosition");

    Throwable terminal;

    CoreSubscriber<? super ByteBuf> actual;
    CoreSubscriber<? super ByteBuf> pendingActual;

    volatile long state;
    static final AtomicLongFieldUpdater<RedisResumableFramesStore> STATE =
            AtomicLongFieldUpdater.newUpdater(RedisResumableFramesStore.class, "state");

    /**
     * Flag which indicates that {@link RedisResumableFramesStore} is finalized and all related
     * stores are cleaned
     */
    static final long FINALIZED_FLAG =
            0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that {@link RedisResumableFramesStore} is terminated via the {@link
     * RedisResumableFramesStore#dispose()} method
     */
    static final long DISPOSED_FLAG =
            0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that {@link RedisResumableFramesStore} is terminated via the {@link
     * FramesSubscriber#onComplete()} or {@link FramesSubscriber#onError(Throwable)} ()} methods
     */
    static final long TERMINATED_FLAG =
            0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that {@link RedisResumableFramesStore} has active frames consumer
     */
    static final long CONNECTED_FLAG =
            0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that {@link RedisResumableFramesStore} has no active frames consumer
     * but there is a one pending
     */
    static final long PENDING_CONNECTION_FLAG =
            0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that there are some received implied position changes from the remote
     * party
     */
    static final long REMOTE_IMPLIED_POSITION_CHANGED_FLAG =
            0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that there are some frames stored in the {@link
     * io.rsocket.internal.UnboundedProcessor} which has to be cached and sent to the remote party
     */
    static final long HAS_FRAME_FLAG =
            0b0000_0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
    /**
     * Flag which indicates that {@link RedisResumableFramesStore#drain(long)} has an actor which
     * is currently progressing on the work. This flag should work as a guard to enter|exist into|from
     * the {@link RedisResumableFramesStore#drain(long)} method.
     */
    static final long MAX_WORK_IN_PROGRESS =
            0b0000_0000_0000_0000_0000_0000_0000_0000_1111_1111_1111_1111_1111_1111_1111_1111L;

    public RedisResumableFramesStore(String side, ByteBuf session, RedisTemplate<String, byte[]> redisTemplate) {
        this.side = side;
        this.session = session.toString(CharsetUtil.UTF_8);
        // Build the base keys for storing frames and positions
        this.framesKey = "rsocket:resume:%s:%s:frames".formatted(side, this.session);
        this.impliedKey = "rsocket:resume:%s:%s:impliedPos".formatted(side, this.session);
        this.firstAvailKey = "rsocket:resume:%s:%s:firstPos".formatted(side, this.session);
        this.remoteImpliedKey = "rsocket:resume:%s:%s:remotePos".formatted(side, this.session);
        this.redisTemplate = redisTemplate;
    }

    public Mono<Void> saveFrames(Flux<ByteBuf> frames) {
        return frames
                .transform(
                        Operators.<ByteBuf, Void>lift(
                                (__, actual) -> this.framesSubscriber = new FramesSubscriber(actual, this)))
                .then();
    }

    @Override
    public void releaseFrames(long remoteImpliedPos) {
        long lastReceivedRemoteImpliedPosition = this.getRemoteImpliedPosition();
        if (lastReceivedRemoteImpliedPosition > remoteImpliedPos) {
            throw new IllegalStateException(
                    "Given Remote Implied Position is behind the last received Remote Implied Position");
        }

        REMOTE_IMPLIED_POSITION.set(this, remoteImpliedPos);

        // Store new remoteImpliedPos in Redis so we can retrieve it in drain logic
        this.redisTemplate.opsForValue().set(this.remoteImpliedKey, longToBytes(remoteImpliedPos));

        final long previousState = markRemoteImpliedPositionChanged(this);
        if (isFinalized(previousState) || isWorkInProgress(previousState)) {
            return;
        }

        this.drain((previousState + 1) | REMOTE_IMPLIED_POSITION_CHANGED_FLAG);
    }

    void drain(long expectedState) {
        final Fuseable.QueueSubscription<ByteBuf> qs = this.framesSubscriber.qs;

        for (; ; ) {
            if (hasRemoteImpliedPositionChanged(expectedState)) {
                expectedState = this.handlePendingRemoteImpliedPositionChanges(expectedState);
            }

            if (hasPendingConnection(expectedState)) {
                expectedState = this.handlePendingConnection(expectedState);
            }

            if (isConnected(expectedState)) {
                if (isTerminated(expectedState)) {
                    this.handleTerminal(this.terminal);
                } else if (this.isDisposed()) {
                    this.handleTerminal(new CancellationException("Disposed"));
                } else if (hasFrames(expectedState)) {
                    this.handlePendingFrames(qs);
                }
            }

            if (isDisposed(expectedState) || isTerminated(expectedState)) {
                clearAndFinalize(this);
                return;
            }

            expectedState = markWorkDone(this, expectedState);
            if (isFinalized(expectedState)) {
                return;
            }

            if (!isWorkInProgress(expectedState)) {
                return;
            }
        }
    }

    long handlePendingRemoteImpliedPositionChanges(long expectedState) {
        final long remoteImpliedPosition = this.getRemoteImpliedPosition();
        final long firstAvailableFramePosition = this.getFirstAvailablePosition();
        final long toDropFromCache = Math.max(0, remoteImpliedPosition - firstAvailableFramePosition);

        if (toDropFromCache > 0) {
            final int droppedFromCache = this.dropFramesFromRedis(toDropFromCache);

            if (toDropFromCache > droppedFromCache) {
                this.terminal =
                        new IllegalStateException(
                                String.format(
                                        "Local and remote state disagreement: "
                                                + "need to remove additional %d bytes, but cache is empty",
                                        toDropFromCache));
                expectedState = markTerminated(this) | TERMINATED_FLAG;
            }

            if (toDropFromCache < droppedFromCache) {
                this.terminal =
                        new IllegalStateException(
                                "Local and remote state disagreement: local and remote frame sizes are not equal");
                expectedState = markTerminated(this) | TERMINATED_FLAG;
            }

            // increment firstAvailable
            long newVal = firstAvailableFramePosition + droppedFromCache;
            this.setFirstAvailablePosition(newVal);
            if (log.isDebugEnabled()) {
                log.debug(
                        "Side[{}]|Session[{}]. Removed frames from cache to position[{}].",
                        this.side,
                        this.session,
                        newVal);
            }
        }

        return expectedState;
    }

    void handlePendingFrames(Fuseable.QueueSubscription<ByteBuf> qs) {
        for (; ; ) {
            final ByteBuf frame = qs.poll();
            final boolean empty = frame == null;

            if (empty) {
                break;
            }

            this.handleFrame(frame);

            if (!isConnected(this.state)) {
                break;
            }
        }
    }

    long handlePendingConnection(long expectedState) {
        CoreSubscriber<? super ByteBuf> lastActual = null;
        for (; ; ) {
            final CoreSubscriber<? super ByteBuf> nextActual = this.pendingActual;

            if (nextActual != lastActual) {
                // pass any existing frames from Redis if needed
                List<byte[]> existing = this.redisTemplate.opsForList().range(this.framesKey, 0, -1);
                if (existing != null && !existing.isEmpty()) {
                    for (byte[] frameData : existing) {
                        ByteBuf buf = Unpooled.wrappedBuffer(frameData).retainedSlice();
                        nextActual.onNext(buf);
                    }
                }
            }

            expectedState = markConnected(this, expectedState);
            if (isConnected(expectedState)) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Side[{}]|Session[{}]. Connected at Position[{}] and ImpliedPosition[{}]",
                            this.side,
                            this.session,
                            this.getFirstAvailablePosition(), this.getImpliedPosition());
                }

                this.actual = nextActual;
                break;
            }

            if (!hasPendingConnection(expectedState)) {
                break;
            }

            lastActual = nextActual;
        }
        return expectedState;
    }

    /**
     * Removes frames from the LEFT of the Redis list, summing their sizes,
     * until we remove 'toRemoveBytes' or run out of frames.
     *
     * @return total bytes actually removed
     */
    int dropFramesFromRedis(long toRemoveBytes) {
        int removedBytes = 0;
        while (toRemoveBytes > removedBytes) {
            final byte[] popped = this.redisTemplate.opsForList().leftPop(this.framesKey);
            if (popped == null) {
                break; // no more frames in Redis
            }
            removedBytes += popped.length;
        }
        if (log.isDebugEnabled() && removedBytes > 0) {
            log.debug("Dropped {} bytes from Redis for session {}", removedBytes, this.session);
        }
        return removedBytes;
    }

    @Override
    public Flux<ByteBuf> resumeStream() {
        return this;
    }

    @Override
    public long framePosition() {
        return this.getFirstAvailablePosition();
    }

    @Override
    public long frameImpliedPosition() {
        return this.getImpliedPosition() & Long.MAX_VALUE;
    }

    @Override
    public boolean resumableFrameReceived(ByteBuf frame) {
        final int frameSize = frame.readableBytes();
        for (; ; ) {
            final long impliedPosition = this.getImpliedPosition();

            if (impliedPosition < 0) {
                return false;
            }

            var update = impliedPosition + frameSize;
            if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, update)) {
                this.redisTemplate.opsForValue().set(this.impliedKey, longToBytes(update));
                return true;
            }
        }
    }

    void pauseImplied() {
        for (; ; ) {
            final long impliedPosition = this.getImpliedPosition();

            var update = impliedPosition | Long.MIN_VALUE;
            if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, update)) {
                this.redisTemplate.opsForValue().set(this.impliedKey, longToBytes(update));
                log.debug(
                        "Side[{}]|Session[{}]. Paused at position[{}]", this.side, this.session, impliedPosition);
                return;
            }
        }
    }

    void resumeImplied() {
        for (; ; ) {
            final long impliedPosition = this.getImpliedPosition();

            final long restoredImpliedPosition = impliedPosition & Long.MAX_VALUE;
            if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, restoredImpliedPosition)) {
                this.redisTemplate.opsForValue().set(this.impliedKey, longToBytes(restoredImpliedPosition));
                log.debug(
                        "Side[{}]|Session[{}]. Resumed at position[{}]",
                        this.side,
                        this.session,
                        restoredImpliedPosition);
                return;
            }
        }
    }

    private long getFirstAvailablePosition() {
        if (this.firstAvailableFramePosition > 0) {
            return this.firstAvailableFramePosition;
        }
        byte[] val = this.redisTemplate.opsForValue().get(this.firstAvailKey);
        if (val == null) return 0L;
        return bytesToLong(val);
    }

    private long getImpliedPosition() {
        if (this.impliedPosition > 0) {
            return this.impliedPosition;
        }
        byte[] val = this.redisTemplate.opsForValue().get(this.impliedKey);
        if (val == null) return 0L;
        return bytesToLong(val);
    }

    private void setFirstAvailablePosition(long newVal) {
        FIRST_AVAILABLE_FRAME_POSITION.lazySet(this, newVal);
        this.redisTemplate.opsForValue().set(this.firstAvailKey, longToBytes(newVal));
    }

    private long getRemoteImpliedPosition() {
        if (this.remoteImpliedPosition > 0) {
            return this.remoteImpliedPosition;
        }
        byte[] val = this.redisTemplate.opsForValue().get(this.remoteImpliedKey);
        if (val == null) return 0L;
        return bytesToLong(val);
    }

    @Override
    public Mono<Void> onClose() {
        return this.disposed.asMono();
    }

    @Override
    public void dispose() {
        final long previousState = markDisposed(this);
        if (isFinalized(previousState)
                || isDisposed(previousState)
                || isWorkInProgress(previousState)) {
            return;
        }

        this.drain(previousState | DISPOSED_FLAG);
    }

    void clearCache() {
        this.redisTemplate.delete(this.framesKey); // Remove all frames from Redis
        this.redisTemplate.delete(this.impliedKey);
        this.redisTemplate.delete(this.firstAvailKey);
        this.redisTemplate.delete(this.remoteImpliedKey);
        this.remoteImpliedPosition = 0;
        this.impliedPosition = 0;
        this.firstAvailableFramePosition = 0;
    }

    @Override
    public boolean isDisposed() {
        return isDisposed(this.state);
    }

    void handleFrame(ByteBuf frame) {
        final boolean isResumable = this.isResumableFrame(frame);
        if (isResumable) {
            this.handleResumableFrame(frame);
            return;
        }

        this.handleConnectionFrame(frame);
    }

    void handleTerminal(@Nullable Throwable t) {
        if (t != null) {
            this.actual.onError(t);
        } else {
            this.actual.onComplete();
        }
    }

    void handleConnectionFrame(ByteBuf frame) {
        this.actual.onNext(frame);
    }

    void handleResumableFrame(ByteBuf frame) {
        final int incomingFrameSize = frame.readableBytes();

        // push frame bytes to the RIGHT
        byte[] bytes = new byte[incomingFrameSize];
        frame.getBytes(frame.readerIndex(), bytes);

        // store in Redis
        this.redisTemplate.opsForList().rightPush(this.framesKey, bytes);

        this.actual.onNext(frame.retainedSlice());
    }

    @Override
    public void request(long n) {
    }

    @Override
    public void cancel() {
        this.pauseImplied();
        markDisconnected(this);
        if (log.isDebugEnabled()) {
            log.debug(
                    "Side[{}]|Session[{}]. Disconnected at Position[{}] and ImpliedPosition[{}]",
                    this.side,
                    this.session,
                    this.getFirstAvailablePosition(),
                    this.frameImpliedPosition());
        }
    }

    @Override
    public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
        this.resumeImplied();
        actual.onSubscribe(this);
        this.pendingActual = actual;

        final long previousState = markPendingConnection(this);
        if (isDisposed(previousState)) {
            actual.onError(new CancellationException("Disposed"));
            return;
        }

        if (isTerminated(previousState)) {
            actual.onError(new CancellationException("Disposed"));
            return;
        }

        if (isWorkInProgress(previousState)) {
            return;
        }

        this.drain((previousState + 1) | PENDING_CONNECTION_FLAG);
    }

    static class FramesSubscriber
            implements CoreSubscriber<ByteBuf>, Fuseable.QueueSubscription<Void> {

        final CoreSubscriber<? super Void> actual;
        final RedisResumableFramesStore parent;

        Fuseable.QueueSubscription<ByteBuf> qs;

        boolean done;

        FramesSubscriber(CoreSubscriber<? super Void> actual, RedisResumableFramesStore parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSubscribe(Subscription s) {
            if (Operators.validate(this.qs, s)) {
                final Fuseable.QueueSubscription<ByteBuf> qs = (Fuseable.QueueSubscription<ByteBuf>) s;
                this.qs = qs;

                final int m = qs.requestFusion(Fuseable.ANY);

                if (m != Fuseable.ASYNC) {
                    s.cancel();
                    this.actual.onSubscribe(this);
                    this.actual.onError(new IllegalStateException("Source has to be ASYNC fuseable"));
                    return;
                }

                this.actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            final RedisResumableFramesStore parent = this.parent;
            long previousState = RedisResumableFramesStore.markFrameAdded(parent);

            if (isFinalized(previousState)) {
                this.qs.clear();
                return;
            }

            if (isWorkInProgress(previousState)
                    || (!isConnected(previousState) && !hasPendingConnection(previousState))) {
                return;
            }

            parent.drain(previousState + 1);
        }

        @Override
        public void onError(Throwable t) {
            if (this.done) {
                Operators.onErrorDropped(t, this.actual.currentContext());
                return;
            }

            final RedisResumableFramesStore parent = this.parent;

            parent.terminal = t;
            this.done = true;

            final long previousState = RedisResumableFramesStore.markTerminated(parent);
            if (isFinalized(previousState)) {
                Operators.onErrorDropped(t, this.actual.currentContext());
                return;
            }

            if (isWorkInProgress(previousState)) {
                return;
            }

            parent.drain(previousState | TERMINATED_FLAG);
        }

        @Override
        public void onComplete() {
            if (this.done) {
                return;
            }

            final RedisResumableFramesStore parent = this.parent;

            this.done = true;

            final long previousState = RedisResumableFramesStore.markTerminated(parent);
            if (isFinalized(previousState)) {
                return;
            }

            if (isWorkInProgress(previousState)) {
                return;
            }

            parent.drain(previousState | TERMINATED_FLAG);
        }

        @Override
        public void cancel() {
            if (this.done) {
                return;
            }

            this.done = true;

            final long previousState = RedisResumableFramesStore.markTerminated(this.parent);
            if (isFinalized(previousState)) {
                return;
            }

            if (isWorkInProgress(previousState)) {
                return;
            }

            this.parent.drain(previousState | TERMINATED_FLAG);
        }

        @Override
        public void request(long n) {
        }

        @Override
        public int requestFusion(int requestedMode) {
            return Fuseable.NONE;
        }

        @Override
        public Void poll() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public void clear() {
        }
    }

    static long markFrameAdded(RedisResumableFramesStore store) {
        for (; ; ) {
            final long state = store.state;

            if (isFinalized(state)) {
                return state;
            }

            long nextState = state;
            if (isConnected(state) || hasPendingConnection(state) || isWorkInProgress(state)) {
                nextState =
                        (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? nextState : nextState + 1;
            }

            if (STATE.compareAndSet(store, state, nextState | HAS_FRAME_FLAG)) {
                return state;
            }
        }
    }

    static long markPendingConnection(RedisResumableFramesStore store) {
        for (; ; ) {
            final long state = store.state;

            if (isFinalized(state) || isDisposed(state) || isTerminated(state)) {
                return state;
            }

            if (isConnected(state)) {
                return state;
            }

            final long nextState =
                    (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : state + 1;
            if (STATE.compareAndSet(store, state, nextState | PENDING_CONNECTION_FLAG)) {
                return state;
            }
        }
    }

    static long markRemoteImpliedPositionChanged(RedisResumableFramesStore store) {
        for (; ; ) {
            final long state = store.state;

            if (isFinalized(state)) {
                return state;
            }

            final long nextState =
                    (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : (state + 1);
            if (STATE.compareAndSet(store, state, nextState | REMOTE_IMPLIED_POSITION_CHANGED_FLAG)) {
                return state;
            }
        }
    }

    static long markDisconnected(RedisResumableFramesStore store) {
        for (; ; ) {
            final long state = store.state;

            if (isFinalized(state)) {
                return state;
            }

            if (STATE.compareAndSet(store, state, state & ~CONNECTED_FLAG & ~PENDING_CONNECTION_FLAG)) {
                return state;
            }
        }
    }

    static long markWorkDone(RedisResumableFramesStore store, long expectedState) {
        for (; ; ) {
            final long state = store.state;

            if (expectedState != state) {
                return state;
            }

            if (isFinalized(state)) {
                return state;
            }

            final long nextState = state & ~MAX_WORK_IN_PROGRESS & ~REMOTE_IMPLIED_POSITION_CHANGED_FLAG;
            if (STATE.compareAndSet(store, state, nextState)) {
                return nextState;
            }
        }
    }

    static long markConnected(RedisResumableFramesStore store, long expectedState) {
        for (; ; ) {
            final long state = store.state;

            if (state != expectedState) {
                return state;
            }

            if (isFinalized(state)) {
                return state;
            }

            final long nextState = state ^ PENDING_CONNECTION_FLAG | CONNECTED_FLAG;
            if (STATE.compareAndSet(store, state, nextState)) {
                return nextState;
            }
        }
    }

    static long markTerminated(RedisResumableFramesStore store) {
        for (; ; ) {
            final long state = store.state;

            if (isFinalized(state)) {
                return state;
            }

            final long nextState =
                    (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : (state + 1);
            if (STATE.compareAndSet(store, state, nextState | TERMINATED_FLAG)) {
                return state;
            }
        }
    }

    static long markDisposed(RedisResumableFramesStore store) {
        for (; ; ) {
            final long state = store.state;

            if (isFinalized(state)) {
                return state;
            }

            final long nextState =
                    (state & MAX_WORK_IN_PROGRESS) == MAX_WORK_IN_PROGRESS ? state : (state + 1);
            if (STATE.compareAndSet(store, state, nextState | DISPOSED_FLAG)) {
                return state;
            }
        }
    }

    static void clearAndFinalize(RedisResumableFramesStore store) {
        final Fuseable.QueueSubscription<ByteBuf> qs = store.framesSubscriber.qs;
        for (; ; ) {
            final long state = store.state;

            qs.clear();
            store.clearCache();

            if (isFinalized(state)) {
                return;
            }

            if (STATE.compareAndSet(store, state, state | FINALIZED_FLAG & ~MAX_WORK_IN_PROGRESS)) {
                store.disposed.tryEmitEmpty();
                store.framesSubscriber.onComplete();
                return;
            }
        }
    }

    static boolean isConnected(long state) {
        return (state & CONNECTED_FLAG) == CONNECTED_FLAG;
    }

    static boolean hasRemoteImpliedPositionChanged(long state) {
        return (state & REMOTE_IMPLIED_POSITION_CHANGED_FLAG) == REMOTE_IMPLIED_POSITION_CHANGED_FLAG;
    }

    static boolean hasPendingConnection(long state) {
        return (state & PENDING_CONNECTION_FLAG) == PENDING_CONNECTION_FLAG;
    }

    static boolean hasFrames(long state) {
        return (state & HAS_FRAME_FLAG) == HAS_FRAME_FLAG;
    }

    static boolean isTerminated(long state) {
        return (state & TERMINATED_FLAG) == TERMINATED_FLAG;
    }

    static boolean isDisposed(long state) {
        return (state & DISPOSED_FLAG) == DISPOSED_FLAG;
    }

    static boolean isFinalized(long state) {
        return (state & FINALIZED_FLAG) == FINALIZED_FLAG;
    }

    static boolean isWorkInProgress(long state) {
        return (state & MAX_WORK_IN_PROGRESS) > 0;
    }

    //-------------------------------------------------------------------------
    // Byte conversions for storing longs in Redis
    //-------------------------------------------------------------------------
    private static byte[] longToBytes(long value) {
        // 8 bytes
        byte[] arr = new byte[8];
        for (int i = 7; i >= 0; i--) {
            arr[i] = (byte) (value & 0xff);
            value >>= 8;
        }
        return arr;
    }

    private static long bytesToLong(byte[] arr) {
        if (arr.length < 8) return 0L;
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (arr[i] & 0xff);
        }
        return result;
    }

    /**
     * True if this frame is considered "resumable" by RSocket. We skip
     * SETUP, RESUME, KEEPALIVE, ERROR, etc.
     */
    private boolean isResumableFrame(ByteBuf frame) {
        var encodedType = FrameHeaderCodec.nativeFrameType(frame);
        switch (encodedType) {
            case FrameType.SETUP:
            case FrameType.RESUME:
            case FrameType.RESUME_OK:
            case FrameType.ERROR:
            case FrameType.KEEPALIVE:
            case FrameType.LEASE:
            case FrameType.METADATA_PUSH:
                // do NOT store
                return false;
            default:
                return true;
        }
    }
}
