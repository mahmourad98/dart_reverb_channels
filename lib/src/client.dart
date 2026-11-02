import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'authorizer.dart';
import 'cancel_token.dart';
import 'connector.dart';
import 'event.dart';
import 'exceptions.dart';
import 'options.dart';
import 'protocol.dart';
import 'session.dart';

part 'subscription.dart';

/// Receives every failure the client handled without throwing.
typedef ReverbErrorHandler = void Function(ReverbErrorReport report);

/// Receives the client's log lines.
typedef ReverbLogHandler = void Function(ReverbLogRecord record);

enum _EntryStatus {
  /// Waiting for an established socket.
  waiting,

  /// The authorizer is running.
  authorizing,

  /// `pusher:subscribe` was sent; waiting for the acknowledgement.
  acknowledging,

  /// Waiting out the back-off before the next attempt.
  backoff,

  /// Subscribed on the current socket.
  subscribed,

  /// The attempts ran out or a failure was fatal. A new subscriber or a new
  /// socket starts over.
  failed,
}

/// One attempt to subscribe a channel. Its continuations act only while it is
/// the entry's current attempt and its epoch is the client's.
final class _ChannelAttempt {
  _ChannelAttempt({required this.epoch, required this.number});

  final int epoch;
  final int number;
  final ReverbCancelToken token = ReverbCancelToken();
  Timer? deadline;

  void cancel(ReverbException reason) {
    deadline?.cancel();
    deadline = null;
    token.cancel(reason);
  }
}

/// A channel's shared state: its subscribers and its wire subscription.
///
/// A channel whose last subscriber leaves is removed; a later subscribe to the
/// same name creates a new entry with a new [generation], so continuations of
/// the old one find themselves stale by identity.
final class _ChannelEntry {
  _ChannelEntry({required this.name, required this.kind, required this.generation});

  final String name;
  final ReverbChannelKind kind;
  final int generation;
  final List<ReverbSubscription> subscriptions = <ReverbSubscription>[];

  _EntryStatus status = _EntryStatus.waiting;
  int attempts = 0;
  _ChannelAttempt? current;
  Timer? retryTimer;
  String? sharedSecret;
  String? channelData;
  String? myId;
  final Map<String, PresenceMember> members = <String, PresenceMember>{};

  /// Moves the channel to [next] and mirrors it on every owner. The only way
  /// [status] changes, so subscriptions never miss a transition.
  void setStatus(_EntryStatus next, [ReverbException? failure]) {
    status = next;
    for (final subscription in List<ReverbSubscription>.of(subscriptions)) {
      subscription._follow(next, failure);
    }
  }

  void stopAttempt(ReverbException reason) {
    current?.cancel(reason);
    current = null;
    retryTimer?.cancel();
    retryTimer = null;
  }

  void succeed() {
    for (final subscription in List<ReverbSubscription>.of(subscriptions)) {
      subscription._succeed();
    }
  }

  void fail(ReverbException error, [StackTrace? stackTrace]) {
    for (final subscription in List<ReverbSubscription>.of(subscriptions)) {
      subscription._fail(error, stackTrace);
    }
  }

  void deliver(ReverbEvent event) {
    for (final subscription in List<ReverbSubscription>.of(subscriptions)) {
      subscription._deliver(event);
    }
  }
}

/// A Pusher-protocol client for Laravel Reverb.
///
/// - **One shared socket.** [subscribe] returns a [ReverbSubscription];
///   subscriptions to one channel share one wire subscription, reference
///   counted. The first live subscription opens the socket; the last one
///   leaving closes it (see [ReverbOptions.closeWhenIdle]).
/// - **Bounded, generation-guarded retries.** A connection cycle gets
///   [ReverbOptions.connectRetry] attempts of [ReverbOptions.connectTimeout]
///   each; every channel gets [ReverbOptions.subscribeRetry] attempts of
///   [ReverbOptions.subscriptionTimeout], covering authorization and the
///   acknowledgement. Each continuation carries the epoch and the channel
///   entry it started for, so [closeAll], a cancel, or a new socket strands
///   it.
/// - **Reconnect and resubscribe.** After an established socket drops, a new
///   cycle runs and every live channel resubscribes with fresh authorization;
///   [reconnect] does the same on demand, failed channels included. Events
///   published while disconnected are not replayed: each subscription's
///   [ReverbSubscription.statusChanges] shows when its channel came back.
/// - **Pluggable edges.** The transport ([ReverbConnector]), authorization
///   ([ChannelAuthorizer]), decryption ([ReverbChannelDecrypter]), error
///   reporting and logging are all injected.
///
/// Create one per server and keep it; [subscribe] never throws.
final class ReverbClient {
  /// A client for [options].
  ///
  /// [connector] defaults to [WebSocketChannelConnector]; on native platforms
  /// pass `IOWebSocketConnector` from `package:dart_reverb_channels/io.dart` to send
  /// upgrade headers and use your own `HttpClient`. [authorizer] is required
  /// for private, encrypted and presence channels, [decrypter] for encrypted
  /// ones. [random] seeds the retry jitter.
  ReverbClient({
    required this.options,
    ReverbConnector? connector,
    ChannelAuthorizer? authorizer,
    ReverbChannelDecrypter? decrypter,
    ReverbErrorHandler? onError,
    ReverbLogHandler? onLog,
    math.Random? random,
  }) : _connector = connector ?? const WebSocketChannelConnector(),
       _authorizer = authorizer,
       _decrypter = decrypter,
       _onError = onError,
       _onLog = onLog,
       _random = random ?? math.Random() {
    options.validate();
  }

  /// The configuration.
  final ReverbOptions options;

  final ReverbConnector _connector;
  final ChannelAuthorizer? _authorizer;
  final ReverbChannelDecrypter? _decrypter;
  final ReverbErrorHandler? _onError;
  final ReverbLogHandler? _onLog;
  final math.Random _random;

  final StreamController<ReverbConnectionState> _states = StreamController<ReverbConnectionState>.broadcast();
  final Map<String, _ChannelEntry> _channels = <String, _ChannelEntry>{};
  final Map<String, int> _staleAcks = <String, int>{};
  final List<Timer> _staleAckTimers = <Timer>[];

  ReverbConnectionState _state = ReverbConnectionState.disconnected;
  int _epoch = 0;
  int _generations = 0;
  bool _disposed = false;
  bool _paused = false;

  ReverbSession? _session;
  bool _established = false;
  bool _cycleRunning = false;
  int _cycleAttempt = 0;
  Timer? _cycleTimer;
  Timer? _connectDeadline;
  ReverbCancelToken? _connectToken;
  Timer? _idleTimer;

  /// Set by [connect]: keeps the socket open with no channel until a
  /// subscription takes it over.
  bool _held = false;

  /// Callers of [connect] and [reconnect] waiting for the running cycle's
  /// outcome.
  final List<Completer<void>> _connectWaiters = <Completer<void>>[];

  final StreamController<ReverbErrorReport> _errors = StreamController<ReverbErrorReport>.broadcast();
  final StreamController<ReverbEvent> _events = StreamController<ReverbEvent>.broadcast();

  /// The connection state.
  ReverbConnectionState get state => _state;

  /// Connection state changes.
  Stream<ReverbConnectionState> get states => _states.stream;

  /// Every failure the client handled, as a broadcast stream: the same
  /// reports the `onError` handler receives, for any number of listeners.
  /// It does not replay earlier reports, and it is done after [dispose].
  Stream<ReverbErrorReport> get errors => _errors.stream;

  /// Every event delivered to a subscription of this client, from every
  /// channel and once per event, presence member events included, as a
  /// broadcast stream. It only observes: channels arrive here while some
  /// [ReverbSubscription] holds them, so it suits logging and diagnostics,
  /// not receiving channels nobody subscribed to. Done after [dispose].
  Stream<ReverbEvent> get events => _events.stream;

  /// The socket id of the established socket, or null.
  String? get socketId => _established ? _session?.socketId : null;

  /// The channels with at least one live subscription, as a snapshot.
  List<String> get channels => List<String>.unmodifiable(_channels.keys);

  /// Joins [channel]. Never throws.
  ///
  /// Problems that make the request impossible (an invalid name, a private
  /// channel without an authorizer, a disposed client) come back as a
  /// subscription whose [ReverbSubscription.ready] fails with
  /// [ReverbConfigurationException].
  ReverbSubscription subscribe(String channel) {
    final problem = _problemWith(channel);
    if (problem != null) {
      _log(ReverbLogLevel.warning, problem.message);
      return ReverbSubscription._failed(channel, problem);
    }

    _idleTimer?.cancel();
    _idleTimer = null;
    // A subscription owns the socket from now on; connect()'s hold ends.
    _held = false;

    final subscription = ReverbSubscription._(channel, this);
    final existing = _channels[channel];

    if (existing == null) {
      final entry = _ChannelEntry(name: channel, kind: ReverbChannelKind.of(channel), generation: ++_generations);
      _channels[channel] = entry;
      _join(entry, subscription);
      _log(ReverbLogLevel.debug, 'channel $channel (generation ${entry.generation}) opened');
      _activate(entry);
      return subscription;
    }

    _join(existing, subscription);
    switch (existing.status) {
      case _EntryStatus.subscribed:
        subscription._follow(_EntryStatus.subscribed);
        subscription._succeed();
      case _EntryStatus.failed:
        // A new owner asks again: the channel gets a fresh set of attempts.
        _activate(existing);
      case _EntryStatus.waiting || _EntryStatus.authorizing || _EntryStatus.acknowledging || _EntryStatus.backoff:
        // Shares the outcome of the attempt already running.
        break;
    }
    return subscription;
  }

  /// Ends every subscription and closes the socket. Pending `ready` futures
  /// fail with [ReverbCancelledException]; every event stream ends; a pending
  /// upgrade, authorization or back-off is abandoned, and a [connect] hold
  /// ends. The client stays usable: the next [subscribe] starts over. Call it
  /// on logout.
  Future<void> closeAll() async {
    final entries = _channels.values.toList();
    _channels.clear();
    _held = false;
    _closeSocket(const ReverbCancelledException('Client closed'));
    for (final entry in entries) {
      entry.stopAttempt(const ReverbCancelledException('Client closed'));
      for (final subscription in List<ReverbSubscription>.of(entry.subscriptions)) {
        subscription._terminate(const ReverbCancelledException('Client closed'));
        subscription._entry = null;
      }
      entry.subscriptions.clear();
    }
    _settleConnectWaiters(const ReverbCancelledException('Client closed'));
    if (!_paused) _setState(ReverbConnectionState.disconnected);
  }

  /// Closes the socket but keeps every subscription, for an app going to the
  /// background (iOS silently kills sockets an app is not watching). Pending
  /// `ready` futures keep waiting. [resume] reconnects.
  void pause() {
    if (_disposed || _paused) return;
    _paused = true;
    _closeSocket(const ReverbCancelledException('Client paused'));
    _settleConnectWaiters(const ReverbCancelledException('Client paused'));
    _setState(ReverbConnectionState.paused);
  }

  /// Reconnects after [pause] when channels are live, or a [connect] hold is.
  void resume() {
    if (_disposed || !_paused) return;
    _paused = false;
    if (_channels.isEmpty && !_held) {
      _setState(ReverbConnectionState.disconnected);
    } else {
      _startCycle(firstDelay: Duration.zero, reconnecting: true);
    }
  }

  /// Opens the socket before the first [subscribe], so that subscription
  /// only waits for its authorization and the server's acknowledgement.
  ///
  /// The socket stays open with no channel until a subscription takes it
  /// over (from then on [ReverbOptions.closeWhenIdle] applies as usual), or
  /// until [closeAll] or [dispose]. A drop reconnects it, and [pause] /
  /// [resume] keep the hold.
  ///
  /// Completes once a socket is established, at once if one already is, or
  /// with the [ReverbException] that ended the cycle, which also ends the
  /// hold ([ReverbCancelledException] when [closeAll] or [pause] came first).
  /// Awaiting it is optional: an unawaited failure is not reported as
  /// uncaught. While paused it fails with [ReverbCancelledException]; after
  /// [dispose], with [ReverbConfigurationException].
  Future<void> connect() {
    if (_disposed) {
      return _failedFuture(const ReverbConfigurationException('Client is disposed; cannot connect'));
    }
    if (_paused) {
      return _failedFuture(const ReverbCancelledException('Client is paused; resume() first'));
    }
    _held = true;
    // An idle socket waiting out its idle timeout is kept.
    _idleTimer?.cancel();
    _idleTimer = null;
    if (_established) return Future<void>.value();

    final connected = _waitForConnection();
    if (!_cycleRunning) _startCycle(firstDelay: Duration.zero, reconnecting: false);
    return connected;
  }

  /// Drops the socket and connects again at once, with a fresh set of
  /// [ReverbOptions.connectRetry] attempts. Every live channel resubscribes
  /// with fresh authorization, failed ones included: this is how
  /// subscriptions recover after [ReverbConnectionState.unavailable] or
  /// [ReverbConnectionState.failed] without subscribing again. Call it when
  /// connectivity returns, or from a retry button.
  ///
  /// Completes once a socket is established, or with the [ReverbException]
  /// that ended the new cycle ([ReverbCancelledException] when [closeAll],
  /// [pause] or the release of the last channel came first). Awaiting it is
  /// optional: an unawaited failure is not reported as uncaught.
  ///
  /// With no live channel, no [connect] hold and no open socket there is
  /// nothing to reconnect, and it completes at once. While paused it fails
  /// with [ReverbCancelledException] ([resume] reconnects); after [dispose],
  /// with [ReverbConfigurationException].
  Future<void> reconnect() {
    if (_disposed) {
      return _failedFuture(const ReverbConfigurationException('Client is disposed; cannot reconnect'));
    }
    if (_paused) {
      return _failedFuture(const ReverbCancelledException('Client is paused; resume() reconnects'));
    }
    if (_channels.isEmpty && !_held && _session == null && !_cycleRunning) return Future<void>.value();

    final connected = _waitForConnection();
    _log(ReverbLogLevel.info, 'reconnecting on request');
    _closeSocket(const ReverbCancelledException('Reconnecting'));
    _startCycle(firstDelay: Duration.zero, reconnecting: true);
    return connected;
  }

  /// [closeAll], then releases the [states], [errors] and [events] streams.
  /// The client is unusable after.
  Future<void> dispose() async {
    if (_disposed) return;
    // Refuse new subscriptions before anything yields.
    _disposed = true;
    await closeAll();
    await _states.close();
    await _errors.close();
    await _events.close();
  }

  // ---------------------------------------------------------------------------
  // Subscriptions
  // ---------------------------------------------------------------------------

  ReverbConfigurationException? _problemWith(String channel) {
    if (_disposed) return ReverbConfigurationException('Client is disposed; cannot subscribe to $channel');
    final invalid = validateChannelName(channel);
    if (invalid != null) return ReverbConfigurationException('$invalid: "$channel"');
    final kind = ReverbChannelKind.of(channel);
    if (kind.requiresAuthorization && _authorizer == null) {
      return ReverbConfigurationException('$channel needs a ChannelAuthorizer');
    }
    if (kind == ReverbChannelKind.encrypted && _decrypter == null) {
      return ReverbConfigurationException('$channel needs a ReverbChannelDecrypter');
    }
    return null;
  }

  void _join(_ChannelEntry entry, ReverbSubscription subscription) {
    subscription._entry = entry;
    entry.subscriptions.add(subscription);
  }

  void _activate(_ChannelEntry entry) {
    if (_paused) {
      entry.setStatus(_EntryStatus.waiting);
      return;
    }
    if (_established) {
      _startChannelAttempt(entry, fresh: true);
    } else {
      entry.setStatus(_EntryStatus.waiting);
      if (!_cycleRunning) _startCycle(firstDelay: Duration.zero, reconnecting: false);
    }
  }

  /// Called by [ReverbSubscription.cancel] after the subscription ended.
  void _detach(ReverbSubscription subscription) {
    final entry = subscription._entry;
    if (entry == null) return;
    subscription._entry = null;
    entry.subscriptions.remove(subscription);
    if (entry.subscriptions.isNotEmpty || !identical(_channels[entry.name], entry)) return;

    _removeChannel(entry);
    if (_channels.isEmpty) _onLastChannelGone();
  }

  void _removeChannel(_ChannelEntry entry) {
    _channels.remove(entry.name);
    final status = entry.status;
    entry.stopAttempt(const ReverbCancelledException('Channel released'));

    final session = _session;
    if (_established && session != null) {
      if (status == _EntryStatus.acknowledging) _addStaleAck(entry.name);
      if (status == _EntryStatus.acknowledging || status == _EntryStatus.subscribed || status == _EntryStatus.backoff) {
        session.send(PusherEvents.unsubscribe, data: <String, Object?>{'channel': entry.name});
      }
    }
    _log(ReverbLogLevel.debug, 'channel ${entry.name} (generation ${entry.generation}) released');
  }

  void _onLastChannelGone() {
    _idleTimer?.cancel();
    _idleTimer = null;
    // connect() keeps the socket for the subscription that will take it over.
    if (_held) return;

    // A socket that is still connecting has nothing left to serve: abandon it.
    if (!_established || (options.closeWhenIdle && options.idleTimeout == Duration.zero)) {
      _closeSocket(const ReverbCancelledException('No channels left'));
      _settleConnectWaiters(const ReverbCancelledException('No channels left'));
      return;
    }
    if (!options.closeWhenIdle) return;

    _idleTimer = Timer(options.idleTimeout, () {
      _idleTimer = null;
      if (_channels.isEmpty && !_held) _closeSocket(const ReverbCancelledException('No channels left'));
    });
  }

  bool _trigger(ReverbSubscription subscription, String event, Object? data) {
    final entry = subscription._entry;
    if (entry == null) return false;
    if (!entry.kind.acceptsClientEvents) {
      throw StateError('Client events need a private or presence channel, not ${entry.name}');
    }
    final session = _session;
    if (!_established || session == null || entry.status != _EntryStatus.subscribed) return false;
    final name = event.startsWith(PusherEvents.clientEventPrefix) ? event : '${PusherEvents.clientEventPrefix}$event';
    session.send(name, data: data, channel: entry.name);
    return true;
  }

  // ---------------------------------------------------------------------------
  // Connection cycle
  // ---------------------------------------------------------------------------

  void _startCycle({required Duration firstDelay, required bool reconnecting}) {
    if (_disposed || _paused) return;
    _epoch++;
    _cycleRunning = true;
    _cycleAttempt = 0;
    _setState(reconnecting ? ReverbConnectionState.reconnecting : ReverbConnectionState.connecting);

    final epoch = _epoch;
    if (firstDelay > Duration.zero) {
      _log(ReverbLogLevel.debug, 'connecting in ${firstDelay.inMilliseconds}ms');
      _cycleTimer = Timer(firstDelay, () => unawaited(_connectAttempt(epoch)));
    } else {
      unawaited(_connectAttempt(epoch));
    }
  }

  Future<void> _connectAttempt(int epoch) async {
    if (epoch != _epoch || _disposed) return;
    _cycleTimer = null;

    final attempt = ++_cycleAttempt;
    final timeout = options.connectTimeout;
    final token = ReverbCancelToken();
    final deadline = Timer(timeout, () {
      token.cancel(
        ReverbTimeoutException('Connection attempt timed out after ${timeout.inSeconds}s', timeout: timeout),
      );
    });
    _connectToken = token;
    _connectDeadline = deadline;
    final uri = options.socketUri;
    _log(ReverbLogLevel.debug, 'connection attempt $attempt to ${uri.scheme}://${uri.host}:${uri.port}${uri.path}');

    ReverbSession? session;
    try {
      final builder = options.headersBuilder;
      final extraHeaders = builder == null ? null : await builder();
      if (epoch != _epoch) return;
      if (token.isCancelled) throw token.reason!;

      final opened = ReverbSession(
        connector: _connector,
        request: ReverbConnectRequest(
          uri: options.socketUri,
          headers: <String, String>{...options.headers, ...?extraHeaders},
          timeout: timeout,
          cancelToken: token,
        ),
        activityTimeout: options.activityTimeout,
        pongTimeout: options.pongTimeout,
        onFrame: (PusherFrame frame) => _onFrame(session, frame),
        onError: (ReverbException error, StackTrace? stackTrace, ReverbErrorStage stage) {
          if (identical(_session, session)) {
            _report(ReverbErrorReport(error: error, stage: stage, stackTrace: stackTrace));
          }
        },
        onLog: _log,
      );
      session = opened;
      _session = opened;

      await opened.open();
      deadline.cancel();

      if (epoch != _epoch || !identical(_session, opened)) {
        unawaited(opened.close());
        return;
      }
      _onEstablished(opened);
    } catch (error, stackTrace) {
      deadline.cancel();
      if (epoch != _epoch) return;

      if (session != null) unawaited(session.close());
      if (identical(_session, session)) _session = null;
      _connectToken = null;
      _connectDeadline = null;

      final failure = _asConnectionFailure(error, stackTrace, token);
      final fatal = failure is ReverbProtocolException && failure.isFatal;
      final retry = !fatal && options.shouldRetry(failure) && options.connectRetry.allowsAttemptAfter(attempt);
      _report(
        ReverbErrorReport(
          error: failure,
          stage: ReverbErrorStage.connect,
          stackTrace: _stackOf(failure) ?? stackTrace,
          attempt: attempt,
          willRetry: retry,
        ),
      );
      // The handler may have closed or restarted the client.
      if (epoch != _epoch) return;

      if (retry) {
        var delay = options.connectRetry.delayAfter(attempt, _random);
        if (failure is ReverbProtocolException && failure.requiresBackoff && delay < options.reconnectBackoff) {
          delay = options.reconnectBackoff;
        }
        _cycleTimer = Timer(delay, () => unawaited(_connectAttempt(epoch)));
      } else {
        _onCycleExhausted(failure, stackTrace);
      }
    }
  }

  void _onEstablished(ReverbSession session) {
    _established = true;
    _cycleRunning = false;
    _cycleAttempt = 0;
    _connectToken = null;
    _connectDeadline = null;
    _clearStaleAcks();
    _setState(ReverbConnectionState.connected);
    _settleConnectWaiters();

    unawaited(session.dropped.then((ReverbException cause) => _onDropped(session, cause)));

    for (final entry in _channels.values.toList()) {
      if (entry.subscriptions.isNotEmpty) _startChannelAttempt(entry, fresh: true);
    }
    if (_channels.isEmpty) _onLastChannelGone();
  }

  void _onDropped(ReverbSession session, ReverbException cause) {
    if (!identical(session, _session) || !_established) return;
    _session = null;
    _established = false;
    _clearStaleAcks();
    _resetEntries(cause);

    if (_channels.isEmpty && !_held) {
      _setState(ReverbConnectionState.disconnected);
      return;
    }

    final fatal = cause is ReverbProtocolException && cause.isFatal;
    final reconnect = options.reconnect && !fatal && !_paused;
    final epoch = _epoch;
    _report(
      ReverbErrorReport(
        error: cause,
        stage: ReverbErrorStage.connection,
        stackTrace: _stackOf(cause),
        willRetry: reconnect,
      ),
    );
    // The handler may have closed the client or released every channel.
    if (epoch != _epoch || (_channels.isEmpty && !_held)) return;

    if (reconnect) {
      final backoff = cause is ReverbProtocolException && cause.requiresBackoff;
      _startCycle(firstDelay: backoff ? options.reconnectBackoff : Duration.zero, reconnecting: true);
    } else {
      _onCycleExhausted(cause, _stackOf(cause));
    }
  }

  /// The cycle ran out or hit a fatal failure: pending `ready` futures fail,
  /// every subscription becomes [ReverbSubscriptionStatus.failed], the socket
  /// stays idle and live subscriptions stay open and silent; a [connect] hold
  /// ends with the failure its caller receives. The next [subscribe], or
  /// [reconnect], starts a new cycle.
  void _onCycleExhausted(ReverbException failure, StackTrace? stackTrace) {
    _cycleRunning = false;
    _held = false;
    _cycleTimer?.cancel();
    _cycleTimer = null;
    final fatal = failure is ReverbProtocolException && failure.isFatal;
    _setState(fatal ? ReverbConnectionState.failed : ReverbConnectionState.unavailable);
    for (final entry in _channels.values.toList()) {
      entry.stopAttempt(failure);
      entry.setStatus(_EntryStatus.failed, failure);
      entry.fail(failure, stackTrace);
    }
    _settleConnectWaiters(failure, stackTrace);
  }

  /// Stops every connection-level task, closes the socket and advances the
  /// epoch so every pending continuation finds itself stale.
  void _closeSocket(ReverbException reason) {
    _epoch++;
    _idleTimer?.cancel();
    _idleTimer = null;
    _cycleTimer?.cancel();
    _cycleTimer = null;
    _connectDeadline?.cancel();
    _connectDeadline = null;
    _connectToken?.cancel(reason);
    _connectToken = null;
    _cycleRunning = false;
    _cycleAttempt = 0;

    final session = _session;
    _session = null;
    _established = false;
    _clearStaleAcks();
    if (session != null) unawaited(session.close());

    _resetEntries(reason);
    if (!_paused && _channels.isEmpty) _setState(ReverbConnectionState.disconnected);
  }

  /// Every remaining channel waits for the next socket; a failed one stays
  /// failed until that socket retries it.
  void _resetEntries(ReverbException reason) {
    for (final entry in _channels.values.toList()) {
      entry.stopAttempt(reason);
      entry.members.clear();
      entry.myId = null;
      if (entry.status != _EntryStatus.failed) entry.setStatus(_EntryStatus.waiting);
    }
  }

  ReverbException _asConnectionFailure(Object error, StackTrace stackTrace, ReverbCancelToken token) {
    if (token.isCancelled && token.reason is ReverbException) return token.reason! as ReverbException;
    if (error is ReverbException) return error;
    return ReverbConnectionException('Connection attempt failed: $error', cause: error, stackTrace: stackTrace);
  }

  /// A future for the running cycle's outcome, settled by
  /// [_settleConnectWaiters]. A replaced cycle hands its waiters to the next.
  Future<void> _waitForConnection() {
    final waiter = Completer<void>();
    // Optional reading, like `ready`: an unawaited failure is not uncaught.
    waiter.future.ignore();
    _connectWaiters.add(waiter);
    return waiter.future;
  }

  /// Completes every waiter: with nothing on success, with [failure] otherwise.
  void _settleConnectWaiters([ReverbException? failure, StackTrace? stackTrace]) {
    if (_connectWaiters.isEmpty) return;
    final waiters = List<Completer<void>>.of(_connectWaiters);
    _connectWaiters.clear();
    for (final waiter in waiters) {
      if (failure == null) {
        waiter.complete();
      } else {
        waiter.completeError(failure, stackTrace);
      }
    }
  }

  static Future<void> _failedFuture(ReverbException error) => Future<void>.error(error)..ignore();

  // ---------------------------------------------------------------------------
  // Channel attempts
  // ---------------------------------------------------------------------------

  void _startChannelAttempt(_ChannelEntry entry, {bool fresh = false}) {
    final session = _session;
    final socketId = session?.socketId;
    if (!_established || session == null || socketId == null) {
      entry.setStatus(_EntryStatus.waiting);
      return;
    }

    entry.stopAttempt(const ReverbCancelledException('Superseded'));
    if (fresh) entry.attempts = 0;
    final attempt = _ChannelAttempt(epoch: _epoch, number: ++entry.attempts);
    entry.current = attempt;

    final timeout = options.subscriptionTimeout;
    attempt.deadline = Timer(timeout, () {
      _onChannelAttemptFailed(
        entry,
        attempt,
        ReverbTimeoutException(
          'Subscribing to ${entry.name} timed out after ${timeout.inSeconds}s',
          timeout: timeout,
          channel: entry.name,
        ),
        null,
      );
    });

    if (!entry.kind.requiresAuthorization) {
      _sendSubscribe(entry, attempt, null);
      return;
    }

    entry.setStatus(_EntryStatus.authorizing);
    unawaited(_authorize(entry, attempt, socketId));
  }

  Future<void> _authorize(_ChannelEntry entry, _ChannelAttempt attempt, String socketId) async {
    try {
      final authorization = await _authorizer!.authorize(
        ChannelAuthorizationRequest(channel: entry.name, socketId: socketId, cancelToken: attempt.token),
      );
      if (!_isCurrent(entry, attempt)) return;

      if (entry.kind == ReverbChannelKind.encrypted && authorization.sharedSecret == null) {
        throw ReverbAuthorizationException(
          'Authorization for ${entry.name} has no shared_secret',
          channel: entry.name,
          statusCode: 200,
          malformedResponse: true,
        );
      }
      _sendSubscribe(entry, attempt, authorization);
    } catch (error, stackTrace) {
      // A stale attempt's result, success or failure, is dropped silently.
      if (!_isCurrent(entry, attempt)) return;
      final failure = error is ReverbException
          ? error
          : ReverbAuthorizationException(
              'Authorizing ${entry.name} failed: $error',
              channel: entry.name,
              cause: error,
              stackTrace: stackTrace,
            );
      _onChannelAttemptFailed(entry, attempt, failure, stackTrace);
    }
  }

  void _sendSubscribe(_ChannelEntry entry, _ChannelAttempt attempt, ChannelAuthorization? authorization) {
    entry.sharedSecret = authorization?.sharedSecret;
    entry.channelData = authorization?.channelData;
    entry.setStatus(_EntryStatus.acknowledging);
    _session!.send(
      PusherEvents.subscribe,
      data: <String, Object?>{
        'channel': entry.name,
        'auth': ?authorization?.auth,
        'channel_data': ?authorization?.channelData,
      },
    );
  }

  bool _isCurrent(_ChannelEntry entry, _ChannelAttempt attempt) {
    return !_disposed &&
        _established &&
        attempt.epoch == _epoch &&
        identical(_channels[entry.name], entry) &&
        identical(entry.current, attempt);
  }

  void _onChannelAttemptFailed(
    _ChannelEntry entry,
    _ChannelAttempt attempt,
    ReverbException failure,
    StackTrace? stackTrace,
  ) {
    if (!_isCurrent(entry, attempt)) return;
    entry.stopAttempt(failure);

    final retry = options.shouldRetry(failure) && options.subscribeRetry.allowsAttemptAfter(attempt.number);
    final epoch = _epoch;
    _report(
      ReverbErrorReport(
        error: failure,
        stage: ReverbErrorStage.subscribe,
        stackTrace: _stackOf(failure) ?? stackTrace,
        channel: entry.name,
        attempt: attempt.number,
        willRetry: retry,
      ),
    );
    // The handler may have closed the client or released the channel.
    if (epoch != _epoch || !identical(_channels[entry.name], entry)) return;

    if (!retry) {
      entry.setStatus(_EntryStatus.failed, failure);
      entry.fail(failure, stackTrace);
      return;
    }

    entry.setStatus(_EntryStatus.backoff);
    entry.retryTimer = Timer(options.subscribeRetry.delayAfter(attempt.number, _random), () {
      entry.retryTimer = null;
      if (epoch != _epoch || !identical(_channels[entry.name], entry) || entry.status != _EntryStatus.backoff) return;
      _startChannelAttempt(entry);
    });
  }

  // ---------------------------------------------------------------------------
  // Inbound frames
  // ---------------------------------------------------------------------------

  void _onFrame(ReverbSession? session, PusherFrame frame) {
    if (session == null || !identical(session, _session) || !_established) return;
    final name = frame.channel;
    if (name == null) return;

    switch (frame.event) {
      case PusherEvents.subscriptionSucceeded:
        _onSubscriptionSucceeded(name, frame);
      case PusherEvents.subscriptionError:
        _onSubscriptionError(name, frame);
      case PusherEvents.internalMemberAdded:
        _onMember(name, frame, added: true);
      case PusherEvents.internalMemberRemoved:
        _onMember(name, frame, added: false);
      default:
        if (frame.event.startsWith(PusherEvents.internalPrefix)) {
          // Protocol bookkeeping (for example subscription counts), not an
          // application event.
          _log(ReverbLogLevel.debug, 'ignored ${frame.event} on $name');
          return;
        }
        _onChannelEvent(name, frame);
    }
  }

  /// Delivers [event] to [entry]'s owners and to [events].
  void _deliver(_ChannelEntry entry, ReverbEvent event) {
    entry.deliver(event);
    if (!_events.isClosed) _events.add(event);
  }

  void _onSubscriptionSucceeded(String name, PusherFrame frame) {
    if (_consumeStaleAck(name)) return;
    final entry = _channels[name];
    if (entry == null) return;

    if (entry.status == _EntryStatus.failed) {
      // A late answer to an attempt the client already gave up on: the server
      // would now deliver to a subscription whose `ready` failed. Leave the
      // channel again, so failed subscriptions stay silent.
      _session?.send(PusherEvents.unsubscribe, data: <String, Object?>{'channel': name});
      return;
    }
    // A late answer to a timed-out attempt (backoff) is as good as the retry's.
    if (entry.status != _EntryStatus.acknowledging && entry.status != _EntryStatus.backoff) return;

    entry.stopAttempt(const ReverbCancelledException('Acknowledged'));
    entry.attempts = 0;
    if (entry.kind == ReverbChannelKind.presence) _seedPresence(entry, frame.decodedData);
    entry.setStatus(_EntryStatus.subscribed);
    _log(ReverbLogLevel.debug, 'subscribed to $name');
    entry.succeed();
  }

  void _onSubscriptionError(String name, PusherFrame frame) {
    if (_consumeStaleAck(name)) return;
    final entry = _channels[name];
    final attempt = entry?.current;
    if (entry == null || attempt == null || entry.status != _EntryStatus.acknowledging) return;

    final data = frame.decodedData;
    final status = readPusherInt(data['status']);
    final type = data['type']?.toString();
    final message = data['error']?.toString() ?? data['message']?.toString() ?? 'Subscription refused';
    _onChannelAttemptFailed(
      entry,
      attempt,
      ReverbSubscriptionException(message, channel: name, status: status, type: type, data: data),
      null,
    );
  }

  void _onChannelEvent(String name, PusherFrame frame) {
    final entry = _channels[name];
    if (entry == null || entry.subscriptions.isEmpty || entry.status != _EntryStatus.subscribed) return;

    Object? payload = frame.data;
    if (entry.kind == ReverbChannelKind.encrypted) {
      final secret = entry.sharedSecret;
      try {
        if (secret == null) throw StateError('No shared secret for $name');
        payload = _decrypter!.decrypt(channel: name, event: frame.event, sharedSecret: secret, data: frame.decodedData);
      } catch (error, stackTrace) {
        _report(
          ReverbErrorReport(
            error: ReverbDecryptionException(
              'Could not decrypt ${frame.event} on $name',
              channel: name,
              event: frame.event,
              cause: error,
              stackTrace: stackTrace,
            ),
            stage: ReverbErrorStage.frame,
            stackTrace: stackTrace,
            channel: name,
          ),
        );
        return;
      }
    }

    _deliver(
      entry,
      ReverbEvent(
        channel: name,
        name: frame.event,
        data: decodePusherData(payload),
        rawData: frame.data,
        userId: frame.userId,
      ),
    );
  }

  void _seedPresence(_ChannelEntry entry, Map<String, dynamic> data) {
    entry.members.clear();
    final presence = data['presence'];
    if (presence is Map) {
      final hash = presence['hash'];
      final ids = presence['ids'];
      if (hash is Map) {
        hash.forEach((Object? id, Object? info) {
          entry.members['$id'] = PresenceMember(id: '$id', info: info);
        });
      } else if (ids is List) {
        for (final Object? id in ids) {
          entry.members['$id'] = PresenceMember(id: '$id');
        }
      }
    }

    final channelData = entry.channelData;
    if (channelData != null) {
      try {
        final decoded = jsonDecode(channelData);
        if (decoded is Map && decoded['user_id'] != null) entry.myId = '${decoded['user_id']}';
      } on FormatException {
        entry.myId = null;
      }
    }
  }

  void _onMember(String name, PusherFrame frame, {required bool added}) {
    final entry = _channels[name];
    if (entry == null || entry.kind != ReverbChannelKind.presence || entry.status != _EntryStatus.subscribed) return;

    final data = frame.decodedData;
    final id = data['user_id']?.toString();
    if (id == null) return;
    if (added) {
      entry.members[id] = PresenceMember(id: id, info: data['user_info']);
    } else {
      entry.members.remove(id);
    }

    _deliver(
      entry,
      ReverbEvent(
        channel: name,
        name: added ? PusherEvents.memberAdded : PusherEvents.memberRemoved,
        data: data,
        rawData: frame.data,
      ),
    );
  }

  // ---------------------------------------------------------------------------
  // Stale acknowledgements
  // ---------------------------------------------------------------------------

  /// A channel released while its `pusher:subscribe` was unacknowledged leaves
  /// one slot: the answer to that frame is drained before a new entry of the
  /// same name can take an acknowledgement. Slots expire after the
  /// subscription timeout, so a frame the server never answers cannot starve
  /// the name.
  void _addStaleAck(String name) {
    _staleAcks[name] = (_staleAcks[name] ?? 0) + 1;
    late final Timer timer;
    timer = Timer(options.subscriptionTimeout, () {
      _staleAckTimers.remove(timer);
      final count = _staleAcks[name] ?? 0;
      if (count <= 1) {
        _staleAcks.remove(name);
      } else {
        _staleAcks[name] = count - 1;
      }
    });
    _staleAckTimers.add(timer);
  }

  bool _consumeStaleAck(String name) {
    final count = _staleAcks[name] ?? 0;
    if (count == 0) return false;
    if (count == 1) {
      _staleAcks.remove(name);
    } else {
      _staleAcks[name] = count - 1;
    }
    return true;
  }

  void _clearStaleAcks() {
    for (final timer in _staleAckTimers) {
      timer.cancel();
    }
    _staleAckTimers.clear();
    _staleAcks.clear();
  }

  // ---------------------------------------------------------------------------
  // Reporting
  // ---------------------------------------------------------------------------

  void _setState(ReverbConnectionState next) {
    if (_state == next) return;
    _state = next;
    _log(ReverbLogLevel.debug, 'state ${next.name}');
    if (!_states.isClosed) _states.add(next);
  }

  void _report(ReverbErrorReport report) {
    _log(ReverbLogLevel.warning, report.toString());
    final handler = _onError;
    if (handler != null) {
      try {
        handler(report);
      } catch (error) {
        _log(ReverbLogLevel.warning, 'onError handler threw: $error');
      }
    }
    if (!_errors.isClosed) _errors.add(report);
  }

  void _log(ReverbLogLevel level, String message) {
    final handler = _onLog;
    if (handler == null) return;
    try {
      handler(ReverbLogRecord(level, message));
    } catch (_) {
      // A logger must never break the client.
    }
  }

  StackTrace? _stackOf(ReverbException error) {
    return switch (error) {
      final ReverbConnectionException e => e.stackTrace,
      final ReverbAuthorizationException e => e.stackTrace,
      final ReverbFormatException e => e.stackTrace,
      final ReverbDecryptionException e => e.stackTrace,
      _ => null,
    };
  }
}
