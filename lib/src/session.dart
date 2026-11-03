import 'dart:async';

import 'connector.dart';
import 'event.dart';
import 'exceptions.dart';
import 'protocol.dart';

/// One socket to a Pusher-protocol server: open, handshake, `socket_id`,
/// heartbeat, frame encoding and inbound routing. It never retries; the
/// client owns attempts, reconnection and channels.
final class ReverbSession {
  /// A session that opens [request] through [connector].
  ReverbSession({
    required ReverbConnector connector,
    required ReverbConnectRequest request,
    required Duration activityTimeout,
    required Duration pongTimeout,
    required void Function(PusherFrame frame) onFrame,
    required void Function(ReverbException error, StackTrace? stackTrace, ReverbErrorStage stage) onError,
    void Function(int ping)? onPong,
    void Function(ReverbLogLevel level, String message)? onLog,
  }) : _connector = connector,
       _request = request,
       _activityCap = activityTimeout,
       _pongTimeout = pongTimeout,
       _onFrame = onFrame,
       _onError = onError,
       _onPong = onPong,
       _onLog = onLog {
    // A handshake that fails while nobody awaits it yet (the deadline firing
    // during the upgrade) must not surface as an uncaught error; [open]'s
    // caller still receives it.
    _handshake.future.ignore();
  }

  final ReverbConnector _connector;
  final ReverbConnectRequest _request;
  final Duration _activityCap;
  final Duration _pongTimeout;
  final void Function(PusherFrame frame) _onFrame;
  final void Function(ReverbException error, StackTrace? stackTrace, ReverbErrorStage stage) _onError;
  final void Function(int ping)? _onPong;
  final void Function(ReverbLogLevel level, String message)? _onLog;

  final Completer<String> _handshake = Completer<String>();
  final Completer<ReverbException> _dropped = Completer<ReverbException>();

  ReverbSocket? _socket;
  StreamSubscription<Object?>? _subscription;
  Timer? _idleTimer;
  Timer? _pongTimer;
  Duration _activityTimeout = const Duration(seconds: 120);
  String? _socketId;
  bool _terminated = false;
  int _pingsSent = 0;
  int _pongsReceived = 0;

  /// The pings sent on this socket so far, by the keep-alive or [ping].
  int get pingsSent => _pingsSent;

  /// The server-assigned socket id, once established.
  String? get socketId => _socketId;

  /// Whether the handshake completed and the socket is still open.
  bool get isEstablished => _socketId != null && !_terminated;

  /// The activity timeout in use: the shorter of the server's and the client's.
  Duration get activityTimeout => _activityTimeout;

  /// Completes once an established socket ends, with the cause: a
  /// [ReverbConnectionException] for a plain close or a transport error, a
  /// [ReverbProtocolException] for a 4000–4299 close code, a
  /// [ReverbHeartbeatException] for a missed pong, or a
  /// [ReverbCancelledException] when this side closed it.
  Future<ReverbException> get dropped => _dropped.future;

  /// Opens the socket and completes with the socket id once the server's
  /// `pusher:connection_established` arrives.
  ///
  /// Throws a [ReverbException]. When the request's cancel token fires first,
  /// its reason is thrown if it is a [ReverbException] (the client's deadline
  /// passes a [ReverbTimeoutException]); otherwise [ReverbCancelledException].
  Future<String> open() async {
    final token = _request.cancelToken;
    unawaited(token.whenCancelled.then((Object reason) => _failHandshake(_cancellation(reason), null)));

    final ReverbSocket socket;
    try {
      socket = await _connector.connect(_request);
    } catch (error, stackTrace) {
      if (token.isCancelled) throw _cancellation(token.reason);
      if (error is ReverbException) rethrow;
      throw ReverbConnectionException('Socket could not be opened: $error', cause: error, stackTrace: stackTrace);
    }

    if (token.isCancelled || _terminated) {
      unawaited(_closeQuietly(socket));
      throw _cancellation(token.reason);
    }

    _socket = socket;
    _subscription = socket.stream.listen(
      _onMessage,
      onError: _onStreamError,
      onDone: _onStreamDone,
      cancelOnError: true,
    );

    return _handshake.future;
  }

  /// Sends a frame. A no-op unless the socket is established.
  void send(String event, {Object? data, String? channel}) {
    final socket = _socket;
    if (socket == null || !isEstablished) return;
    try {
      socket.add(encodePusherFrame(event, data: data, channel: channel));
    } catch (error, stackTrace) {
      _log(ReverbLogLevel.warning, 'send $event failed: $error');
      _terminate(
        ReverbConnectionException('Sending failed: $error', cause: error, stackTrace: stackTrace),
        stackTrace,
      );
    }
  }

  /// Sends `pusher:ping`. A no-op unless the socket is established.
  ///
  /// The server handles a socket's frames in order and answers every ping
  /// with one `pusher:pong`, so a pong follows every immediate reply to the
  /// frames sent before its ping. `onPong` reports each pong with the number
  /// of the ping it answers: the [pingsSent] count right after it.
  void ping() {
    if (isEstablished && _sendRaw(PusherEvents.ping, data: const <String, Object?>{})) _pingsSent++;
  }

  /// Closes the socket from this side. A pending [open] fails with
  /// [ReverbCancelledException]; [dropped] completes with one.
  Future<void> close() async {
    _terminate(const ReverbCancelledException('Session closed'), null);
  }

  ReverbException _cancellation(Object? reason) =>
      reason is ReverbException ? reason : const ReverbCancelledException('Connection attempt cancelled');

  void _onMessage(Object? raw) {
    if (_terminated) return;
    _onActivity();

    final PusherFrame frame;
    try {
      frame = PusherFrame.parse(raw);
    } on ReverbFormatException catch (error, stackTrace) {
      _onError(error, stackTrace, ReverbErrorStage.frame);
      return;
    }

    switch (frame.event) {
      case PusherEvents.connectionEstablished:
        _onEstablished(frame);
      case PusherEvents.ping:
        _sendRaw(PusherEvents.pong, data: const <String, Object?>{});
      case PusherEvents.pong:
        // Pongs answer pings in order; one with no ping waiting is ignored.
        if (_pongsReceived < _pingsSent) _onPong?.call(++_pongsReceived);
      case PusherEvents.error:
        _onProtocolError(frame);
      default:
        if (isEstablished) _onFrame(frame);
    }
  }

  void _onEstablished(PusherFrame frame) {
    if (_socketId != null) return;

    final data = frame.decodedData;
    final socketId = data['socket_id'];
    if (socketId is! String || socketId.isEmpty) {
      final error = ReverbFormatException('Handshake carries no socket_id', raw: frame.data);
      _onError(error, null, ReverbErrorStage.frame);
      _terminate(error, null);
      return;
    }

    final serverTimeout = readPusherInt(data['activity_timeout']);
    final server = serverTimeout != null && serverTimeout > 0 ? Duration(seconds: serverTimeout) : _activityCap;
    _activityTimeout = server < _activityCap ? server : _activityCap;
    _socketId = socketId;
    _onActivity();
    _log(ReverbLogLevel.info, 'connected as $socketId (activity timeout ${_activityTimeout.inSeconds}s)');
    if (!_handshake.isCompleted) _handshake.complete(socketId);
  }

  void _onProtocolError(PusherFrame frame) {
    final data = frame.decodedData;
    final code = readPusherInt(data['code']);
    final message = data['message']?.toString() ?? 'Protocol error';
    final error = ReverbProtocolException(message, code: code);

    if (!isEstablished) {
      // Reverb answers origin and connection-limit refusals with pusher:error
      // before the handshake and leaves the socket open.
      _terminate(error, null);
      return;
    }

    // On an established socket an error changes no state; a close frame, if
    // the server sends one, drives the reaction. Reverb reports a rejected
    // channel signature this way, without a channel.
    _onError(error, null, ReverbErrorStage.protocol);
  }

  void _onStreamError(Object error, StackTrace stackTrace) {
    _terminate(
      ReverbConnectionException('Socket failed: $error', cause: error, stackTrace: stackTrace),
      stackTrace,
    );
  }

  void _onStreamDone() {
    final code = _socket?.closeCode;
    final reason = _socket?.closeReason;
    if (code != null && code >= 4000 && code < 4300) {
      _terminate(ReverbProtocolException(reason?.isNotEmpty == true ? reason! : 'Closed with $code', code: code), null);
    } else {
      _terminate(
        ReverbConnectionException(
          'Socket closed${code == null ? '' : ' with $code'}',
          closeCode: code,
          closeReason: reason,
        ),
        null,
      );
    }
  }

  /// Any inbound frame proves the socket alive: it answers a pending ping and
  /// restarts the silence timer.
  void _onActivity() {
    _pongTimer?.cancel();
    _pongTimer = null;
    if (_socketId == null || _terminated) return;
    _idleTimer?.cancel();
    _idleTimer = Timer(_activityTimeout, _ping);
  }

  void _ping() {
    if (!isEstablished) return;
    _log(ReverbLogLevel.debug, 'no frame for ${_activityTimeout.inSeconds}s, pinging');
    ping();
    _pongTimer?.cancel();
    _pongTimer = Timer(_pongTimeout, () {
      _terminate(
        ReverbHeartbeatException('No pong within ${_pongTimeout.inSeconds}s', timeout: _pongTimeout),
        null,
      );
    });
  }

  /// Whether the frame went out.
  bool _sendRaw(String event, {Object? data}) {
    final socket = _socket;
    if (socket == null || _terminated) return false;
    try {
      socket.add(encodePusherFrame(event, data: data));
      return true;
    } catch (error) {
      _log(ReverbLogLevel.warning, 'send $event failed: $error');
      return false;
    }
  }

  void _failHandshake(ReverbException error, StackTrace? stackTrace) {
    if (_handshake.isCompleted) return;
    _handshake.completeError(error, stackTrace);
  }

  /// Ends the session once: fails a pending handshake, completes [dropped],
  /// stops the timers and closes the socket.
  void _terminate(ReverbException cause, StackTrace? stackTrace) {
    if (_terminated) return;
    _terminated = true;

    _idleTimer?.cancel();
    _pongTimer?.cancel();
    _idleTimer = null;
    _pongTimer = null;

    final wasEstablished = _socketId != null;
    _failHandshake(cause, stackTrace);
    if (wasEstablished) _log(ReverbLogLevel.info, 'socket ended: ${cause.message}');
    if (!_dropped.isCompleted) _dropped.complete(cause);

    unawaited(_subscription?.cancel());
    _subscription = null;

    final socket = _socket;
    if (socket != null) unawaited(_closeQuietly(socket));
  }

  Future<void> _closeQuietly(ReverbSocket socket) async {
    try {
      await socket.close(1000);
    } catch (_) {
      // Closing a socket that already failed has nothing left to report.
    }
  }

  void _log(ReverbLogLevel level, String message) => _onLog?.call(level, message);
}
