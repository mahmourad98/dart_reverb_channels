/// Test doubles for code built on `dart_reverb_channels`: an in-memory connector
/// whose sockets a test drives from the server side, and a scriptable
/// authorizer. No network, no real time: everything runs on the test's clock.
library;

import 'dart:async';
import 'dart:convert';

import 'dart_reverb_channels.dart';

export 'dart_reverb_channels.dart';

/// The server end of one in-memory socket.
final class FakeReverbSocket implements ReverbSocket {
  /// A socket for [request].
  FakeReverbSocket(this.request);

  /// The request that opened this socket.
  final ReverbConnectRequest request;

  final StreamController<Object?> _inbound = StreamController<Object?>();

  /// Every text message the client sent, raw.
  final List<String> sentRaw = <String>[];

  /// Called with each decoded frame the client sends, after it is recorded.
  void Function(FakeReverbSocket socket, Map<String, dynamic> frame)? onClientFrame;

  int? _closeCode;
  String? _closeReason;

  /// Whether the client closed this socket.
  bool closedByClient = false;

  /// Whether the server side closed or failed this socket.
  bool closedByServer = false;

  /// The close code the client sent.
  int? clientCloseCode;

  /// Every frame the client sent, decoded.
  List<Map<String, dynamic>> get sent =>
      sentRaw.map((String raw) => (jsonDecode(raw) as Map).cast<String, dynamic>()).toList();

  /// The frames named [event] the client sent.
  List<Map<String, dynamic>> sentNamed(String event) =>
      sent.where((Map<String, dynamic> frame) => frame['event'] == event).toList();

  /// Whether either side closed this socket.
  bool get isClosed => closedByClient || closedByServer;

  @override
  Stream<Object?> get stream => _inbound.stream;

  @override
  void add(String message) {
    if (closedByClient) throw StateError('Socket is closed');
    sentRaw.add(message);
    final callback = onClientFrame;
    if (callback != null) callback(this, (jsonDecode(message) as Map).cast<String, dynamic>());
  }

  @override
  Future<void> close([int? code, String? reason]) async {
    if (closedByClient) return;
    closedByClient = true;
    clientCloseCode = code;
    if (!_inbound.isClosed) unawaited(_inbound.close());
  }

  @override
  int? get closeCode => _closeCode;

  @override
  String? get closeReason => _closeReason;

  /// Delivers [frame] as JSON. `data` maps are encoded to a JSON string, the
  /// way Pusher servers send them; pass [encodeData] false to keep an object.
  void emit(Map<String, Object?> frame, {bool encodeData = true}) {
    final data = frame['data'];
    final wire = encodeData && data is Map ? <String, Object?>{...frame, 'data': jsonEncode(data)} : frame;
    emitRaw(jsonEncode(wire));
  }

  /// Delivers [raw] as is.
  void emitRaw(Object? raw) {
    if (_inbound.isClosed) return;
    _inbound.add(raw);
  }

  /// Delivers a stream error.
  void emitError(Object error, [StackTrace? stackTrace]) {
    if (_inbound.isClosed) return;
    closedByServer = true;
    _inbound.addError(error, stackTrace);
  }

  /// Sends `pusher:connection_established`.
  void handshake({String socketId = '1.1', int activityTimeout = 30}) {
    emit(<String, Object?>{
      'event': PusherEvents.connectionEstablished,
      'data': <String, Object?>{'socket_id': socketId, 'activity_timeout': activityTimeout},
    });
  }

  /// Sends `pusher_internal:subscription_succeeded` for [channel].
  void acknowledge(String channel, {Map<String, Object?> data = const <String, Object?>{}}) {
    emit(<String, Object?>{'event': PusherEvents.subscriptionSucceeded, 'channel': channel, 'data': data});
  }

  /// Sends `pusher:subscription_error` for [channel].
  void refuse(String channel, {int status = 403, String type = 'AuthError', String error = 'Forbidden'}) {
    emit(<String, Object?>{
      'event': PusherEvents.subscriptionError,
      'channel': channel,
      'data': <String, Object?>{'type': type, 'error': error, 'status': status},
    });
  }

  /// Sends an application event.
  void event(String channel, String name, Object? data) {
    emit(<String, Object?>{'event': name, 'channel': channel, 'data': data});
  }

  /// Sends `pusher:error` with [code].
  void protocolError(int code, [String message = 'Error']) {
    emit(<String, Object?>{
      'event': PusherEvents.error,
      'data': <String, Object?>{'code': code, 'message': message},
    });
  }

  /// Closes the socket from the server, with an optional close [code].
  void serverClose([int? code, String? reason]) {
    if (_inbound.isClosed) return;
    closedByServer = true;
    _closeCode = code;
    _closeReason = reason;
    unawaited(_inbound.close());
  }
}

/// An in-memory connector. Every attempt is recorded; by default it opens a
/// [FakeReverbSocket] at once. [hold] keeps attempts pending until the test
/// completes them, [failNext] fails the next attempts, and the auto flags make
/// the socket behave like a well-mannered server.
final class FakeReverbConnector implements ReverbConnector {
  /// A connector.
  FakeReverbConnector({this.autoHandshake = false, this.autoAcknowledge = false, this.autoPong = false});

  /// Whether each new socket sends its handshake at once, with socket ids
  /// `1.1`, `2.2`, …
  bool autoHandshake;

  /// Whether each `pusher:subscribe` is acknowledged at once.
  bool autoAcknowledge;

  /// Whether each `pusher:ping` is answered with a `pusher:pong` at once.
  /// Off by default, so a test can drive the heartbeat itself.
  bool autoPong;

  /// Whether attempts stay pending until [completePending] or [failPending].
  bool hold = false;

  /// Every attempt, in order.
  final List<ReverbConnectRequest> requests = <ReverbConnectRequest>[];

  /// Every socket opened, in order.
  final List<FakeReverbSocket> sockets = <FakeReverbSocket>[];

  final List<Object> _failures = <Object>[];
  final List<(ReverbConnectRequest, Completer<ReverbSocket>)> _pending =
      <(ReverbConnectRequest, Completer<ReverbSocket>)>[];

  /// The most recent socket.
  FakeReverbSocket get socket => sockets.last;

  /// The sockets neither side has closed.
  List<FakeReverbSocket> get openSockets => sockets.where((FakeReverbSocket s) => !s.isClosed).toList();

  /// Attempts held by [hold] and not yet settled or cancelled.
  int get pendingCount => _pending.length;

  /// Fails the next [times] attempts with [error].
  void failNext(Object error, {int times = 1}) {
    for (var i = 0; i < times; i++) {
      _failures.add(error);
    }
  }

  /// Opens the oldest held attempt.
  FakeReverbSocket completePending() {
    final (request, completer) = _pending.removeAt(0);
    final socket = _open(request);
    completer.complete(socket);
    return socket;
  }

  /// Fails the oldest held attempt with [error].
  void failPending(Object error) {
    final (_, completer) = _pending.removeAt(0);
    completer.completeError(error);
  }

  @override
  Future<ReverbSocket> connect(ReverbConnectRequest request) {
    requests.add(request);
    if (_failures.isNotEmpty) return Future<ReverbSocket>.error(_failures.removeAt(0));
    if (!hold) return Future<ReverbSocket>.value(_open(request));

    final completer = Completer<ReverbSocket>();
    final slot = (request, completer);
    _pending.add(slot);
    unawaited(
      request.cancelToken.whenCancelled.then((Object reason) {
        if (_pending.remove(slot) && !completer.isCompleted) completer.completeError(StateError('aborted: $reason'));
      }),
    );
    return completer.future;
  }

  FakeReverbSocket _open(ReverbConnectRequest request) {
    final socket = FakeReverbSocket(request);
    sockets.add(socket);
    // The flags are read per frame, so a test may flip them after connecting.
    socket.onClientFrame = (FakeReverbSocket s, Map<String, dynamic> frame) {
      final event = frame['event'];
      if (autoPong && event == PusherEvents.ping) {
        scheduleMicrotask(() => s.emit(<String, Object?>{'event': PusherEvents.pong}));
      }
      if (autoAcknowledge && event == PusherEvents.subscribe) {
        final data = frame['data'];
        if (data is Map && data['channel'] is String) {
          scheduleMicrotask(() => s.acknowledge(data['channel'] as String));
        }
      }
    };
    if (autoHandshake) {
      final n = sockets.length;
      scheduleMicrotask(() => socket.handshake(socketId: '$n.$n'));
    }
    return socket;
  }
}

/// A scriptable authorizer. By default it signs at once with
/// `key:{channel}:{socketId}`; [hold] keeps requests pending until the test
/// settles them with [completePending] or [failPending].
///
/// Cancellation is recorded in [cancelled] but does not settle a held request,
/// so a test can still complete it and check that the client ignores the
/// stale result.
final class FakeChannelAuthorizer implements ChannelAuthorizer {
  /// An authorizer; [handler] replaces the default signature.
  FakeChannelAuthorizer({this.handler});

  /// Produces each authorization when set; may throw.
  FutureOr<ChannelAuthorization> Function(ChannelAuthorizationRequest request)? handler;

  /// Whether requests stay pending until [completePending] or [failPending].
  bool hold = false;

  /// Every request, in order.
  final List<ChannelAuthorizationRequest> requests = <ChannelAuthorizationRequest>[];

  /// Requests whose cancel token fired, in order.
  final List<ChannelAuthorizationRequest> cancelled = <ChannelAuthorizationRequest>[];

  final List<(ChannelAuthorizationRequest, Completer<ChannelAuthorization>)> _pending =
      <(ChannelAuthorizationRequest, Completer<ChannelAuthorization>)>[];

  /// Requests held by [hold] and not yet settled.
  int get pendingCount => _pending.length;

  /// The signature the default handler returns.
  static String signatureFor(ChannelAuthorizationRequest request) => 'key:${request.channel}:${request.socketId}';

  /// Settles the oldest held request, with [authorization] or the default.
  void completePending([ChannelAuthorization? authorization]) {
    final (request, completer) = _pending.removeAt(0);
    completer.complete(authorization ?? ChannelAuthorization(auth: signatureFor(request)));
  }

  /// Fails the oldest held request.
  void failPending(Object error) {
    final (_, completer) = _pending.removeAt(0);
    completer.completeError(error);
  }

  @override
  Future<ChannelAuthorization> authorize(ChannelAuthorizationRequest request) async {
    requests.add(request);
    unawaited(request.cancelToken.whenCancelled.then((_) => cancelled.add(request)));

    if (hold) {
      final completer = Completer<ChannelAuthorization>();
      _pending.add((request, completer));
      return completer.future;
    }

    final custom = handler;
    if (custom != null) return custom(request);
    return ChannelAuthorization(auth: signatureFor(request));
  }
}
