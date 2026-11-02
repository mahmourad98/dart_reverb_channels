/// Native (`dart:io`) transport for `dart_reverb_channels`.
///
/// [IOWebSocketConnector] opens sockets through an `HttpClient` you provide, so
/// the socket shares your certificate trust (pinning, a restricted trust store)
/// and sends upgrade headers such as `Origin`. A pending upgrade is aborted by
/// force-closing that client.
library;

import 'dart:async';
import 'dart:io';

import 'package:web_socket_channel/io.dart';

import 'dart_reverb_channels.dart';

export 'dart_reverb_channels.dart';

/// Opens sockets with `dart:io`'s [WebSocket], over an [HttpClient] from
/// `httpClientFactory`.
///
/// Connect errors arrive unwrapped (a `SocketException`, a
/// `HandshakeException`, a `WebSocketException` carrying the refused upgrade's
/// status), so a host can map them by type. Only the open socket is wrapped in
/// an [IOWebSocketChannel].
final class IOWebSocketConnector implements ReverbConnector {
  /// A connector. `httpClientFactory` defaults to a plain [HttpClient];
  /// [pingInterval] turns on WebSocket-level ping frames, on top of the Pusher
  /// heartbeat, for proxies that drop idle sockets early.
  const IOWebSocketConnector({
    HttpClient Function()? httpClientFactory,
    this.pingInterval,
    this.compression = CompressionOptions.compressionDefault,
  }) : _httpClientFactory = httpClientFactory;

  final HttpClient Function()? _httpClientFactory;

  /// WebSocket-level ping interval, or null for none.
  final Duration? pingInterval;

  /// The per-message deflate settings.
  final CompressionOptions compression;

  @override
  Future<ReverbSocket> connect(ReverbConnectRequest request) async {
    final client = (_httpClientFactory ?? HttpClient.new)()..connectionTimeout = request.timeout;
    final abort = request.cancelToken.whenCancelled.then((_) => client.close(force: true));

    final WebSocket socket;
    try {
      socket = await WebSocket.connect(
        request.uri.toString(),
        headers: request.headers.isEmpty ? null : request.headers,
        compression: compression,
        customClient: client,
      );
    } finally {
      // The upgraded socket is detached from the client, so closing the
      // client only releases its idle connections.
      client.close();
      unawaited(abort);
    }

    if (request.cancelToken.isCancelled) {
      unawaited(socket.close(WebSocketStatus.goingAway).catchError((Object _) {}));
      throw request.cancelToken.reason ?? const ReverbCancelledException();
    }

    socket.pingInterval = pingInterval;
    return WebSocketChannelSocket(IOWebSocketChannel(socket));
  }
}

/// Whether [error]'s root cause is a TLS failure (a rejected certificate or
/// handshake). A host's `ReverbOptions.retryIf` usually refuses to retry these:
/// a certificate does not fix itself between attempts.
bool isTlsFailure(Object? error) {
  final root = error is ReverbException
      ? switch (error) {
          final ReverbConnectionException e => e.rootCause,
          final ReverbAuthorizationException e => e.rootCause,
          _ => null,
        }
      : unwrapReverbCause(error);
  return root is TlsException;
}
