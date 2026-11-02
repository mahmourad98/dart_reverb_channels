import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'cancel_token.dart';
import 'exceptions.dart';

/// One open socket, as the client sees it: text in, text out, and the close
/// code once it is done. Adapt any transport by implementing it.
abstract interface class ReverbSocket {
  /// Inbound messages. Single subscription; done when the socket closes.
  Stream<Object?> get stream;

  /// Sends a text message.
  void add(String message);

  /// Closes the socket from this side.
  Future<void> close([int? code, String? reason]);

  /// The close code, once [stream] is done.
  int? get closeCode;

  /// The close reason, once [stream] is done.
  String? get closeReason;
}

/// What a connector needs to open one socket.
final class ReverbConnectRequest {
  /// A request.
  const ReverbConnectRequest({
    required this.uri,
    required this.headers,
    required this.timeout,
    required this.cancelToken,
  });

  /// The socket URL.
  final Uri uri;

  /// The upgrade request headers (for example `Origin`).
  final Map<String, String> headers;

  /// The attempt's deadline. The client enforces it through [cancelToken];
  /// a connector may also hand it to its own transport.
  final Duration timeout;

  /// Cancelled when the attempt is abandoned (deadline, cancel, `closeAll`).
  /// A connector stops pending work on it.
  final ReverbCancelToken cancelToken;
}

/// Opens sockets: the transport seam, like Dio's `HttpClientAdapter`.
///
/// Errors may be thrown raw; the client wraps them into
/// [ReverbConnectionException] keeping the original as its cause.
abstract interface class ReverbConnector {
  /// Opens a socket for [request], completing once it is open.
  Future<ReverbSocket> connect(ReverbConnectRequest request);
}

/// Adapts a [WebSocketChannel] to [ReverbSocket].
final class WebSocketChannelSocket implements ReverbSocket {
  /// Wraps [channel].
  WebSocketChannelSocket(this.channel);

  /// The wrapped channel.
  final WebSocketChannel channel;

  @override
  Stream<Object?> get stream => channel.stream;

  @override
  void add(String message) => channel.sink.add(message);

  @override
  Future<void> close([int? code, String? reason]) async {
    await channel.sink.close(code, reason);
  }

  @override
  int? get closeCode => channel.closeCode;

  @override
  String? get closeReason => channel.closeReason;
}

/// Builds a channel for [uri] with the upgrade [headers]. Platforms that cannot
/// send upgrade headers (browsers) ignore them.
typedef WebSocketChannelFactory = WebSocketChannel Function(Uri uri, Map<String, String> headers);

/// A platform-neutral connector over `package:web_socket_channel`.
///
/// The default factory is [WebSocketChannel.connect], which cannot send upgrade
/// headers. Pass `factory` to build the channel yourself (an
/// `IOWebSocketChannel` with headers, a test double). On native platforms
/// prefer `IOWebSocketConnector` from `package:dart_reverb_channels/io.dart`, which
/// can also abort a pending upgrade.
final class WebSocketChannelConnector implements ReverbConnector {
  /// A connector that builds channels with [factory].
  const WebSocketChannelConnector({WebSocketChannelFactory? factory}) : _factory = factory;

  final WebSocketChannelFactory? _factory;

  @override
  Future<ReverbSocket> connect(ReverbConnectRequest request) async {
    final factory = _factory;
    final channel = factory != null ? factory(request.uri, request.headers) : WebSocketChannel.connect(request.uri);

    try {
      await Future.any<void>(<Future<void>>[
        channel.ready,
        request.cancelToken.whenCancelled.then<void>((Object reason) => throw reason),
      ]);
    } catch (_) {
      unawaited(channel.sink.close().catchError((Object _) {}));
      rethrow;
    }

    return WebSocketChannelSocket(channel);
  }
}
