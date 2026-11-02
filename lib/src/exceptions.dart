import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';

/// Base type of every failure the client reports, through a subscription's
/// `ready` future or through `ReverbClient.onError`.
///
/// Sealed so a host can switch over it exhaustively, the way it maps the
/// failures into its own error model.
sealed class ReverbException implements Exception {
  const ReverbException(this.message);

  /// A human readable description, for logs.
  final String message;

  /// The default retry decision, used unless `ReverbOptions.retryIf` overrides
  /// it. Transport failures, timeouts and server-side (5xx, 429) failures are
  /// retried; rejections, malformed data and configuration problems are not.
  bool get isRetryable;

  @override
  String toString() => '$runtimeType: $message';
}

/// The socket could not be opened, or it dropped.
///
/// [cause] is the raw transport error (for example a `SocketException`, a
/// `WebSocketException` or a TLS failure), when there is one. A plain close
/// carries [closeCode] and [closeReason] instead.
final class ReverbConnectionException extends ReverbException {
  /// A connection failure described by [message].
  const ReverbConnectionException(
    super.message, {
    this.cause,
    this.stackTrace,
    this.closeCode,
    this.closeReason,
  });

  /// The raw error that ended the attempt, if any.
  final Object? cause;

  /// Where [cause] was raised, if known.
  final StackTrace? stackTrace;

  /// The WebSocket close code, when the socket closed rather than failed.
  final int? closeCode;

  /// The WebSocket close reason, when the server sent one.
  final String? closeReason;

  /// [cause] with every known wrapper removed. See [unwrapReverbCause].
  Object? get rootCause => unwrapReverbCause(cause);

  @override
  bool get isRetryable => true;
}

/// An attempt did not finish within its deadline: opening the socket and
/// completing the handshake, or authorizing and acknowledging a channel.
final class ReverbTimeoutException extends ReverbException {
  /// An attempt that ran past [timeout].
  const ReverbTimeoutException(super.message, {required this.timeout, this.channel});

  /// The deadline that elapsed.
  final Duration timeout;

  /// The channel whose subscription timed out, or null for the connection.
  final String? channel;

  @override
  bool get isRetryable => true;
}

/// A Pusher protocol error: a `pusher:error` frame or a 4000–4299 close code.
///
/// Per the Pusher protocol, 4000–4099 mean "do not reconnect", 4100–4199
/// "reconnect after backing off" and 4200–4299 "reconnect immediately".
final class ReverbProtocolException extends ReverbException {
  /// A protocol error with [code].
  const ReverbProtocolException(super.message, {required this.code});

  /// The protocol code, or null when the server sent none.
  final int? code;

  /// Whether [code] forbids reconnecting.
  bool get isFatal => code != null && code! >= 4000 && code! < 4100;

  /// Whether [code] asks the client to back off before reconnecting.
  bool get requiresBackoff => code != null && code! >= 4100 && code! < 4200;

  @override
  bool get isRetryable => !isFatal;
}

/// No `pusher:pong` arrived within `ReverbOptions.pongTimeout` of a ping, so
/// the socket is treated as dead.
final class ReverbHeartbeatException extends ReverbException {
  /// A missed pong after [timeout].
  const ReverbHeartbeatException(super.message, {required this.timeout});

  /// The pong deadline that elapsed.
  final Duration timeout;

  @override
  bool get isRetryable => true;
}

/// The channel authorizer failed.
///
/// [statusCode] is the HTTP status of a rejected request, null when no response
/// arrived (then [cause] holds the transport error). [malformedResponse] marks a
/// 200 response whose body carried no usable `auth` string.
final class ReverbAuthorizationException extends ReverbException {
  /// An authorization failure for [channel].
  const ReverbAuthorizationException(
    super.message, {
    required this.channel,
    this.statusCode,
    this.body,
    this.cause,
    this.stackTrace,
    this.malformedResponse = false,
  });

  /// The channel that failed to authorize.
  final String channel;

  /// The HTTP status code, or null when no response arrived.
  final int? statusCode;

  /// The response body, for diagnostics.
  final String? body;

  /// The transport error when no response arrived.
  final Object? cause;

  /// Where [cause] was raised, if known.
  final StackTrace? stackTrace;

  /// Whether a 200 response carried an unusable body.
  final bool malformedResponse;

  /// [cause] with every known wrapper removed. See [unwrapReverbCause].
  Object? get rootCause => unwrapReverbCause(cause);

  @override
  bool get isRetryable {
    if (malformedResponse) return false;
    final status = statusCode;
    if (status == null) return true;
    return status >= 500 || status == 429 || status == 408;
  }
}

/// The server refused a subscription with `pusher:subscription_error`.
final class ReverbSubscriptionException extends ReverbException {
  /// A refused subscription to [channel].
  const ReverbSubscriptionException(
    super.message, {
    required this.channel,
    this.status,
    this.type,
    this.data = const <String, dynamic>{},
  });

  /// The refused channel.
  final String channel;

  /// The HTTP-like status the server reported, if any.
  final int? status;

  /// The error type the server reported (for example `AuthError`), if any.
  final String? type;

  /// The decoded payload of the error frame.
  final Map<String, dynamic> data;

  @override
  bool get isRetryable {
    final code = status;
    if (code == null) return false;
    return code >= 500 || code == 429;
  }
}

/// An inbound frame or payload could not be decoded.
final class ReverbFormatException extends ReverbException {
  /// An undecodable frame or payload.
  const ReverbFormatException(super.message, {this.raw, this.cause, this.stackTrace});

  /// The raw frame, for diagnostics.
  final Object? raw;

  /// The decoding error, if any.
  final Object? cause;

  /// Where [cause] was raised, if known.
  final StackTrace? stackTrace;

  @override
  bool get isRetryable => false;
}

/// An event on an encrypted channel could not be decrypted.
final class ReverbDecryptionException extends ReverbException {
  /// An event on [channel] that failed to decrypt.
  const ReverbDecryptionException(
    super.message, {
    required this.channel,
    required this.event,
    this.cause,
    this.stackTrace,
  });

  /// The encrypted channel.
  final String channel;

  /// The event that failed to decrypt.
  final String event;

  /// The decrypter's error, if any.
  final Object? cause;

  /// Where [cause] was raised, if known.
  final StackTrace? stackTrace;

  @override
  bool get isRetryable => false;
}

/// The subscription, or the whole client, was cancelled before it was ready.
final class ReverbCancelledException extends ReverbException {
  /// A cancellation.
  const ReverbCancelledException([super.message = 'Cancelled']);

  @override
  bool get isRetryable => false;
}

/// The request cannot be served with this configuration: an invalid channel
/// name, a private channel without an authorizer, an encrypted channel without
/// a decrypter, or a disposed client.
final class ReverbConfigurationException extends ReverbException {
  /// A request this configuration cannot serve.
  const ReverbConfigurationException(super.message);

  @override
  bool get isRetryable => false;
}

/// Removes the wrappers the transport layers put around a root cause: a
/// [WebSocketChannelException]'s `inner` error, an [AsyncError], and a
/// [ReverbConnectionException] or [ReverbAuthorizationException] cause. The
/// result is what a host maps, so a TLS failure maps the same whether it
/// arrived raw or wrapped.
///
/// `http.ClientException` is not unwrapped: it carries no inner error.
Object? unwrapReverbCause(Object? error) {
  Object? current = error;
  for (var depth = 0; depth < 8; depth++) {
    final next = switch (current) {
      final WebSocketChannelException wrapper => wrapper.inner,
      final AsyncError wrapper => wrapper.error,
      final ReverbConnectionException wrapper => wrapper.cause,
      final ReverbAuthorizationException wrapper => wrapper.cause,
      _ => null,
    };
    if (next == null) return current;
    current = next;
  }
  return current;
}
