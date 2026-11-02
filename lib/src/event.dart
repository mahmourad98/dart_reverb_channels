import 'exceptions.dart';

/// One event delivered on a channel.
final class ReverbEvent {
  /// An event.
  const ReverbEvent({
    required this.channel,
    required this.name,
    required this.data,
    this.rawData,
    this.userId,
  });

  /// The channel, with its prefix.
  final String channel;

  /// The wire event name, such as `order.shipped` (a
  /// `broadcastAs()` name) or `App\Events\OrderShipped`.
  final String name;

  /// The payload, decoded once. A JSON string holding an object is decoded;
  /// any other value arrives as `{'data': value}`.
  final Map<String, dynamic> data;

  /// The payload exactly as it arrived on the wire.
  final Object? rawData;

  /// The sender of a client event, when the server includes it.
  final String? userId;

  @override
  String toString() => 'ReverbEvent($channel, $name, $data)';
}

/// A member of a presence channel.
final class PresenceMember {
  /// A member.
  const PresenceMember({required this.id, this.info});

  /// The member's `user_id`.
  final String id;

  /// The member's `user_info`, if the server sent one.
  final Object? info;

  @override
  bool operator ==(Object other) => other is PresenceMember && other.id == id;

  @override
  int get hashCode => id.hashCode;

  @override
  String toString() => 'PresenceMember($id)';
}

/// Decrypts the payloads of `private-encrypted-` channels.
///
/// The library ships no cipher, so it adds no crypto dependency: implement this
/// with the scheme your server uses (Pusher's is NaCl secretbox with the
/// `shared_secret` from authorization). Throw to drop an event; the failure is
/// reported through `ReverbClient.onError` as a [ReverbDecryptionException].
abstract interface class ReverbChannelDecrypter {
  /// Decrypts [data] of [event] on [channel] with [sharedSecret].
  Object? decrypt({
    required String channel,
    required String event,
    required String sharedSecret,
    required Object? data,
  });
}

/// Where a failure the client handled was raised.
enum ReverbErrorStage {
  /// Opening a socket or waiting for its handshake.
  connect,

  /// An established socket dropped.
  connection,

  /// Authorizing or acknowledging a channel subscription.
  subscribe,

  /// A frame the client could not use (malformed, undecryptable).
  frame,

  /// A `pusher:error` on an established socket. Changes no state.
  protocol,
}

/// A failure the client handled without throwing, reported once through
/// `ReverbClient.onError`.
final class ReverbErrorReport {
  /// A report.
  const ReverbErrorReport({
    required this.error,
    required this.stage,
    this.stackTrace,
    this.channel,
    this.attempt,
    this.willRetry = false,
  });

  /// The failure.
  final ReverbException error;

  /// Where it was raised.
  final ReverbErrorStage stage;

  /// Where the failure was observed, when known.
  final StackTrace? stackTrace;

  /// The channel involved, if any.
  final String? channel;

  /// The 1-based attempt number within its cycle, if any.
  final int? attempt;

  /// Whether the client schedules another attempt because of it.
  final bool willRetry;

  @override
  String toString() =>
      'ReverbErrorReport(stage: ${stage.name}, channel: $channel, attempt: $attempt, willRetry: $willRetry, error: $error)';
}

/// The severity of a log record.
enum ReverbLogLevel {
  /// Lifecycle detail.
  debug,

  /// Notable lifecycle changes.
  info,

  /// Handled failures.
  warning,
}

/// One log line from the client. Hosts route it to their own logger.
final class ReverbLogRecord {
  /// A record.
  const ReverbLogRecord(this.level, this.message);

  /// The severity.
  final ReverbLogLevel level;

  /// The message.
  final String message;

  @override
  String toString() => '[${level.name}] $message';
}

/// The client's connection state.
enum ReverbConnectionState {
  /// No socket and no attempt in flight.
  disconnected,

  /// The first connection cycle is running.
  connecting,

  /// A socket is established.
  connected,

  /// A cycle is running after an established socket dropped.
  reconnecting,

  /// The last cycle ran out of attempts. The next subscribe starts a new one.
  unavailable,

  /// The server refused the connection with a fatal (4000–4099) code. The next
  /// subscribe starts a new cycle.
  failed,

  /// Closed by `ReverbClient.pause`, usually because the app went to the
  /// background. `ReverbClient.resume` reconnects.
  paused,
}

/// Where one subscription stands, from `ReverbSubscription.status`.
///
/// A subscription starts [pending] and becomes [subscribed]. When its socket
/// drops it moves to [resubscribing], or to [failed] if the attempts run out,
/// and back to [subscribed] once the channel is subscribed again; [cancelled]
/// ends it. Every [subscribed] that follows [resubscribing] or [failed] marks
/// a gap: events published meanwhile were not delivered.
enum ReverbSubscriptionStatus {
  /// Not subscribed yet: waiting for a socket, authorizing, waiting for the
  /// server's acknowledgement, or waiting between attempts.
  pending,

  /// Subscribed: events are delivered.
  subscribed,

  /// It was subscribed, and the channel is being subscribed again because the
  /// socket dropped or the client was paused.
  /// Nothing is delivered until it is [subscribed] again.
  resubscribing,

  /// Not subscribed, and no attempt is running: the attempts ran out or a
  /// failure was fatal (`ReverbSubscription.error` holds it). Not final: the
  /// subscription recovers when its channel is subscribed again, after a new
  /// connection cycle or a new subscriber of the same channel.
  failed,

  /// Ended by `ReverbSubscription.cancel` or `ReverbClient.closeAll`. Final.
  cancelled,
}
