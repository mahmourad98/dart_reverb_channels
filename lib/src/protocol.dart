import 'dart:convert';

import 'exceptions.dart';

/// Protocol event names the client handles itself.
abstract final class PusherEvents {
  /// Server → client: the handshake, carrying `socket_id` and `activity_timeout`.
  static const String connectionEstablished = 'pusher:connection_established';

  /// Either direction: a keep-alive probe.
  static const String ping = 'pusher:ping';

  /// Either direction: the answer to [ping].
  static const String pong = 'pusher:pong';

  /// Server → client: a protocol error, with `code` and `message`.
  static const String error = 'pusher:error';

  /// Client → server: join a channel.
  static const String subscribe = 'pusher:subscribe';

  /// Client → server: leave a channel.
  static const String unsubscribe = 'pusher:unsubscribe';

  /// Server → client: a channel subscription was refused.
  static const String subscriptionError = 'pusher:subscription_error';

  /// Server → client: a channel subscription was accepted.
  static const String subscriptionSucceeded = 'pusher_internal:subscription_succeeded';

  /// Server → client: a member joined a presence channel.
  static const String internalMemberAdded = 'pusher_internal:member_added';

  /// Server → client: a member left a presence channel.
  static const String internalMemberRemoved = 'pusher_internal:member_removed';

  /// Delivered to subscribers when a presence member joins.
  static const String memberAdded = 'pusher:member_added';

  /// Delivered to subscribers when a presence member leaves.
  static const String memberRemoved = 'pusher:member_removed';

  /// The prefix of client events (whispers).
  static const String clientEventPrefix = 'client-';

  /// The prefix of protocol bookkeeping frames, never delivered to subscribers.
  static const String internalPrefix = 'pusher_internal:';
}

/// The kind of a channel, from its name prefix.
enum ReverbChannelKind {
  /// No prefix: anyone may subscribe.
  public,

  /// `private-`: authorized per socket.
  private,

  /// `private-encrypted-`: authorized, with payloads decrypted by a shared secret.
  encrypted,

  /// `presence-`: authorized, with a member roster.
  presence;

  /// The kind of the channel called [name].
  static ReverbChannelKind of(String name) {
    if (name.startsWith('private-encrypted-')) return ReverbChannelKind.encrypted;
    if (name.startsWith('private-')) return ReverbChannelKind.private;
    if (name.startsWith('presence-')) return ReverbChannelKind.presence;
    return ReverbChannelKind.public;
  }

  /// Whether subscribing needs an authorizer.
  bool get requiresAuthorization => this != ReverbChannelKind.public;

  /// Whether the channel accepts client events.
  bool get acceptsClientEvents => this == ReverbChannelKind.private || this == ReverbChannelKind.presence;
}

final RegExp _invalidChannelCharacters = RegExp(r'[^a-zA-Z0-9_\-=@,.;]');

/// Why [name] is not a valid channel name, or null when it is.
String? validateChannelName(String name) {
  if (name.isEmpty) return 'Channel name must not be empty';
  if (name.length > 200) return 'Channel name must not exceed 200 characters';
  if (_invalidChannelCharacters.hasMatch(name)) {
    return 'Channel name may only contain letters, digits and _-=@,.;';
  }
  return null;
}

/// One decoded text frame.
final class PusherFrame {
  /// A frame.
  const PusherFrame({required this.event, this.channel, this.data, this.userId});

  /// Parses a raw text frame. Throws [ReverbFormatException] when [raw] is not
  /// a JSON object with a string `event`.
  factory PusherFrame.parse(Object? raw) {
    if (raw is! String) {
      throw ReverbFormatException('Frame is not text', raw: raw);
    }
    final Object? decoded;
    try {
      decoded = jsonDecode(raw);
    } on FormatException catch (error, stackTrace) {
      throw ReverbFormatException('Frame is not JSON', raw: raw, cause: error, stackTrace: stackTrace);
    }
    if (decoded is! Map) {
      throw ReverbFormatException('Frame is not a JSON object', raw: raw);
    }
    final event = decoded['event'];
    if (event is! String || event.isEmpty) {
      throw ReverbFormatException('Frame has no event name', raw: raw);
    }
    final channel = decoded['channel'];
    final userId = decoded['user_id'];
    return PusherFrame(
      event: event,
      channel: channel is String ? channel : null,
      data: decoded['data'],
      userId: userId?.toString(),
    );
  }

  /// The wire event name.
  final String event;

  /// The channel, for channel frames.
  final String? channel;

  /// The raw `data` member: usually a JSON string, sometimes an object, and
  /// absent from Reverb's data-less heartbeat frames.
  final Object? data;

  /// The sender of a client event, when the server includes it.
  final String? userId;

  /// [data] decoded into a map. See [decodePusherData].
  Map<String, dynamic> get decodedData => decodePusherData(data);
}

/// Decodes Pusher's `data` member, which arrives as a JSON string holding JSON
/// rather than as a nested object.
///
/// Null becomes an empty map, so data-less frames such as Reverb's
/// `{"event":"pusher:pong"}` read as `{}`. Maps pass through. A string holding
/// a JSON object is decoded once. Anything else is wrapped as `{'data': value}`
/// so callers always receive a map.
Map<String, dynamic> decodePusherData(Object? raw) {
  if (raw == null) return <String, dynamic>{};
  if (raw is Map<String, dynamic>) return raw;
  if (raw is Map) return raw.map((Object? key, Object? value) => MapEntry('$key', value));
  if (raw is String) {
    if (raw.isEmpty) return <String, dynamic>{};
    Object? decoded;
    try {
      decoded = jsonDecode(raw);
    } on FormatException {
      return <String, dynamic>{'data': raw};
    }
    if (decoded is Map) return decodePusherData(decoded);
    return <String, dynamic>{'data': decoded};
  }
  return <String, dynamic>{'data': raw};
}

/// Encodes an outbound frame.
String encodePusherFrame(String event, {Object? data, String? channel}) {
  return jsonEncode(<String, Object?>{
    'event': event,
    'data': ?data,
    'channel': ?channel,
  });
}

/// Reads an int that may arrive as a number or a numeric string.
int? readPusherInt(Object? value) {
  return switch (value) {
    final int number => number,
    final num number => number.toInt(),
    final String text => int.tryParse(text),
    _ => null,
  };
}

/// Resolves a Laravel Echo-style event name to the name on the wire, the way
/// Laravel Echo does.
///
/// - `OrderShipped` becomes `App\Events\OrderShipped` (an event class in
///   [namespace]).
/// - `.order.shipped` becomes `order.shipped` (a `broadcastAs()` name: a
///   leading dot).
/// - `\Domain\Events\OrderShipped` becomes `Domain\Events\OrderShipped`
///   (fully qualified: a leading backslash).
/// - `pusher:…` and `client-…` names are returned unchanged.
String laravelEventName(String event, {String namespace = r'App\Events'}) {
  if (event.startsWith('pusher:') || event.startsWith(PusherEvents.clientEventPrefix)) return event;
  if (event.startsWith('.') || event.startsWith(r'\')) return event.substring(1);
  if (namespace.isEmpty) return event;
  return '$namespace\\$event';
}
