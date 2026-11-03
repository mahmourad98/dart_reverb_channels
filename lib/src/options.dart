import 'dart:async';
import 'dart:math' as math;

import 'exceptions.dart';

/// This package's version, sent as the socket URL's `version` query parameter.
/// Kept equal to `pubspec.yaml`'s `version` (a test checks it).
const String dartReverbChannelsVersion = '1.2.1';

/// Builds headers on demand, so a value that changes between attempts (a
/// refreshed token, a rotated origin) is read fresh every time.
typedef ReverbHeadersBuilder = FutureOr<Map<String, String>> Function();

/// Decides whether a failure is worth another attempt. Replaces
/// [ReverbException.isRetryable] when set on [ReverbOptions.retryIf].
typedef ReverbRetryPredicate = bool Function(ReverbException error);

/// How many attempts a connection cycle, or a channel subscription, gets and
/// how long to wait between them.
///
/// The first attempt is attempt 1. [delays] gives the wait before attempt 2,
/// 3, …; its last entry repeats for any later attempt. [jitter] adds up to that
/// fraction of each delay at random, so clients dropped by one outage do not
/// return in lockstep.
final class RetryPolicy {
  /// A policy with explicit delays.
  const RetryPolicy({
    this.maxAttempts = 3,
    this.delays = const <Duration>[Duration(seconds: 1), Duration(seconds: 2)],
    this.jitter = 0,
  }) : assert(maxAttempts == null || maxAttempts > 0, 'maxAttempts must be positive'),
       assert(jitter >= 0 && jitter <= 1, 'jitter is a fraction between 0 and 1');

  /// Doubling delays from [initial], capped at [max], with [jitter]. The shape
  /// most Pusher clients use; leave [maxAttempts] null to retry forever.
  ///
  /// Throws [ArgumentError] unless `0 < initial <= max`.
  factory RetryPolicy.exponential({
    int? maxAttempts,
    Duration initial = const Duration(seconds: 1),
    Duration max = const Duration(seconds: 30),
    double jitter = 0.25,
  }) {
    if (initial <= Duration.zero) throw ArgumentError.value(initial, 'initial', 'must be positive');
    if (max < initial) throw ArgumentError.value(max, 'max', 'must not be shorter than initial');
    final delays = <Duration>[];
    var next = initial;
    while (next < max) {
      delays.add(next);
      next *= 2;
    }
    delays.add(max);
    return RetryPolicy(maxAttempts: maxAttempts, delays: delays, jitter: jitter);
  }

  /// A single attempt, never retried.
  static const RetryPolicy none = RetryPolicy(maxAttempts: 1, delays: <Duration>[Duration.zero]);

  /// The attempt limit, or null for no limit.
  final int? maxAttempts;

  /// The waits before the second, third, … attempt; the last one repeats.
  final List<Duration> delays;

  /// The random fraction of each delay added on top of it, from 0 to 1.
  final double jitter;

  /// Whether another attempt may follow attempt number [attempt] (1-based).
  bool allowsAttemptAfter(int attempt) {
    final limit = maxAttempts;
    return limit == null || attempt < limit;
  }

  /// The wait before the attempt that follows attempt number [attempt].
  Duration delayAfter(int attempt, math.Random random) {
    if (delays.isEmpty) return Duration.zero;
    final base = delays[math.min(math.max(attempt, 1), delays.length) - 1];
    if (jitter == 0 || base == Duration.zero) return base;
    final extra = (base.inMicroseconds * jitter * random.nextDouble()).round();
    return base + Duration(microseconds: extra);
  }
}

/// Everything about the server and the client's timing, the way Dio's
/// `BaseOptions` describe an HTTP client. Immutable; use [copyWith].
final class ReverbOptions {
  /// Options for the Reverb server at [host] with application key [appKey].
  const ReverbOptions({
    required this.host,
    required this.appKey,
    this.port,
    this.useTls = true,
    this.pathPrefix = '',
    this.socketPath,
    this.protocolVersion = 7,
    this.clientName = 'dart_reverb_channels',
    this.clientVersion = dartReverbChannelsVersion,
    this.queryParameters = const <String, String>{},
    this.headers = const <String, String>{},
    this.headersBuilder,
    this.connectTimeout = const Duration(seconds: 10),
    this.subscriptionTimeout = const Duration(seconds: 10),
    this.activityTimeout = const Duration(seconds: 120),
    this.pongTimeout = const Duration(seconds: 30),
    this.connectRetry = const RetryPolicy(),
    this.subscribeRetry = const RetryPolicy(),
    this.retryIf,
    this.reconnect = true,
    this.reconnectBackoff = const Duration(seconds: 1),
    this.closeWhenIdle = true,
    this.idleTimeout = Duration.zero,
  });

  /// Options for a hosted Pusher Channels app in [cluster] (such as `eu` or
  /// `mt1`): host `ws-{cluster}.pusher.com`, TLS on port 443 and every other
  /// field at its default. Adjust the rest with [copyWith].
  ///
  /// Throws [ArgumentError] unless [cluster] is a cluster name: letters,
  /// digits and hyphens.
  factory ReverbOptions.pusherCluster({required String cluster, required String appKey}) {
    final name = cluster.trim().toLowerCase();
    if (!RegExp(r'^[a-z0-9]+(-[a-z0-9]+)*$').hasMatch(name)) {
      throw ArgumentError.value(cluster, 'cluster', 'must be a Pusher cluster name such as "eu" or "mt1"');
    }
    return ReverbOptions(host: 'ws-$name.pusher.com', appKey: appKey);
  }

  /// The server's host name, without a scheme.
  final String host;

  /// The application key (`REVERB_APP_KEY`).
  final String appKey;

  /// The port; 443 with TLS and 80 without when null.
  final int? port;

  /// `wss` when true, `ws` otherwise.
  final bool useTls;

  /// Reverb's `REVERB_SERVER_PATH`: a server behind a proxy at `/ws` serves
  /// `/ws/app/{key}`. Leading and trailing slashes are optional.
  final String pathPrefix;

  /// Replaces the whole path, `{pathPrefix}/app/{key}`, for servers that
  /// route the socket somewhere else entirely.
  final String? socketPath;

  /// The Pusher protocol version sent as the `protocol` query parameter.
  final int protocolVersion;

  /// Sent as the `client` query parameter.
  final String clientName;

  /// Sent as the `version` query parameter.
  final String clientVersion;

  /// Extra query parameters for the socket URL.
  final Map<String, String> queryParameters;

  /// Headers sent with every socket upgrade request, such as `Origin`.
  /// Honoured by native connectors; browsers cannot set upgrade headers.
  final Map<String, String> headers;

  /// Headers built per connection attempt, merged over [headers].
  final ReverbHeadersBuilder? headersBuilder;

  /// The deadline for one connection attempt: opening the socket, the upgrade
  /// and `pusher:connection_established`.
  final Duration connectTimeout;

  /// The deadline for one channel attempt: authorization, its response body
  /// and the subscription acknowledgement.
  final Duration subscriptionTimeout;

  /// The longest silence before the client pings. The server's own
  /// `activity_timeout` wins when it is shorter.
  final Duration activityTimeout;

  /// How long a ping may go unanswered before the socket counts as dead.
  final Duration pongTimeout;

  /// The attempts one connection cycle gets.
  final RetryPolicy connectRetry;

  /// The attempts one channel subscription gets on an established socket.
  final RetryPolicy subscribeRetry;

  /// Overrides [ReverbException.isRetryable] for both policies, and for
  /// reconnecting after an established socket drops: a drop whose cause it
  /// refuses ends the cycle instead.
  final ReverbRetryPredicate? retryIf;

  /// Whether a dropped socket with live channels starts a new connection
  /// cycle and resubscribes them, when [retryIf] (or the cause's
  /// [ReverbException.isRetryable]) allows it.
  final bool reconnect;

  /// The wait before the first reconnect attempt after a 4100–4199 close or
  /// error, which the Pusher protocol asks clients to back off from. Other
  /// drops reconnect at once; later attempts follow [connectRetry].
  final Duration reconnectBackoff;

  /// Whether the socket closes once its last channel is released (after
  /// [idleTimeout]). When false, an established socket stays open until
  /// `closeAll`, ready for the next subscription.
  final bool closeWhenIdle;

  /// How long an established socket stays open after its last channel goes,
  /// when [closeWhenIdle] is true. Zero closes it at once. A socket that is
  /// still connecting when its last channel goes is always abandoned at once.
  final Duration idleTimeout;

  /// The socket URL these options describe.
  Uri get socketUri {
    final prefix = pathPrefix.replaceAll(RegExp(r'^/+|/+$'), '');
    final custom = socketPath;
    final path = custom != null
        ? (custom.startsWith('/') ? custom : '/$custom')
        : (prefix.isEmpty ? '/app/$appKey' : '/$prefix/app/$appKey');

    return Uri(
      scheme: useTls ? 'wss' : 'ws',
      host: host,
      port: port ?? (useTls ? 443 : 80),
      path: path,
      queryParameters: <String, String>{
        'protocol': '$protocolVersion',
        'client': clientName,
        'version': clientVersion,
        ...queryParameters,
      },
    );
  }

  /// Whether [error] deserves another attempt under these options.
  bool shouldRetry(ReverbException error) => retryIf?.call(error) ?? error.isRetryable;

  /// A copy with the given fields replaced.
  ///
  /// A field you leave out (or pass as null) keeps its current value, so
  /// [copyWith] cannot clear [port], [socketPath], [headersBuilder] or
  /// [retryIf]; build new options for that.
  ReverbOptions copyWith({
    String? host,
    String? appKey,
    int? port,
    bool? useTls,
    String? pathPrefix,
    String? socketPath,
    int? protocolVersion,
    String? clientName,
    String? clientVersion,
    Map<String, String>? queryParameters,
    Map<String, String>? headers,
    ReverbHeadersBuilder? headersBuilder,
    Duration? connectTimeout,
    Duration? subscriptionTimeout,
    Duration? activityTimeout,
    Duration? pongTimeout,
    RetryPolicy? connectRetry,
    RetryPolicy? subscribeRetry,
    ReverbRetryPredicate? retryIf,
    bool? reconnect,
    Duration? reconnectBackoff,
    bool? closeWhenIdle,
    Duration? idleTimeout,
  }) {
    return ReverbOptions(
      host: host ?? this.host,
      appKey: appKey ?? this.appKey,
      port: port ?? this.port,
      useTls: useTls ?? this.useTls,
      pathPrefix: pathPrefix ?? this.pathPrefix,
      socketPath: socketPath ?? this.socketPath,
      protocolVersion: protocolVersion ?? this.protocolVersion,
      clientName: clientName ?? this.clientName,
      clientVersion: clientVersion ?? this.clientVersion,
      queryParameters: queryParameters ?? this.queryParameters,
      headers: headers ?? this.headers,
      headersBuilder: headersBuilder ?? this.headersBuilder,
      connectTimeout: connectTimeout ?? this.connectTimeout,
      subscriptionTimeout: subscriptionTimeout ?? this.subscriptionTimeout,
      activityTimeout: activityTimeout ?? this.activityTimeout,
      pongTimeout: pongTimeout ?? this.pongTimeout,
      connectRetry: connectRetry ?? this.connectRetry,
      subscribeRetry: subscribeRetry ?? this.subscribeRetry,
      retryIf: retryIf ?? this.retryIf,
      reconnect: reconnect ?? this.reconnect,
      reconnectBackoff: reconnectBackoff ?? this.reconnectBackoff,
      closeWhenIdle: closeWhenIdle ?? this.closeWhenIdle,
      idleTimeout: idleTimeout ?? this.idleTimeout,
    );
  }

  /// Throws [ArgumentError] when a field cannot work.
  void validate() {
    if (host.trim().isEmpty) throw ArgumentError.value(host, 'host', 'must not be empty');
    if (appKey.trim().isEmpty) throw ArgumentError.value(appKey, 'appKey', 'must not be empty');
    final value = port;
    if (value != null && (value <= 0 || value > 65535)) {
      throw ArgumentError.value(value, 'port', 'must be between 1 and 65535');
    }
    for (final (String name, Duration duration) in <(String, Duration)>[
      ('connectTimeout', connectTimeout),
      ('subscriptionTimeout', subscriptionTimeout),
      ('activityTimeout', activityTimeout),
      ('pongTimeout', pongTimeout),
    ]) {
      if (duration <= Duration.zero) throw ArgumentError.value(duration, name, 'must be positive');
    }
    for (final (String name, Duration duration) in <(String, Duration)>[
      ('reconnectBackoff', reconnectBackoff),
      ('idleTimeout', idleTimeout),
    ]) {
      if (duration < Duration.zero) throw ArgumentError.value(duration, name, 'must not be negative');
    }
  }
}
