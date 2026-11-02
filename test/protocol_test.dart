import 'dart:math' as math;

import 'package:dart_reverb_channels/dart_reverb_channels.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('ReverbOptions', () {
    test('builds the socket URL with the default path, a prefix or a custom path', () {
      const options = ReverbOptions(host: 'ws.example.com', appKey: 'key');
      expect(
        options.socketUri.toString(),
        'wss://ws.example.com:443/app/key?protocol=7&client=dart_reverb_channels&version=$dartReverbChannelsVersion',
      );
      expect(options.copyWith(pathPrefix: '/ws/').socketUri.path, '/ws/app/key');
      expect(options.copyWith(socketPath: 'custom/path').socketUri.path, '/custom/path');
      expect(options.copyWith(useTls: false).socketUri.port, 80);
      expect(
        options.copyWith(port: 6001, queryParameters: <String, String>{'x': 'y'}).socketUri.query,
        contains('x=y'),
      );
    });

    test('validates its fields', () {
      expect(() => const ReverbOptions(host: '', appKey: 'k').validate(), throwsArgumentError);
      expect(() => const ReverbOptions(host: 'h', appKey: ' ').validate(), throwsArgumentError);
      expect(() => const ReverbOptions(host: 'h', appKey: 'k', port: 70000).validate(), throwsArgumentError);
      expect(
        () => const ReverbOptions(host: 'h', appKey: 'k', connectTimeout: Duration.zero).validate(),
        throwsArgumentError,
      );
    });

    test('retryIf overrides the default classification', () {
      const timeout = ReverbTimeoutException('t', timeout: Duration(seconds: 1));
      expect(const ReverbOptions(host: 'h', appKey: 'k').shouldRetry(timeout), isTrue);
      expect(ReverbOptions(host: 'h', appKey: 'k', retryIf: (_) => false).shouldRetry(timeout), isFalse);
    });
  });

  group('RetryPolicy', () {
    test('the default allows three attempts, 1 s then 2 s apart', () {
      const policy = RetryPolicy();
      final random = math.Random(1);
      expect(policy.allowsAttemptAfter(1), isTrue);
      expect(policy.allowsAttemptAfter(2), isTrue);
      expect(policy.allowsAttemptAfter(3), isFalse);
      expect(policy.delayAfter(1, random), const Duration(seconds: 1));
      expect(policy.delayAfter(2, random), const Duration(seconds: 2));
      expect(policy.delayAfter(5, random), const Duration(seconds: 2));
    });

    test('exponential doubles up to the cap and jitters within bounds', () {
      final policy = RetryPolicy.exponential(max: const Duration(seconds: 8), jitter: 0.25);
      final random = math.Random(3);
      expect(policy.allowsAttemptAfter(1000), isTrue);
      expect(policy.delays, const <Duration>[
        Duration(seconds: 1),
        Duration(seconds: 2),
        Duration(seconds: 4),
        Duration(seconds: 8),
      ]);
      for (var attempt = 1; attempt < 8; attempt++) {
        final base = policy.delays[math.min(attempt, policy.delays.length) - 1];
        final delay = policy.delayAfter(attempt, random);
        expect(delay >= base && delay <= base * 1.25, isTrue, reason: '$attempt: $delay');
      }
    });
  });

  test('exponential refuses a non-positive start or a cap below it instead of looping', () {
    expect(() => RetryPolicy.exponential(initial: Duration.zero), throwsArgumentError);
    expect(() => RetryPolicy.exponential(initial: const Duration(seconds: -1)), throwsArgumentError);
    expect(
      () => RetryPolicy.exponential(initial: const Duration(seconds: 5), max: const Duration(seconds: 1)),
      throwsArgumentError,
    );
    expect(
      RetryPolicy.exponential(initial: const Duration(seconds: 2), max: const Duration(seconds: 2)).delays,
      const <Duration>[Duration(seconds: 2)],
    );
  });

  test('copyWith changes the idle rules, and validate refuses negative waits', () {
    const options = ReverbOptions(host: 'h', appKey: 'k');
    expect(options.closeWhenIdle, isTrue);
    expect(options.copyWith(closeWhenIdle: false).closeWhenIdle, isFalse);
    expect(options.copyWith(closeWhenIdle: false).copyWith(port: 1).closeWhenIdle, isFalse);
    expect(() => options.copyWith(idleTimeout: const Duration(seconds: -1)).validate(), throwsArgumentError);
    expect(() => options.copyWith(reconnectBackoff: const Duration(seconds: -1)).validate(), throwsArgumentError);
  });

  group('default retry classification', () {
    test('matches the Pusher close code ranges', () {
      expect(const ReverbProtocolException('x', code: 4001).isRetryable, isFalse);
      expect(const ReverbProtocolException('x', code: 4100).isRetryable, isTrue);
      expect(const ReverbProtocolException('x', code: 4100).requiresBackoff, isTrue);
      expect(const ReverbProtocolException('x', code: 4200).isRetryable, isTrue);
    });

    test('retries server-side authorization failures only', () {
      ReverbAuthorizationException auth(int? status, {bool malformed = false}) =>
          ReverbAuthorizationException('x', channel: 'c', statusCode: status, malformedResponse: malformed);
      expect(auth(null).isRetryable, isTrue);
      expect(auth(500).isRetryable, isTrue);
      expect(auth(429).isRetryable, isTrue);
      expect(auth(401).isRetryable, isFalse);
      expect(auth(403).isRetryable, isFalse);
      expect(auth(200, malformed: true).isRetryable, isFalse);
      expect(const ReverbSubscriptionException('x', channel: 'c', status: 403).isRetryable, isFalse);
      expect(const ReverbSubscriptionException('x', channel: 'c', status: 503).isRetryable, isTrue);
    });

    test('unwrapReverbCause removes nested wrappers', () {
      const root = FormatException('root');
      const wrapped = ReverbConnectionException('outer', cause: ReverbConnectionException('inner', cause: root));
      expect(wrapped.rootCause, same(root));
    });
  });
}
