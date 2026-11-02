import 'package:dart_reverb_channels/dart_reverb_channels.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
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
