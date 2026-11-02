import 'dart:async';
import 'dart:convert';

import 'package:dart_reverb_channels/testing.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

import 'support.dart';

const String _users = 'private-users.1';
const String _news = 'news';

ChannelAuthorizationRequest _request() =>
    ChannelAuthorizationRequest(channel: _users, socketId: '123.456', cancelToken: ReverbCancelToken());

void main() {
  group('connect()', () {
    test('opens the socket with no channel and keeps it until a subscription takes it over', () {
      fakeAsync((FakeAsync async) {
        // The server answers pings, so the idle socket passes its keep-alive.
        final rig = ClientRig()..connector.autoPong = true;
        final outcome = Outcome(rig.client.connect());
        async.flushMicrotasks();
        expect(outcome.succeeded, isTrue);
        expect(rig.client.state, ReverbConnectionState.connected);

        async.elapse(const Duration(minutes: 5));
        expect(rig.connector.socket.isClosed, isFalse, reason: 'held open with no channel');

        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);
        expect(rig.connector.requests, hasLength(1), reason: 'the subscription reused the socket');

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();
        expect(rig.connector.socket.closedByClient, isTrue, reason: 'closeWhenIdle applies again');
      });
    });

    test('completes at once on an established socket and joins a running cycle', () {
      fakeAsync((FakeAsync async) {
        final connected = ClientRig();
        connected.client.subscribe(_news);
        async.flushMicrotasks();
        final immediate = Outcome(connected.client.connect());
        async.flushMicrotasks();
        expect(immediate.succeeded, isTrue);
        expect(connected.connector.requests, hasLength(1));

        final connecting = ClientRig();
        connecting.connector.hold = true;
        final subscription = connecting.client.subscribe(_news);
        final joined = Outcome(connecting.client.connect());
        async.flushMicrotasks();
        expect(joined.done, isFalse);
        connecting.connector.completePending();
        async.flushMicrotasks();
        expect(joined.succeeded, isTrue);
        expect(connecting.connector.requests, hasLength(1));

        // connect() came after that subscription, so it holds the socket on.
        unawaited(subscription.cancel());
        async.flushMicrotasks();
        expect(connecting.connector.socket.isClosed, isFalse);
      });
    });

    test('a failed cycle fails it and ends the hold', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(StateError('offline'), times: 3);
        final outcome = Outcome(rig.client.connect());
        async.elapse(const Duration(seconds: 10));
        expect(outcome.error, isA<ReverbConnectionException>());
        expect(rig.client.state, ReverbConnectionState.unavailable);

        async.elapse(const Duration(minutes: 10));
        expect(rig.connector.requests, hasLength(3), reason: 'no retries after the cycle failed');

        final subscription = rig.client.subscribe(_news);
        async.flushMicrotasks();
        unawaited(subscription.cancel());
        async.flushMicrotasks();
        expect(rig.connector.socket.closedByClient, isTrue, reason: 'the hold ended with the failure');
      });
    });

    test('a drop while held reconnects; pause and resume keep the hold', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        unawaited(rig.client.connect());
        async.flushMicrotasks();

        rig.connector.socket.serverClose(1006);
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(2));
        expect(rig.client.state, ReverbConnectionState.connected);

        rig.client.pause();
        async.flushMicrotasks();
        expect(rig.client.state, ReverbConnectionState.paused);
        rig.client.resume();
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(3));
        expect(rig.client.state, ReverbConnectionState.connected);
      });
    });

    test('closeAll ends the hold; while paused or after dispose it fails', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        unawaited(rig.client.connect());
        async.flushMicrotasks();

        unawaited(rig.client.closeAll());
        async.elapse(const Duration(minutes: 1));
        expect(rig.connector.socket.closedByClient, isTrue);
        expect(rig.client.state, ReverbConnectionState.disconnected);
        expect(rig.connector.requests, hasLength(1));

        rig.client.pause();
        final paused = Outcome(rig.client.connect());
        unawaited(rig.client.dispose());
        final disposed = Outcome(rig.client.connect());
        async.flushMicrotasks();
        expect(paused.error, isA<ReverbCancelledException>());
        expect(disposed.error, isA<ReverbConfigurationException>());
        expect(rig.connector.requests, hasLength(1));
      });
    });
  });

  group('client-wide streams', () {
    test('errors carries every report onError receives, to any number of listeners', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        rig.authorizer.handler = (ChannelAuthorizationRequest request) =>
            throw ReverbAuthorizationException('Forbidden', channel: request.channel, statusCode: 403);
        final first = <ReverbErrorReport>[];
        final second = <ReverbErrorReport>[];
        rig.client.errors.listen(first.add);
        rig.client.errors.listen(second.add);

        rig.client.subscribe(_users);
        async.flushMicrotasks();

        expect(rig.reports, hasLength(1));
        expect(first, rig.reports);
        expect(second, rig.reports);
      });
    });

    test('events carries each delivered event once, from every held channel, until dispose', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final seen = <ReverbEvent>[];
        var done = false;
        rig.client.events.listen(seen.add, onDone: () => done = true);
        rig.client
          ..subscribe(_users)
          ..subscribe(_users)
          ..subscribe(_news);
        async.flushMicrotasks();

        rig.connector.socket
          ..event(_users, 'order.shipped', <String, Object?>{'order_id': 1})
          ..event(_news, 'headline', <String, Object?>{'id': 2})
          ..event('nobody-holds-this', 'ignored', <String, Object?>{'id': 3});
        async.flushMicrotasks();
        expect(seen.map((ReverbEvent e) => '${e.channel} ${e.name}'), <String>[
          '$_users order.shipped',
          '$_news headline',
        ]);

        unawaited(rig.client.dispose());
        async.flushMicrotasks();
        expect(done, isTrue);
      });
    });
  });

  group('HttpChannelAuthorizer.responseParser', () {
    final endpoint = Uri.parse('https://api.test/broadcasting/auth');

    HttpChannelAuthorizer authorizer(http.Response response, ChannelAuthorizationResponseParser parser) =>
        HttpChannelAuthorizer(
          endpoint: endpoint,
          responseParser: parser,
          clientFactory: () => MockClient((http.Request request) async => response),
        );

    test('reads a response wrapped in an envelope', () async {
      final authorization = await authorizer(
        http.Response('{"data":{"auth":"key:signature"}}', 200),
        (http.Response response) {
          final envelope = jsonDecode(response.body) as Map<String, dynamic>;
          return ChannelAuthorization.fromJson((envelope['data'] as Map).cast<String, dynamic>());
        },
      ).authorize(_request());
      expect(authorization.auth, 'key:signature');
    });

    test('a parser error is a malformed response, which is not retried', () async {
      final failure = await authorizer(
        http.Response('{"unexpected":true}', 200),
        (http.Response response) => throw const FormatException('no auth'),
      ).authorize(_request()).then<Object?>((_) => null, onError: (Object error) => error);
      expect(failure, isA<ReverbAuthorizationException>());
      final error = failure! as ReverbAuthorizationException;
      expect(error.malformedResponse, isTrue);
      expect(error.isRetryable, isFalse);
      expect(error.cause, isA<FormatException>());
    });

    test('a ReverbAuthorizationException from the parser passes through unchanged', () async {
      const refusal = ReverbAuthorizationException('Account locked', channel: _users, statusCode: 423);
      final failure = await authorizer(
        http.Response('{"locked":true}', 200),
        (http.Response response) => throw refusal,
      ).authorize(_request()).then<Object?>((_) => null, onError: (Object error) => error);
      expect(failure, same(refusal));
    });

    test('a non-200 response never reaches the parser', () async {
      var parsed = false;
      final failure = await authorizer(http.Response('busy', 503), (http.Response response) {
        parsed = true;
        return const ChannelAuthorization(auth: 'never');
      }).authorize(_request()).then<Object?>((_) => null, onError: (Object error) => error);
      expect(parsed, isFalse);
      expect((failure! as ReverbAuthorizationException).statusCode, 503);
    });
  });

  group('ReverbOptions.pusherCluster', () {
    test('builds the hosted Pusher socket URL', () {
      final options = ReverbOptions.pusherCluster(cluster: 'EU', appKey: 'key');
      expect(options.host, 'ws-eu.pusher.com');
      expect(options.socketUri.toString(), startsWith('wss://ws-eu.pusher.com:443/app/key?protocol=7'));
      expect(options.copyWith(connectTimeout: const Duration(seconds: 5)).host, 'ws-eu.pusher.com');
    });

    test('rejects names that are not cluster names', () {
      for (final name in <String>['', ' ', 'eu.pusher.com', 'eu/', '-eu', 'eu-']) {
        expect(
          () => ReverbOptions.pusherCluster(cluster: name, appKey: 'key'),
          throwsArgumentError,
          reason: name,
        );
      }
    });
  });

  group('subscription count', () {
    test('is read from pusher_internal:subscription_count and delivered as pusher:subscription_count', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_news));
        final tapped = <ReverbEvent>[];
        rig.client.events.listen(tapped.add);
        async.flushMicrotasks();
        expect(probe.subscription.subscriptionCount, isNull);

        rig.connector.socket.event(_news, PusherEvents.internalSubscriptionCount, <String, Object?>{
          'subscription_count': 3,
        });
        async.flushMicrotasks();

        expect(probe.subscription.subscriptionCount, 3);
        expect(probe.events.single.name, PusherEvents.subscriptionCount);
        expect(probe.events.single.data['subscription_count'], 3);
        expect(tapped.single.name, PusherEvents.subscriptionCount);
      });
    });

    test('is ignored before the channel is subscribed and forgotten on a drop', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final subscription = rig.client.subscribe(_news);
        async.flushMicrotasks();

        rig.connector.socket.event(_news, PusherEvents.internalSubscriptionCount, <String, Object?>{
          'subscription_count': 9,
        });
        async.flushMicrotasks();
        expect(subscription.subscriptionCount, isNull, reason: 'not subscribed yet');

        rig.connector.socket
          ..acknowledge(_news)
          ..event(_news, PusherEvents.internalSubscriptionCount, <String, Object?>{'subscription_count': 4});
        async.flushMicrotasks();
        expect(subscription.subscriptionCount, 4);

        rig.connector.socket.serverClose(1006);
        async.flushMicrotasks();
        expect(subscription.subscriptionCount, isNull);
      });
    });
  });
}
