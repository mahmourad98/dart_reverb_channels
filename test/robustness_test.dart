import 'dart:async';
import 'dart:io';

import 'package:dart_reverb_channels/testing.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

const String _users = 'private-users.1';

/// Edge cases: disposal races, handlers that re-enter the client, idle rules,
/// late acknowledgements and protocol bookkeeping frames.
void main() {
  group('disposal', () {
    test('a subscribe racing dispose() gets a failed subscription and opens nothing', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        Probe? late;
        unawaited(rig.client.dispose());
        scheduleMicrotask(() => late = Probe(rig.client.subscribe(_users)));
        async.flushMicrotasks();

        expect(late!.readyError, isA<ReverbConfigurationException>());
        expect(rig.connector.requests, isEmpty);
        expect(rig.client.channels, isEmpty);
      });
    });
  });

  group('re-entrant handlers', () {
    test('closeAll from onError on a fatal connect failure leaves the client disconnected', () {
      fakeAsync((FakeAsync async) {
        late ReverbClient client;
        final connector = FakeReverbConnector();
        client = ReverbClient(
          options: testOptions,
          connector: connector,
          authorizer: FakeChannelAuthorizer(),
          onError: (_) => unawaited(client.closeAll()),
        );
        final probe = Probe(client.subscribe(_users));
        async.flushMicrotasks();

        connector.socket.protocolError(4001, 'App does not exist');
        async.flushMicrotasks();

        expect(client.state, ReverbConnectionState.disconnected);
        expect(probe.readyError, isA<ReverbCancelledException>());
      });
    });

    test('closeAll from onError on a drop does not dial a new socket', () {
      fakeAsync((FakeAsync async) {
        late ReverbClient client;
        final connector = FakeReverbConnector(autoHandshake: true, autoAcknowledge: true);
        client = ReverbClient(
          options: testOptions,
          connector: connector,
          authorizer: FakeChannelAuthorizer(),
          onError: (ReverbErrorReport report) {
            if (report.stage == ReverbErrorStage.connection) unawaited(client.closeAll());
          },
        );
        Probe(client.subscribe(_users));
        async.flushMicrotasks();

        connector.socket.serverClose(1006);
        async.elapse(const Duration(seconds: 5));

        expect(connector.requests, hasLength(1));
        expect(client.state, ReverbConnectionState.disconnected);
      });
    });

    test('cancelling from onError on a channel failure schedules no retry', () {
      fakeAsync((FakeAsync async) {
        late ReverbSubscription subscription;
        final connector = FakeReverbConnector(autoHandshake: true);
        final authorizer = FakeChannelAuthorizer()
          ..handler = (ChannelAuthorizationRequest request) =>
              throw ReverbAuthorizationException('Down', channel: request.channel, statusCode: 503);
        final client = ReverbClient(
          options: testOptions,
          connector: connector,
          authorizer: authorizer,
          onError: (ReverbErrorReport report) {
            if (report.stage == ReverbErrorStage.subscribe) unawaited(subscription.cancel());
          },
        );
        subscription = client.subscribe(_users);
        async.elapse(const Duration(seconds: 10));

        expect(authorizer.requests, hasLength(1));
        expect(client.channels, isEmpty);
      });
    });
  });

  group('idle rules', () {
    test('releasing the last channel while connecting abandons the attempt even with an idle timeout', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(options: testOptions.copyWith(idleTimeout: const Duration(seconds: 5)))
          ..connector.hold = true;
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();

        expect(rig.connector.requests.single.cancelToken.isCancelled, isTrue);
        expect(rig.client.state, ReverbConnectionState.disconnected);
        async.elapse(const Duration(seconds: 30));
        expect(rig.connector.requests, hasLength(1));
      });
    });

    test('closeWhenIdle false keeps the socket open for the next subscription', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(options: testOptions.copyWith(closeWhenIdle: false))..connector.autoPong = true;
        final first = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        unawaited(first.subscription.cancel());
        async.elapse(const Duration(minutes: 5));

        expect(rig.connector.socket.isClosed, isFalse);
        expect(rig.client.state, ReverbConnectionState.connected);

        final second = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(second.succeeded, isTrue);
        expect(rig.connector.sockets, hasLength(1));
      });
    });
  });

  group('acknowledgements and frames', () {
    test('a late acknowledgement during the back-off completes the subscription without a retry', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        async.elapse(const Duration(seconds: 10));
        expect(rig.reports.single.error, isA<ReverbTimeoutException>());

        rig.connector.socket.acknowledge(_users);
        async.elapse(const Duration(seconds: 5));

        expect(probe.succeeded, isTrue);
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(1));
      });
    });

    test('a subscription that failed stays silent: a late acknowledgement is unsubscribed, events dropped', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(
          autoAcknowledge: false,
          options: testOptions.copyWith(subscribeRetry: RetryPolicy.none),
        );
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        async.elapse(const Duration(seconds: 10));
        expect(probe.readyError, isA<ReverbTimeoutException>());

        rig.connector.socket
          ..acknowledge(_users)
          ..event(_users, 'order.shipped', <String, Object?>{'order_id': 1});
        async.flushMicrotasks();

        expect(probe.events, isEmpty);
        expect(rig.unsubscribes(rig.connector.socket, _users), hasLength(1));
      });
    });

    test('pusher_internal frames the client does not use are not delivered as events', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket
          ..event(_users, 'pusher_internal:future_bookkeeping', <String, Object?>{'value': 2})
          ..event(_users, 'order.shipped', <String, Object?>{'order_id': 1});
        async.flushMicrotasks();

        expect(probe.events.map((ReverbEvent e) => e.name), <String>['order.shipped']);
      });
    });

    test('a presence roster seeded from ids alone lists every member', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final probe = Probe(rig.client.subscribe('presence-room.1'));
        async.flushMicrotasks();

        rig.connector.socket.acknowledge(
          'presence-room.1',
          data: <String, Object?>{
            'presence': <String, Object?>{
              'ids': <int>[1, 2],
              'count': 2,
            },
          },
        );
        async.flushMicrotasks();

        expect(probe.subscription.members.keys, <String>['1', '2']);
        expect(probe.subscription.members['1']!.info, isNull);
      });
    });
  });

  group('public surface', () {
    test('channels is a snapshot that cannot be modified', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        final channels = rig.client.channels;
        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();

        expect(channels, <String>[_users]);
        expect(() => channels.add('x'), throwsUnsupportedError);
        expect(rig.client.channels, isEmpty);
      });
    });

    test('a cancelled subscription reports no members and refuses client events', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();

        expect(probe.subscription.members, isEmpty);
        expect(probe.subscription.me, isNull);
        expect(() => probe.subscription.trigger('typing'), throwsStateError);
      });
    });

    test('the version constant matches pubspec.yaml', () {
      final pubspec = File('pubspec.yaml').readAsStringSync();
      final version = RegExp(r'^version:\s*(\S+)', multiLine: true).firstMatch(pubspec)!.group(1);

      expect(dartReverbChannelsVersion, version);
      expect(testOptions.socketUri.queryParameters['version'], version);
    });
  });
}
