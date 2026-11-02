import 'dart:async';

import 'package:dart_reverb_channels/testing.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

const String _users = 'private-users.1';
const String _news = 'news';

void main() {
  group('subscription status', () {
    test('pending until acknowledged, then subscribed', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final subscription = rig.client.subscribe(_users);
        final log = StatusLog(subscription);
        async.flushMicrotasks();
        expect(subscription.status, ReverbSubscriptionStatus.pending);

        rig.connector.socket.acknowledge(_users);
        async.flushMicrotasks();
        expect(subscription.status, ReverbSubscriptionStatus.subscribed);
        expect(subscription.error, isNull);
        expect(log.changes, <ReverbSubscriptionStatus>[ReverbSubscriptionStatus.subscribed]);
      });
    });

    test('a drop reads resubscribing, then subscribed once the channel is back', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        final log = StatusLog(probe.subscription);
        async.flushMicrotasks();

        rig.connector.socket.serverClose(1006);
        async.flushMicrotasks();

        expect(log.changes, <ReverbSubscriptionStatus>[
          ReverbSubscriptionStatus.subscribed,
          ReverbSubscriptionStatus.resubscribing,
          ReverbSubscriptionStatus.subscribed,
        ]);
        rig.connector.socket.event(_users, 'order.shipped', <String, Object?>{'order_id': 4});
        async.flushMicrotasks();
        expect(probe.events.single.data['order_id'], 4);
      });
    });

    test('a refused resubscribe after ready reads failed with the error; ready stays successful', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        final log = StatusLog(probe.subscription);
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);

        rig.authorizer.handler = (ChannelAuthorizationRequest request) =>
            throw ReverbAuthorizationException('Token expired', channel: request.channel, statusCode: 403);
        rig.connector.socket.serverClose(1006);
        async.flushMicrotasks();

        final subscription = probe.subscription;
        expect(subscription.status, ReverbSubscriptionStatus.failed);
        expect(subscription.error, isA<ReverbAuthorizationException>());
        expect((subscription.error! as ReverbAuthorizationException).statusCode, 403);
        expect(log.changes.last, ReverbSubscriptionStatus.failed);
        expect(probe.succeeded, isTrue, reason: 'ready reports only the first outcome');
        expect(probe.eventsDone, isFalse);
      });
    });

    test('a failed subscription that is subscribed again says so instead of reviving silently', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_news));
        final log = StatusLog(probe.subscription);
        async.flushMicrotasks();

        rig.connector.failNext(StateError('offline'), times: 3);
        rig.connector.socket.serverClose();
        async.elapse(const Duration(seconds: 10));
        expect(rig.client.state, ReverbConnectionState.unavailable);
        expect(probe.subscription.status, ReverbSubscriptionStatus.failed);
        expect(probe.subscription.error, isA<ReverbConnectionException>());

        // Another owner subscribes elsewhere: the new cycle brings this channel back too.
        final other = Probe(rig.client.subscribe('other'));
        async.flushMicrotasks();
        expect(other.subscription.status, ReverbSubscriptionStatus.subscribed);

        expect(log.changes, <ReverbSubscriptionStatus>[
          ReverbSubscriptionStatus.subscribed,
          ReverbSubscriptionStatus.resubscribing,
          ReverbSubscriptionStatus.failed,
          ReverbSubscriptionStatus.resubscribing,
          ReverbSubscriptionStatus.subscribed,
        ]);
        expect(probe.subscription.error, isNull);
        rig.connector.socket.event(_news, 'headline', <String, Object?>{'id': 1});
        async.flushMicrotasks();
        expect(probe.events, hasLength(1));
      });
    });

    test('a subscription that never succeeded goes failed, pending, subscribed when retried', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(StateError('offline'), times: 3);
        final probe = Probe(rig.client.subscribe(_news));
        final log = StatusLog(probe.subscription);
        async.elapse(const Duration(seconds: 10));
        expect(probe.readyError, isA<ReverbConnectionException>());

        unawaited(rig.client.reconnect());
        async.flushMicrotasks();

        expect(log.changes, <ReverbSubscriptionStatus>[
          ReverbSubscriptionStatus.failed,
          ReverbSubscriptionStatus.pending,
          ReverbSubscriptionStatus.subscribed,
        ]);
        expect(probe.readyError, isA<ReverbConnectionException>(), reason: 'ready keeps its first outcome');
      });
    });

    test('an owner joining during a resubscribe is pending while the earlier owner resubscribes', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final first = rig.client.subscribe(_users);
        async.flushMicrotasks();

        rig.connector.hold = true;
        rig.connector.socket.serverClose(1006);
        async.flushMicrotasks();
        final second = rig.client.subscribe(_users);
        expect(first.status, ReverbSubscriptionStatus.resubscribing);
        expect(second.status, ReverbSubscriptionStatus.pending);

        rig.connector.completePending();
        async.flushMicrotasks();
        expect(first.status, ReverbSubscriptionStatus.subscribed);
        expect(second.status, ReverbSubscriptionStatus.subscribed);
      });
    });

    test('a new owner of a subscribed channel starts subscribed', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        rig.client.subscribe(_users);
        async.flushMicrotasks();
        expect(rig.client.subscribe(_users).status, ReverbSubscriptionStatus.subscribed);
      });
    });

    test('pause reads resubscribing; resume brings subscribed back', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final subscription = rig.client.subscribe(_users);
        final log = StatusLog(subscription);
        async.flushMicrotasks();

        rig.client.pause();
        async.flushMicrotasks();
        expect(subscription.status, ReverbSubscriptionStatus.resubscribing);
        rig.client.resume();
        async.flushMicrotasks();
        expect(log.changes, <ReverbSubscriptionStatus>[
          ReverbSubscriptionStatus.subscribed,
          ReverbSubscriptionStatus.resubscribing,
          ReverbSubscriptionStatus.subscribed,
        ]);
      });
    });

    test('cancel and closeAll end with cancelled and close the status stream', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final cancelled = rig.client.subscribe(_users);
        final closed = rig.client.subscribe(_news);
        final cancelledLog = StatusLog(cancelled);
        final closedLog = StatusLog(closed);
        async.flushMicrotasks();

        unawaited(cancelled.cancel());
        async.flushMicrotasks();
        unawaited(rig.client.closeAll());
        async.flushMicrotasks();

        expect(cancelled.status, ReverbSubscriptionStatus.cancelled);
        expect(closed.status, ReverbSubscriptionStatus.cancelled);
        expect(cancelledLog.changes.last, ReverbSubscriptionStatus.cancelled);
        expect(closedLog.changes.last, ReverbSubscriptionStatus.cancelled);
        expect(cancelledLog.done, isTrue);
        expect(closedLog.done, isTrue);
      });
    });

    test('a request the configuration cannot serve is failed from the start', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(withAuthorizer: false);
        final subscription = rig.client.subscribe(_users);
        expect(subscription.status, ReverbSubscriptionStatus.failed);
        expect(subscription.error, isA<ReverbConfigurationException>());

        final log = StatusLog(subscription);
        unawaited(subscription.cancel());
        async.flushMicrotasks();
        expect(log.changes, <ReverbSubscriptionStatus>[ReverbSubscriptionStatus.cancelled]);
        expect(rig.connector.requests, isEmpty);
      });
    });
  });

  group('reconnect()', () {
    test('after the attempts run out it retries at once and failed subscriptions recover', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_news));
        async.flushMicrotasks();
        rig.connector.failNext(StateError('offline'), times: 3);
        rig.connector.socket.serverClose();
        async.elapse(const Duration(minutes: 10));
        expect(rig.client.state, ReverbConnectionState.unavailable);
        expect(rig.connector.requests, hasLength(4), reason: 'nothing retries on its own');

        final outcome = Outcome(rig.client.reconnect());
        async.flushMicrotasks();

        expect(outcome.succeeded, isTrue);
        expect(rig.client.state, ReverbConnectionState.connected);
        expect(rig.connector.requests, hasLength(5));
        expect(probe.subscription.status, ReverbSubscriptionStatus.subscribed);
        rig.connector.socket.event(_news, 'headline', <String, Object?>{'id': 1});
        async.flushMicrotasks();
        expect(probe.events, hasLength(1));
      });
    });

    test('on a connected client it replaces the socket and resubscribes with fresh authorization', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final subscription = rig.client.subscribe(_users);
        final log = StatusLog(subscription);
        async.flushMicrotasks();
        final old = rig.connector.socket;

        final outcome = Outcome(rig.client.reconnect());
        async.flushMicrotasks();

        expect(outcome.succeeded, isTrue);
        expect(old.closedByClient, isTrue);
        expect(rig.connector.sockets, hasLength(2));
        expect(rig.authorizer.requests.map((ChannelAuthorizationRequest r) => r.socketId), <String>['1.1', '2.2']);
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(1));
        expect(log.changes, <ReverbSubscriptionStatus>[
          ReverbSubscriptionStatus.subscribed,
          ReverbSubscriptionStatus.resubscribing,
          ReverbSubscriptionStatus.subscribed,
        ]);
        expect(rig.reports, isEmpty, reason: 'closing the old socket on purpose is not a failure');
      });
    });

    test('during a back-off it retries at once with a fresh set of attempts', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(StateError('offline'), times: 2);
        final probe = Probe(rig.client.subscribe(_news));
        async.elapse(const Duration(seconds: 1));
        expect(rig.connector.requests, hasLength(2), reason: 'two of three attempts used, waiting 2 s');

        rig.connector.failNext(StateError('offline'), times: 2);
        final outcome = Outcome(rig.client.reconnect());
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(3), reason: 'no wait for the back-off');
        async.elapse(const Duration(seconds: 3));

        expect(rig.connector.requests, hasLength(5), reason: 'the new cycle got three attempts again');
        expect(outcome.succeeded, isTrue);
        expect(probe.succeeded, isTrue);
      });
    });

    test('its future fails with the error that ended the new cycle', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(StateError('offline'), times: 6);
        final probe = Probe(rig.client.subscribe(_news));
        async.elapse(const Duration(seconds: 10));

        final outcome = Outcome(rig.client.reconnect());
        async.elapse(const Duration(seconds: 10));

        expect(outcome.error, isA<ReverbConnectionException>());
        expect(rig.client.state, ReverbConnectionState.unavailable);
        expect(probe.subscription.status, ReverbSubscriptionStatus.failed);
      });
    });

    test('closeAll, pause or the release of the last channel cancel a pending reconnect', () {
      fakeAsync((FakeAsync async) {
        for (final stop in <String>['closeAll', 'pause', 'cancel']) {
          final rig = ClientRig();
          final subscription = rig.client.subscribe(_news);
          async.flushMicrotasks();
          rig.connector.hold = true;

          final outcome = Outcome(rig.client.reconnect());
          async.flushMicrotasks();
          expect(rig.connector.pendingCount, 1);
          switch (stop) {
            case 'closeAll':
              unawaited(rig.client.closeAll());
            case 'pause':
              rig.client.pause();
            case 'cancel':
              unawaited(subscription.cancel());
          }
          async.flushMicrotasks();

          expect(outcome.error, isA<ReverbCancelledException>(), reason: stop);
          expect(rig.connector.pendingCount, 0, reason: '$stop aborts the pending upgrade');
        }
      });
    });

    test('while paused or after dispose it changes nothing and fails', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        rig.client.subscribe(_news);
        async.flushMicrotasks();

        rig.client.pause();
        final paused = Outcome(rig.client.reconnect());
        async.flushMicrotasks();
        expect(paused.error, isA<ReverbCancelledException>());
        expect(rig.client.state, ReverbConnectionState.paused);
        expect(rig.connector.requests, hasLength(1));

        unawaited(rig.client.dispose());
        async.flushMicrotasks();
        final disposed = Outcome(rig.client.reconnect());
        async.flushMicrotasks();
        expect(disposed.error, isA<ReverbConfigurationException>());
        expect(rig.connector.requests, hasLength(1));
      });
    });

    test('with nothing to reconnect it completes at once without opening a socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final outcome = Outcome(rig.client.reconnect());
        async.flushMicrotasks();
        expect(outcome.succeeded, isTrue);
        expect(rig.connector.requests, isEmpty);
      });
    });

    test('two calls share the outcome and leave one socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        rig.client.subscribe(_news);
        async.flushMicrotasks();
        rig.connector.hold = true;

        final first = Outcome(rig.client.reconnect());
        async.flushMicrotasks();
        final second = Outcome(rig.client.reconnect());
        async.flushMicrotasks();
        expect(rig.connector.pendingCount, 1, reason: 'the first attempt was abandoned');
        rig.connector.completePending();
        async.flushMicrotasks();

        expect(first.succeeded, isTrue);
        expect(second.succeeded, isTrue);
        expect(rig.connector.openSockets, hasLength(1));
      });
    });

    test('an unawaited reconnect that fails is not an uncaught error', () {
      final uncaught = <Object>[];
      runZonedGuarded(() {
        fakeAsync((FakeAsync async) {
          final rig = ClientRig()..connector.failNext(StateError('offline'), times: 6);
          rig.client.subscribe(_news);
          async.elapse(const Duration(seconds: 10));
          unawaited(rig.client.reconnect());
          async.elapse(const Duration(seconds: 10));
          rig.client.pause();
          unawaited(rig.client.reconnect());
          async.flushMicrotasks();
        });
      }, (Object error, StackTrace stackTrace) => uncaught.add(error));
      expect(uncaught, isEmpty);
    });
  });
}
