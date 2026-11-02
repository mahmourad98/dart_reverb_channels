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
}
