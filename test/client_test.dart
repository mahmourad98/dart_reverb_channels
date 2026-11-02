import 'dart:async';
import 'dart:io';

import 'package:dart_reverb_channels/testing.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

const String _users = 'private-users.1';
const String _other = 'private-users.2';

void main() {
  group('reference counting', () {
    test('two subscriptions share one subscribe frame; cancelling one leaves the other receiving', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final first = Probe(rig.client.subscribe(_users));
        final second = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        expect(rig.connector.requests, hasLength(1));
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(1));
        expect(first.succeeded && second.succeeded, isTrue);

        rig.connector.socket.event(_users, 'order.shipped', <String, Object?>{'order_id': 9});
        async.flushMicrotasks();
        expect(first.events.single.data, <String, dynamic>{'order_id': 9});
        expect(second.events.single.name, 'order.shipped');

        unawaited(first.subscription.cancel());
        rig.connector.socket.event(_users, 'order.cancelled', <String, Object?>{'order_id': 9});
        async.flushMicrotasks();

        expect(first.events, hasLength(1));
        expect(first.eventsDone, isTrue);
        expect(second.events, hasLength(2));
        expect(rig.unsubscribes(rig.connector.socket, _users), isEmpty);
        expect(rig.connector.socket.isClosed, isFalse);
      });
    });

    test('the last cancel sends one unsubscribe frame, then closes the socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();

        final socket = rig.connector.socket;
        expect(rig.unsubscribes(socket, _users), hasLength(1));
        expect(socket.sentRaw.last, contains(PusherEvents.unsubscribe));
        expect(socket.closedByClient, isTrue);
        expect(rig.client.state, ReverbConnectionState.disconnected);
      });
    });

    test('cancelling twice, or after closeAll, does nothing', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final kept = Probe(rig.client.subscribe(_users));
        final cancelled = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        unawaited(cancelled.subscription.cancel());
        unawaited(cancelled.subscription.cancel());
        async.flushMicrotasks();
        expect(rig.unsubscribes(rig.connector.socket, _users), isEmpty);
        expect(kept.subscription.isCancelled, isFalse);

        unawaited(rig.client.closeAll());
        unawaited(kept.subscription.cancel());
        async.flushMicrotasks();
        expect(kept.eventsDone, isTrue);
        expect(rig.connector.requests, hasLength(1));
      });
    });

    test('cancel while connecting: no subscribe frame, the upgrade is aborted, ready is cancelled', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.hold = true;
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(rig.connector.pendingCount, 1);

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();

        expect(rig.connector.requests.single.cancelToken.isCancelled, isTrue);
        expect(rig.connector.pendingCount, 0);
        expect(rig.connector.sockets, isEmpty);
        expect(probe.readyError, isA<ReverbCancelledException>());
        expect(probe.eventsDone, isTrue);
        expect(rig.client.state, ReverbConnectionState.disconnected);

        async.elapse(const Duration(seconds: 30));
        expect(rig.connector.requests, hasLength(1));
        expect(rig.reports, isEmpty);
      });
    });

    test('cancel while the handshake is pending closes the socket without a subscribe frame', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoHandshake: false);
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();
        rig.connector.socket.handshake();
        async.flushMicrotasks();

        expect(rig.connector.socket.closedByClient, isTrue);
        expect(rig.connector.socket.sentNamed(PusherEvents.subscribe), isEmpty);
        expect(rig.authorizer.requests, isEmpty);
      });
    });

    test('cancel while authorization is in flight drops its result and sends no subscribe frame', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..authorizer.hold = true;
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(rig.authorizer.pendingCount, 1);

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();
        expect(rig.authorizer.cancelled, hasLength(1));

        rig.authorizer.completePending();
        async.flushMicrotasks();

        expect(rig.connector.socket.sentNamed(PusherEvents.subscribe), isEmpty);
        expect(rig.connector.socket.closedByClient, isTrue);
        expect(rig.reports, isEmpty);
      });
    });

    test('the last cancel and a new subscribe in the same turn leave exactly one live socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final old = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        unawaited(old.subscription.cancel());
        final fresh = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        expect(rig.connector.openSockets, hasLength(1));
        expect(rig.connector.sockets.first.closedByClient, isTrue);
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(1));
        expect(fresh.succeeded, isTrue);
      });
    });

    test('two channels subscribed in the same microtask open one socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final a = Probe(rig.client.subscribe(_users));
        final b = Probe(rig.client.subscribe('public-feed'));
        async.flushMicrotasks();

        expect(rig.connector.requests, hasLength(1));
        expect(a.succeeded && b.succeeded, isTrue);
        expect(rig.connector.socket.sentNamed(PusherEvents.subscribe), hasLength(2));
      });
    });
  });

  group('connection cycle', () {
    test('a refused connection gets exactly three attempts, 1 s then 2 s apart, then fails ready', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(const SocketException('Connection refused'), times: 10);
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(1));

        async.elapse(const Duration(milliseconds: 999));
        expect(rig.connector.requests, hasLength(1));
        async.elapse(const Duration(milliseconds: 1));
        expect(rig.connector.requests, hasLength(2));
        async.elapse(const Duration(milliseconds: 1999));
        expect(rig.connector.requests, hasLength(2));
        expect(probe.readyDone, isFalse);
        async.elapse(const Duration(milliseconds: 1));
        expect(rig.connector.requests, hasLength(3));

        expect(probe.readyError, isA<ReverbConnectionException>());
        expect((probe.readyError! as ReverbConnectionException).rootCause, isA<SocketException>());
        expect(rig.reports.map((ReverbErrorReport r) => r.willRetry), <bool>[true, true, false]);
        expect(rig.reports.map((ReverbErrorReport r) => r.attempt), <int>[1, 2, 3]);
        expect(rig.client.state, ReverbConnectionState.unavailable);
        expect(probe.eventsDone, isFalse, reason: 'a failed subscription stays open and silent');

        async.elapse(const Duration(minutes: 1));
        expect(rig.connector.requests, hasLength(3));
      });
    });

    test('after the attempts run out, the next subscribe starts a fresh three-attempt cycle', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(const SocketException('down'), times: 3);
        final first = Probe(rig.client.subscribe(_users));
        async.elapse(const Duration(seconds: 5));
        expect(first.readyError, isNotNull);
        expect(rig.connector.requests, hasLength(3));

        rig.connector.failNext(const SocketException('still down'), times: 2);
        final second = Probe(rig.client.subscribe(_other));
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(4));
        async.elapse(const Duration(seconds: 3));
        expect(rig.connector.requests, hasLength(6));

        expect(second.succeeded, isTrue);
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(1), reason: 'live channels join the new socket');
        expect(rig.subscribes(rig.connector.socket, _other), hasLength(1));
      });
    });

    test('an attempt that never upgrades times out after 10 s and is aborted', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.hold = true;
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        async.elapse(const Duration(seconds: 10));
        expect(rig.connector.requests.first.cancelToken.isCancelled, isTrue);
        expect(rig.reports.single.error, isA<ReverbTimeoutException>());
        expect(rig.reports.single.willRetry, isTrue);

        async.elapse(const Duration(seconds: 1));
        expect(rig.connector.requests, hasLength(2));
        rig.connector.completePending().handshake(socketId: '9.9');
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);
        expect(rig.authorizer.requests.single.socketId, '9.9');
      });
    });

    test('closeAll during a back-off stops the attempts, completes every ready and ends every stream', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..connector.failNext(const SocketException('down'));
        final a = Probe(rig.client.subscribe(_users));
        final b = Probe(rig.client.subscribe(_other));
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(1));

        unawaited(rig.client.closeAll());
        async.elapse(const Duration(seconds: 30));

        expect(rig.connector.requests, hasLength(1));
        expect(a.readyError, isA<ReverbCancelledException>());
        expect(b.readyError, isA<ReverbCancelledException>());
        expect(a.eventsDone && b.eventsDone, isTrue);
        expect(rig.client.channels, isEmpty);
      });
    });

    test('pusher:error 4009 before the handshake is fatal: one attempt, socket closed, ready fails', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoHandshake: false);
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket.protocolError(4009, 'Connection is unauthorized');
        async.flushMicrotasks();

        expect(probe.readyError, isA<ReverbProtocolException>().having((e) => e.code, 'code', 4009));
        expect(rig.connector.socket.closedByClient, isTrue);
        expect(rig.client.state, ReverbConnectionState.failed);
        async.elapse(const Duration(seconds: 30));
        expect(rig.connector.requests, hasLength(1));
      });
    });

    test('a malformed frame before the handshake is reported and dropped', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoHandshake: false);
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket.emitRaw('{not json');
        async.flushMicrotasks();
        expect(rig.reports.single.stage, ReverbErrorStage.frame);
        expect(rig.reports.single.error, isA<ReverbFormatException>());

        rig.connector.socket.handshake();
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);
      });
    });
  });
}
