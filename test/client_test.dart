import 'dart:async';
import 'dart:convert';
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

  group('reconnect', () {
    test('a drop while subscribed reconnects at once and resubscribes with the new socket id', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);

        rig.connector.socket.serverClose(1006);
        async.flushMicrotasks();

        expect(rig.connector.requests, hasLength(2));
        expect(rig.client.state, ReverbConnectionState.connected);
        final resubscribe = rig.subscribes(rig.connector.socket, _users).single;
        expect((resubscribe['data'] as Map)['auth'], 'key:$_users:2.2');
        expect(rig.authorizer.requests.map((ChannelAuthorizationRequest r) => r.socketId), <String>['1.1', '2.2']);
        expect(rig.reports.single.stage, ReverbErrorStage.connection);
        expect(rig.reports.single.willRetry, isTrue);

        rig.connector.socket.event(_users, 'order.shipped', <String, Object?>{'order_id': 4});
        async.flushMicrotasks();
        expect(probe.events.single.data['order_id'], 4);
      });
    });

    test('a reconnect cycle is bounded; afterwards live subscriptions stay open and silent', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.failNext(const SocketException('offline'), times: 3);
        rig.connector.socket.serverClose();
        async.elapse(const Duration(seconds: 10));

        expect(rig.connector.requests, hasLength(4));
        expect(rig.client.state, ReverbConnectionState.unavailable);
        expect(probe.succeeded, isTrue, reason: 'ready completes once');
        expect(probe.eventsDone, isFalse);
      });
    });

    test('close code 4100 waits 1 s before the first reattempt; 4200 reattempts at once', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket.serverClose(4100, 'Over capacity');
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(1));
        async.elapse(const Duration(milliseconds: 999));
        expect(rig.connector.requests, hasLength(1));
        async.elapse(const Duration(milliseconds: 1));
        expect(rig.connector.requests, hasLength(2));

        rig.connector.socket.serverClose(4200, 'Generic reconnect');
        async.flushMicrotasks();
        expect(rig.connector.requests, hasLength(3));
      });
    });

    test('close code 4001 on an established socket is fatal: no reconnect', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket.serverClose(4001, 'App does not exist');
        async.elapse(const Duration(seconds: 30));
        expect(rig.connector.requests, hasLength(1));
        expect(rig.client.state, ReverbConnectionState.failed);
        expect(rig.reports.single.willRetry, isFalse);
      });
    });

    test('a missing pong counts as a drop; any frame counts as an answer', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        final socket = rig.connector.socket;

        async.elapse(const Duration(seconds: 30));
        expect(socket.sentNamed(PusherEvents.ping), hasLength(1));
        socket.emit(<String, Object?>{'event': PusherEvents.pong});
        async.elapse(const Duration(seconds: 29));
        expect(socket.isClosed, isFalse);

        async.elapse(const Duration(seconds: 1));
        expect(socket.sentNamed(PusherEvents.ping), hasLength(2));
        async.elapse(const Duration(seconds: 30));
        expect(socket.closedByClient, isTrue);
        expect(rig.reports.last.error, isA<ReverbHeartbeatException>());
        expect(rig.connector.requests, hasLength(2));
      });
    });

    test('a server ping is answered with a pong', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket.emit(<String, Object?>{'event': PusherEvents.ping, 'data': <String, Object?>{}});
        async.flushMicrotasks();
        expect(rig.connector.socket.sentNamed(PusherEvents.pong), hasLength(1));
      });
    });
  });

  group('channel attempts', () {
    test('logout then login as another user never resubscribes the old channel', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..authorizer.hold = true;
        final old = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(rig.authorizer.pendingCount, 1);

        unawaited(rig.client.closeAll());
        final fresh = Probe(rig.client.subscribe(_other));
        async.flushMicrotasks();

        rig.authorizer.completePending();
        async.flushMicrotasks();
        rig.authorizer.completePending();
        async.flushMicrotasks();

        for (final socket in rig.connector.sockets) {
          expect(rig.subscribes(socket, _users), isEmpty);
        }
        expect(rig.subscribes(rig.connector.socket, _other), hasLength(1));
        expect(old.readyError, isA<ReverbCancelledException>());
        expect(fresh.succeeded, isTrue);
      });
    });

    test('a subscription error on one channel does not affect another', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final refused = Probe(rig.client.subscribe(_users));
        final accepted = Probe(rig.client.subscribe(_other));
        async.flushMicrotasks();

        rig.connector.socket.refuse(_users, status: 403);
        rig.connector.socket.acknowledge(_other);
        async.flushMicrotasks();

        expect(refused.readyError, isA<ReverbSubscriptionException>().having((e) => e.status, 'status', 403));
        expect(accepted.succeeded, isTrue);
        async.elapse(const Duration(seconds: 30));
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(1), reason: '4xx is not retried');
        expect(rig.connector.socket.isClosed, isFalse);
      });
    });

    test('a channel cancelled during its authorization and subscribed again uses only the new key', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..authorizer.hold = true;
        final live = Probe(rig.client.subscribe(_other));
        async.flushMicrotasks();
        rig.authorizer.completePending();
        async.flushMicrotasks();
        expect(live.succeeded, isTrue);

        final first = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        unawaited(first.subscription.cancel());
        final second = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(rig.authorizer.pendingCount, 2);

        rig.authorizer.completePending(const ChannelAuthorization(auth: 'key:old'));
        async.flushMicrotasks();
        expect(rig.subscribes(rig.connector.socket, _users), isEmpty);

        rig.authorizer.completePending(const ChannelAuthorization(auth: 'key:new'));
        async.flushMicrotasks();
        final frames = rig.subscribes(rig.connector.socket, _users);
        expect(frames, hasLength(1));
        expect((frames.single['data'] as Map)['auth'], 'key:new');
        expect(second.succeeded, isTrue);
        expect(rig.connector.sockets, hasLength(1));
      });
    });

    test('an acknowledgement owed to a released channel is drained before the new one is ready', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final keep = Probe(rig.client.subscribe(_other));
        final first = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        rig.connector.socket.acknowledge(_other);
        async.flushMicrotasks();
        expect(keep.succeeded, isTrue);

        unawaited(first.subscription.cancel());
        final second = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(2));

        rig.connector.socket.acknowledge(_users);
        async.flushMicrotasks();
        expect(second.readyDone, isFalse, reason: 'the first answer belongs to the released subscribe');

        rig.connector.socket.acknowledge(_users);
        async.flushMicrotasks();
        expect(second.succeeded, isTrue);
      });
    });

    test('pusher:error on an established socket is reported and changes nothing', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        rig.connector.socket.protocolError(4009, 'Invalid signature');
        async.flushMicrotasks();
        expect(rig.reports.single.stage, ReverbErrorStage.protocol);
        expect(rig.client.state, ReverbConnectionState.connected);

        rig.connector.socket.event(_users, 'order.shipped', '{"order_id":3}');
        async.flushMicrotasks();
        expect(probe.events.single.data, <String, dynamic>{'order_id': 3});
      });
    });

    test('a rejected authorization fails ready; the stream stays open and silent until cancel', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        rig.authorizer.handler = (ChannelAuthorizationRequest request) => throw ReverbAuthorizationException(
          'Forbidden',
          channel: request.channel,
          statusCode: 403,
        );
        final probe = Probe(rig.client.subscribe(_users));
        async.elapse(const Duration(seconds: 30));

        expect(probe.readyError, isA<ReverbAuthorizationException>());
        expect(rig.authorizer.requests, hasLength(1));
        expect(probe.eventsDone, isFalse);

        unawaited(probe.subscription.cancel());
        async.flushMicrotasks();
        expect(probe.eventsDone, isTrue);
      });
    });

    test('a 5xx authorization is retried with the subscribe policy', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        var calls = 0;
        rig.authorizer.handler = (ChannelAuthorizationRequest request) {
          calls++;
          if (calls < 3) {
            throw ReverbAuthorizationException('Down', channel: request.channel, statusCode: 503);
          }
          return ChannelAuthorization(auth: FakeChannelAuthorizer.signatureFor(request));
        };
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(calls, 1);
        async.elapse(const Duration(seconds: 1));
        expect(calls, 2);
        async.elapse(const Duration(seconds: 2));
        expect(calls, 3);
        expect(probe.succeeded, isTrue);
      });
    });

    test('authorization is aborted by cancel, by closeAll and by the attempt deadline', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig()..authorizer.hold = true;

        final byCancel = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        unawaited(byCancel.subscription.cancel());
        async.flushMicrotasks();
        expect(rig.authorizer.cancelled, hasLength(1));

        Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        unawaited(rig.client.closeAll());
        async.flushMicrotasks();
        expect(rig.authorizer.cancelled, hasLength(2));

        final byDeadline = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        async.elapse(const Duration(seconds: 10));
        expect(rig.authorizer.cancelled, hasLength(3));
        expect(rig.reports.last.error, isA<ReverbTimeoutException>());
        expect(rig.reports.last.willRetry, isTrue);

        for (final socket in rig.connector.sockets) {
          expect(socket.sentNamed(PusherEvents.subscribe), isEmpty);
        }
        expect(byDeadline.readyDone, isFalse);
      });
    });

    test('an unanswered subscribe times out and is retried on the same socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(autoAcknowledge: false);
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        async.elapse(const Duration(seconds: 11));
        expect(rig.subscribes(rig.connector.socket, _users), hasLength(2));
        rig.connector.socket.acknowledge(_users);
        async.flushMicrotasks();
        expect(probe.succeeded, isTrue);
        expect(rig.connector.sockets, hasLength(1));
      });
    });
  });

  group('configuration problems', () {
    test('an invalid name or a missing authorizer fail ready without touching the socket', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(withAuthorizer: false);
        final invalid = Probe(rig.client.subscribe('bad name'));
        final private = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();

        for (final probe in <Probe>[invalid, private]) {
          expect(probe.readyError, isA<ReverbConfigurationException>());
          expect(probe.eventsDone, isFalse);
        }
        expect(rig.connector.requests, isEmpty);
      });
    });

    test('a disposed client answers every subscribe with a failed subscription', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        unawaited(rig.client.dispose());
        async.flushMicrotasks();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        expect(probe.readyError, isA<ReverbConfigurationException>());
        expect(rig.connector.requests, isEmpty);
      });
    });

    test('an idle timeout keeps the socket open until it elapses', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig(options: testOptions.copyWith(idleTimeout: const Duration(seconds: 5)));
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        unawaited(probe.subscription.cancel());
        async.elapse(const Duration(seconds: 4));
        expect(rig.connector.socket.isClosed, isFalse);
        final again = Probe(rig.client.subscribe(_users));
        async.elapse(const Duration(seconds: 10));
        expect(rig.connector.sockets, hasLength(1));
        expect(again.succeeded, isTrue);
      });
    });
  });

  group('events', () {
    test('payloads are decoded once whether they arrive as JSON strings, objects or plain values', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final probe = Probe(rig.client.subscribe(_users));
        async.flushMicrotasks();
        final socket = rig.connector.socket;

        socket.event(_users, 'a', <String, Object?>{'order_id': '7'});
        socket.emit(<String, Object?>{
          'event': 'b',
          'channel': _users,
          'data': <String, Object?>{'x': 1},
        }, encodeData: false);
        socket.event(_users, 'c', 'hello');
        socket.emit(<String, Object?>{'event': 'd', 'channel': 'private-elsewhere', 'data': '{}'});
        async.flushMicrotasks();

        expect(probe.events.map((ReverbEvent e) => e.name), <String>['a', 'b', 'c']);
        expect(probe.events[0].data, <String, dynamic>{'order_id': '7'});
        expect(probe.events[0].rawData, jsonEncode(<String, Object?>{'order_id': '7'}));
        expect(probe.events[1].data, <String, dynamic>{'x': 1});
        expect(probe.events[2].data, <String, dynamic>{'data': 'hello'});
      });
    });

    test('on() filters by event name', () {
      fakeAsync((FakeAsync async) {
        final rig = ClientRig();
        final subscription = rig.client.subscribe(_users);
        final names = <String>[];
        subscription.on('wanted').listen((ReverbEvent e) => names.add(e.name));
        async.flushMicrotasks();

        rig.connector.socket
          ..event(_users, 'unwanted', <String, Object?>{})
          ..event(_users, 'wanted', <String, Object?>{});
        async.flushMicrotasks();
        expect(names, <String>['wanted']);
      });
    });
  });
}
