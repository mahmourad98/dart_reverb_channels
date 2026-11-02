import 'dart:async';
import 'dart:math' as math;

import 'package:dart_reverb_channels/testing.dart';

/// Options the client tests share: the default policy (3 attempts, 1 s then 2 s,
/// 10 s per attempt) and an Origin header.
const ReverbOptions testOptions = ReverbOptions(
  host: 'reverb.test',
  appKey: 'app-key',
  headers: <String, String>{'Origin': 'https://app.test'},
);

/// A client over in-memory edges, recording every error report.
final class ClientRig {
  ClientRig({
    ReverbOptions options = testOptions,
    bool autoHandshake = true,
    bool autoAcknowledge = true,
    bool withAuthorizer = true,
  }) : connector = FakeReverbConnector(autoHandshake: autoHandshake, autoAcknowledge: autoAcknowledge),
       authorizer = FakeChannelAuthorizer() {
    client = ReverbClient(
      options: options,
      connector: connector,
      authorizer: withAuthorizer ? authorizer : null,
      onError: reports.add,
      onLog: logs.add,
      random: math.Random(1),
    );
  }

  final FakeReverbConnector connector;
  final FakeChannelAuthorizer authorizer;
  final List<ReverbErrorReport> reports = <ReverbErrorReport>[];
  final List<ReverbLogRecord> logs = <ReverbLogRecord>[];
  late final ReverbClient client;

  /// Subscribe frames sent on [socket] for [channel].
  List<Map<String, dynamic>> subscribes(FakeReverbSocket socket, String channel) => socket
      .sentNamed(PusherEvents.subscribe)
      .where((Map<String, dynamic> f) => (f['data'] as Map)['channel'] == channel)
      .toList();

  /// Unsubscribe frames sent on [socket] for [channel].
  List<Map<String, dynamic>> unsubscribes(FakeReverbSocket socket, String channel) => socket
      .sentNamed(PusherEvents.unsubscribe)
      .where((Map<String, dynamic> f) => (f['data'] as Map)['channel'] == channel)
      .toList();
}

/// Observes a subscription's `ready` and events without awaiting.
final class Probe {
  Probe(this.subscription) {
    subscription.ready.then<void>(
      (_) {
        readyDone = true;
      },
      onError: (Object error) {
        readyDone = true;
        readyError = error;
      },
    );
    _listener = subscription.events.listen(events.add, onDone: () => eventsDone = true);
  }

  final ReverbSubscription subscription;
  final List<ReverbEvent> events = <ReverbEvent>[];
  late final StreamSubscription<ReverbEvent> _listener;
  bool readyDone = false;
  Object? readyError;
  bool eventsDone = false;

  bool get succeeded => readyDone && readyError == null;

  Future<void> dispose() => _listener.cancel();
}
