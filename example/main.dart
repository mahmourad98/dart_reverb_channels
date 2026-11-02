// A minimal app that listens to one public channel and lists its events.
//
// Run it inside a Flutter app that depends on dart_reverb_channels:
//
//   flutter run \
//     --dart-define=REVERB_HOST=ws.example.com \
//     --dart-define=REVERB_APP_KEY=your-reverb-app-key \
//     --dart-define=REVERB_CHANNEL=news
import 'dart:async';

import 'package:dart_reverb_channels/flutter.dart';
import 'package:dart_reverb_channels/io.dart';
import 'package:flutter/material.dart';

const String _host = String.fromEnvironment('REVERB_HOST', defaultValue: 'localhost');
const String _appKey = String.fromEnvironment('REVERB_APP_KEY', defaultValue: 'app-key');
const String _channel = String.fromEnvironment('REVERB_CHANNEL', defaultValue: 'news');

/// One client for the whole app.
final ReverbClient reverb = ReverbClient(
  options: const ReverbOptions(host: _host, appKey: _appKey),
  connector: const IOWebSocketConnector(),
  onError: (ReverbErrorReport report) => debugPrint('reverb: $report'),
);

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // Close the socket in the background, reconnect in the foreground.
  ReverbLifecycleBinding(reverb).attach();
  runApp(const MaterialApp(home: EventsPage()));
}

/// Shows the connection state and every event on [_channel].
class EventsPage extends StatefulWidget {
  /// The page.
  const EventsPage({super.key});

  @override
  State<EventsPage> createState() => _EventsPageState();
}

class _EventsPageState extends State<EventsPage> {
  late final ReverbSubscription _subscription;
  late final StreamSubscription<ReverbEvent> _events;
  final List<ReverbEvent> _received = <ReverbEvent>[];
  String _status = 'subscribing…';

  @override
  void initState() {
    super.initState();
    _subscription = reverb.subscribe(_channel);
    _events = _subscription.events.listen((ReverbEvent event) => setState(() => _received.insert(0, event)));
    _subscription.ready.then(
      (_) => _setStatus('subscribed to $_channel'),
      onError: (Object error) => _setStatus('unavailable: $error'),
    );
  }

  void _setStatus(String status) {
    if (mounted) setState(() => _status = status);
  }

  @override
  void dispose() {
    unawaited(_events.cancel());
    unawaited(_subscription.cancel());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text(_status)),
      body: ListView.builder(
        itemCount: _received.length,
        itemBuilder: (BuildContext context, int index) {
          final event = _received[index];
          return ListTile(title: Text(event.name), subtitle: Text('${event.data}'));
        },
      ),
    );
  }
}
