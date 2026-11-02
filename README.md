# dart_reverb_channels

A [Laravel Reverb](https://reverb.laravel.com) client for Flutter, written in pure Dart over the
Pusher protocol (version 7). Its transport and authorization can be adapted to other
Pusher-compatible servers.

This guide describes version **1.0.0**, matching [the `v1.0.0` tag](https://github.com/mahmourad98/dart_reverb_channels/tree/v1.0.0).

- **One shared socket.** Every screen that listens to a channel gets its own handle; the channel
  is left when the last handle is cancelled, and the socket closes when the last channel goes.
- **Bounded, predictable retries.** You choose how many attempts the connection and each channel
  get, how long each may take and how long to wait between them.
- **Automatic reconnect.** If an open socket drops, it reconnects and re-subscribes every live
  channel with a fresh authorization. A keep-alive ping detects dead sockets.
- **Everything is pluggable.** The socket transport (your own `HttpClient`, certificate pinning,
  `Origin` header), channel authorization (your HTTP stack and tokens), payload decryption, error
  reporting and logging are all injected.
- **Public, private, presence and encrypted channels**, client events (whispers), app lifecycle
  handling and in-memory test doubles.

## Contents

1. [Requirements](#requirements)
2. [Installation](#installation)
3. [Quick start](#quick-start)

## Requirements

- Flutter `>=3.32.0 <4.0.0` with Dart `>=3.8.0 <4.0.0`, as declared in [pubspec.yaml](pubspec.yaml).
  The core uses Dart APIs, but this package depends on the Flutter SDK; use Flutter tooling.
- A reachable Laravel Reverb server, or a server implementing the Pusher protocol 7 features you use.
  The Laravel setup below uses the Laravel 11+ application structure.
- For private, presence or encrypted channels, an HTTP authorization endpoint and your app's
  authentication flow. Public channels do not need an authorizer.

## Installation

Add the GitHub dependency to your app's `pubspec.yaml`. This pins the 1.0.0 release tag; update
`ref` deliberately when upgrading:

```yaml
dependencies:
  dart_reverb_channels:
    git:
      url: https://github.com/mahmourad98/dart_reverb_channels.git
      ref: v1.0.0
```

Then fetch it:

```bash
flutter pub get
```

The package has four entry points:

| Import | Use it for |
|---|---|
| `package:dart_reverb_channels/io.dart` | Android, iOS, macOS, Windows and Linux apps. Everything in the core, plus `IOWebSocketConnector`. |
| `package:dart_reverb_channels/dart_reverb_channels.dart` | The platform-neutral core (also for Flutter web). |
| `package:dart_reverb_channels/flutter.dart` | The core plus `ReverbLifecycleBinding`, which pauses the socket in the background. |
| `package:dart_reverb_channels/testing.dart` | The core plus in-memory fakes for your tests. |

## Quick start

This complete function listens to a public channel for 30 seconds, then cleans up. Replace the
host, app key, channel and event name with your server's values:

```dart
import 'package:dart_reverb_channels/io.dart';

Future<void> main() async {
  final reverb = ReverbClient(
    options: const ReverbOptions(
      host: 'ws.example.com',        // your REVERB_HOST
      appKey: 'your-reverb-app-key', // your REVERB_APP_KEY
    ),
    connector: const IOWebSocketConnector(),
    onError: (report) => print(report),
  );

  final subscription = reverb.subscribe('news');
  final listener = subscription.on('news.published').listen((ReverbEvent event) {
    print(event.data); // a Map<String, dynamic>, already decoded
  });

  try {
    await subscription.ready;
    await Future<void>.delayed(const Duration(seconds: 30));
  } finally {
    await listener.cancel();
    await subscription.cancel();
    await reverb.dispose();
  }
}
```

The socket opens on the first `subscribe` and closes when the last subscription is cancelled. You
never call `connect()`.
