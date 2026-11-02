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
4. [Step-by-step integration](#step-by-step-integration)

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

## Step-by-step integration

### 1. Prepare the Laravel server

Use [Laravel's Reverb installation](https://laravel.com/docs/12.x/broadcasting#reverb):

```bash
php artisan install:broadcasting --reverb
```

This writes the Reverb settings to `.env`. The client needs the public values:

```dotenv
BROADCAST_CONNECTION=reverb
REVERB_APP_KEY=your-reverb-app-key
REVERB_HOST=ws.example.com
REVERB_PORT=443
REVERB_SCHEME=https
```

Map `REVERB_APP_KEY` to `appKey`, `REVERB_HOST` to `host`, `REVERB_PORT` to `port` and
`REVERB_SCHEME=https` to `useTls: true`. The host is a name or IP, without a scheme or path.
The app needs a device-reachable address; Laravel may use a different internal address.

`REVERB_APP_ID` and `REVERB_APP_SECRET` stay on the server. Never put the secret in the app.

Run Reverb, and a queue worker when using a queued broadcast event, in separate terminals:

```bash
php artisan reverb:start
```

```bash
php artisan queue:work
```

`REVERB_SERVER_HOST` / `REVERB_SERVER_PORT` control Reverb's listening address (by default
`0.0.0.0:8080`); `REVERB_HOST` / `REVERB_PORT` tell Laravel where to publish. Production setups
typically terminate TLS at a proxy on port 443 and forward WebSocket upgrades to Reverb.
See [Reverb server configuration](https://laravel.com/docs/12.x/reverb#running-the-server).

For a direct local connection, use `REVERB_SCHEME=http`, `REVERB_PORT=8080` and the Dart options
`useTls: false, port: 8080`. See [Platform notes](#platform-notes) for device addresses.

Private and presence channels need an authorization endpoint. With token-authenticated APIs
(for example Sanctum), register the broadcasting routes behind your API guard. In Laravel 11+
`bootstrap/app.php`:

```php
->withBroadcasting(
    __DIR__.'/../routes/channels.php',
    ['prefix' => 'api', 'middleware' => ['api', 'auth:sanctum']],
)
```

Remove the `channels:` entry from `withRouting(...)` when using this `withBroadcasting(...)`
setup, as shown in [Sanctum's broadcasting guide](https://laravel.com/docs/12.x/sanctum#authorizing-private-broadcast-channels).
Sanctum must already be configured and the user signed in. This serves `POST /api/broadcasting/auth`.

Declare who may join a channel in `routes/channels.php`:

```php
use Illuminate\Support\Facades\Broadcast;

Broadcast::channel('users.{id}', function ($user, $id) {
    return (int) $user->id === (int) $id;
});
```

For user 7 the wire channel is `private-users.7`; substitute the id rather than sending `{id}`.
Laravel adds prefixes for `PrivateChannel` / `PresenceChannel`; this client takes the full wire
name and adds no channel prefix:

| Laravel broadcast channel | Dart `subscribe` argument | Authorization |
|---|---|---|
| `new Channel('news')` | `'news'` | None |
| `new PrivateChannel('users.7')` | `'private-users.7'` | Required |
| `new PresenceChannel('chat.1')` | `'presence-chat.1'` | Required; returns member data |

Channel names must be 1–200 characters, using only letters, digits and `_-=@,.;` (including `-`).
`validateChannelName(name)` returns a reason when invalid, otherwise `null`.

For example, create `app/Events/OrderShipped.php` (this assumes your app has an `Order` model):

```php
<?php

namespace App\Events;

use App\Models\Order;
use Illuminate\Broadcasting\PrivateChannel;
use Illuminate\Contracts\Broadcasting\ShouldBroadcast;
use Illuminate\Queue\SerializesModels;

class OrderShipped implements ShouldBroadcast
{
    use SerializesModels;

    public function __construct(public Order $order) {}

    public function broadcastOn(): array
    {
        return [new PrivateChannel('users.'.$this->order->user_id)];
    }

    public function broadcastAs(): string
    {
        return 'order.shipped';
    }

    public function broadcastWith(): array
    {
        return ['order_id' => $this->order->id];
    }
}
```

Dispatch it from your application after the order is shipped:

```php
event(new \App\Events\OrderShipped($order));
```

The subscriber `private-users.7` receives `order.shipped` with `{'order_id': ...}` when the
order belongs to user 7. `ShouldBroadcast` queues the broadcast (unless your queue connection is
synchronous); `ShouldBroadcastNow` sends synchronously. Listen before dispatching the event.

### 2. Create one client for the app

Create one client per server in your app's dependency-injection setup and share it across screens.
This factory accepts your app's token reader; call it once with, for example,
`createReverbClient(tokenStore.readAccessToken)`. The token store belongs to your app.

```dart
import 'package:dart_reverb_channels/io.dart';
import 'package:flutter/foundation.dart';

ReverbClient createReverbClient(Future<String?> Function() readAccessToken) {
  return ReverbClient(
    options: const ReverbOptions(
      host: 'ws.example.com',
      appKey: 'your-reverb-app-key',
      // Match a host allowed by your Reverb app's allowed_origins.
      headers: {'Origin': 'https://example.com'},
    ),
    connector: const IOWebSocketConnector(),
    authorizer: HttpChannelAuthorizer(
      endpoint: Uri.parse('https://api.example.com/api/broadcasting/auth'),
      headers: (ChannelAuthorizationRequest request) async {
        final token = await readAccessToken();
        if (token == null || token.isEmpty) {
          throw ReverbAuthorizationException(
            'Not signed in',
            channel: request.channel,
            statusCode: 401,
          );
        }
        return {'Authorization': 'Bearer $token'};
      },
    ),
    onError: (ReverbErrorReport report) => debugPrint('Reverb: $report'),
    onLog: (ReverbLogRecord record) => debugPrint('Reverb: $record'),
  );
}
```

The header builder runs on every authorization attempt, including after a reconnect. The package
does not sign users in, refresh tokens or automatically retry a 401/403. Read the current token in
the builder and handle refresh in your app's HTTP/authentication layer. WebSocket `headers`
configure the upgrade; `HttpChannelAuthorizer.headers` configure the separate HTTP auth request.

Keep the host and public app key in your build configuration (for example `--dart-define`).
Configure Reverb's `allowed_origins` to accept your app: native apps send no `Origin` unless you
provide one; browsers supply their own origin.

#### Authorization request and response

`HttpChannelAuthorizer` POSTs `socket_id` and the full `channel_name`, with JSON request encoding
by default. For form encoding, pass `body: ChannelAuthorizationBody.form`. It expects HTTP 200
with a raw JSON object containing a non-empty `auth` string:

```json
{"auth": "your-reverb-app-key:signature"}
```

Presence responses also contain `channel_data` (usually a JSON string containing `user_id` and
`user_info`); encrypted responses require `shared_secret`. Do not wrap the response in your API's
usual `data` envelope. Headers default to `Accept: application/json` and the body's content type;
your header builder can override them.

`clientFactory` must return a fresh `http.Client` for each request: the authorizer closes it on
completion or cancellation. Do not return a shared app-wide client.

#### Using your own HTTP stack (optional)

Implement `ChannelAuthorizer` to reuse Dio, interceptors and token refresh. For this example, add
Dio to your app (`flutter pub add dio`); it is not a dependency of this package. Supply a Dio client
with your API base URL and authentication interceptors:

```dart
import 'dart:async';

import 'package:dart_reverb_channels/dart_reverb_channels.dart';
import 'package:dio/dio.dart' as dio;

final class DioChannelAuthorizer implements ChannelAuthorizer {
  DioChannelAuthorizer(this.client);

  final dio.Dio client;

  @override
  Future<ChannelAuthorization> authorize(ChannelAuthorizationRequest request) async {
    final cancel = dio.CancelToken();
    if (request.cancelToken.isCancelled) {
      throw const ReverbCancelledException('Authorization cancelled');
    }
    unawaited(request.cancelToken.whenCancelled.then((_) => cancel.cancel()));

    try {
      final response = await client.post<Map<String, dynamic>>(
        '/api/broadcasting/auth',
        data: {'socket_id': request.socketId, 'channel_name': request.channel},
        cancelToken: cancel,
      );
      final data = response.data;
      if (data == null) throw const FormatException('Empty authorization response');
      return ChannelAuthorization.fromJson(data);
    } on FormatException catch (error) {
      throw ReverbAuthorizationException(
        'Malformed authorization response',
        channel: request.channel,
        malformedResponse: true,
        cause: error,
      );
    } on dio.DioException catch (error) {
      throw ReverbAuthorizationException(
        'Authorization failed',
        channel: request.channel,
        statusCode: error.response?.statusCode,
        cause: error,
      );
    }
  }
}
```

### 3. Subscribe where you need events

Subscribe when a screen (or a controller, or a repository) starts, and cancel when it ends.
The widget below receives the shared client explicitly:

```dart
import 'dart:async';

import 'package:dart_reverb_channels/dart_reverb_channels.dart';
import 'package:flutter/material.dart';

class OrdersScreen extends StatefulWidget {
  const OrdersScreen({super.key, required this.reverb, required this.userId});

  final ReverbClient reverb;
  final int userId;

  @override
  State<OrdersScreen> createState() => _OrdersScreenState();
}

class _OrdersScreenState extends State<OrdersScreen> {
  late final ReverbSubscription _subscription;
  late final StreamSubscription<ReverbEvent> _listener;
  int? _lastShippedOrder;

  @override
  void initState() {
    super.initState();
    _subscription = widget.reverb.subscribe('private-users.${widget.userId}');
    _listener = _subscription.on('order.shipped').listen((ReverbEvent event) {
      if (mounted) {
        setState(() => _lastShippedOrder = event.data['order_id'] as int?);
      }
    });
  }

  @override
  void dispose() {
    unawaited(_listener.cancel());
    unawaited(_subscription.cancel());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => Text('Last shipped order: ${_lastShippedOrder ?? '-'}');
}
```

Two screens subscribing to the same channel share one subscription on the server; cancelling one
does not affect the other. Cancelling only the `StreamSubscription` stops that listener; you
must also cancel the `ReverbSubscription` to release the channel. Attach listeners before awaiting
`ready` so you do not miss events between acknowledgement and listener registration.

This widget assumes `userId` and `reverb` stay the same during its lifetime. If either changes,
cancel the old handles and subscribe again in `didUpdateWidget`, or give the screen a new key.

### 4. Know when a channel is ready (optional)

`subscription.ready` completes when the server has accepted the subscription, or fails with the
reason it could not:

```dart
try {
  await subscription.ready;
  // Subscribed: events published from now on will arrive.
} on ReverbAuthorizationException catch (error) {
  // The auth endpoint refused (error.statusCode), e.g. the user may not join.
} on ReverbCancelledException {
  // The subscription was cancelled first.
} on ReverbException catch (error) {
  // Network, timeout or protocol problem after all attempts.
}
```

You do not have to await `ready`: events flow as soon as the channel is subscribed either way, and
an un-awaited failure is never reported as an uncaught error.

`ready` completes once; `isReady` stays true after that success, even during a later disconnect.
Neither indicates current channel readiness. `reverb.state == ReverbConnectionState.connected` means the socket handshake
succeeded; individual channels may still be authorizing.

To show connection status, start with `reverb.state` and listen to `reverb.states` for subsequent
changes (`connecting`, `connected`, `reconnecting`, `unavailable`, `failed`, `paused`,
`disconnected`). The broadcast stream does not replay the current state. Cancel that listener
when its owner ends.

### 5. Pause in the background (recommended on mobile)

iOS silently kills sockets of suspended apps. Let the client close the socket in the background and
reconnect in the foreground:

```dart
import 'package:dart_reverb_channels/flutter.dart';
import 'package:flutter/widgets.dart';

// In main(), before attaching observers or calling runApp:
WidgetsFlutterBinding.ensureInitialized();
final lifecycle = ReverbLifecycleBinding(reverb)..attach();
// On teardown: lifecycle.detach();
```

The binding pauses on `paused` / `detached` and resumes on `resumed`; it ignores `inactive` /
`hidden`. You can also call `reverb.pause()` / `reverb.resume()` yourself. Pending `ready` futures
keep waiting while paused.

Subscriptions survive the pause; their channels are re-subscribed on resume. Events published while
paused are not replayed (see [How it behaves](#how-it-behaves)). Detaching only removes the lifecycle
observer; dispose the client separately.

### 6. Log out and dispose

On logout, end every subscription so nothing re-authorizes with the old user's token:

```dart
await reverb.closeAll(); // the client stays usable for the next user
```

When the client itself is no longer needed:

```dart
lifecycle.detach(); // if you attached a lifecycle binding
await reverb.dispose();
```

Call `closeAll()` before switching users and release any old screen-owned handles. `dispose()`
also closes the state stream and permanently rejects future subscriptions. Both methods are
idempotent. `closeAll()` keeps a paused client paused; call `resume()` when it should run again.
