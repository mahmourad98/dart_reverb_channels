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
