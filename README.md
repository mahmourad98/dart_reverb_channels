# dart_reverb_channels

A [Laravel Reverb](https://reverb.laravel.com) client for Flutter, written in pure Dart over the
Pusher protocol (version 7). Its transport and authorization can be adapted to other
Pusher-compatible servers.

This guide describes version **1.1.0**, the current [`main` branch](https://github.com/mahmourad98/dart_reverb_channels/tree/main).

- **One shared socket.** Every screen that listens to a channel gets its own handle; the channel
  is left when the last handle is cancelled, and the socket closes when the last channel goes.
- **Bounded, predictable retries.** You choose how many attempts the connection and each channel
  get, how long each may take and how long to wait between them.
- **Automatic reconnect.** If an open socket drops, it reconnects and re-subscribes every live
  channel with a fresh authorization. A keep-alive ping detects dead sockets, `reconnect()` retries
  on demand, and every subscription reports when its channel drops and comes back.
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
5. [Event names](#event-names)
6. [Presence channels and client events](#presence-channels-and-client-events)
7. [Encrypted channels](#encrypted-channels)
8. [Configuration reference](#configuration-reference)
9. [How it behaves](#how-it-behaves)
10. [Errors](#errors)
11. [Testing your code](#testing-your-code)
12. [Platform notes](#platform-notes)
13. [Troubleshooting](#troubleshooting)
14. [Running the example](#running-the-example)
15. [Developing this package](#developing-this-package)
16. [Credits and license](#credits-and-license)

## Requirements

- Flutter `>=3.32.0 <4.0.0` with Dart `>=3.8.0 <4.0.0`, as declared in [pubspec.yaml](pubspec.yaml).
  The core uses Dart APIs, but this package depends on the Flutter SDK; use Flutter tooling.
- A reachable Laravel Reverb server, or a server implementing the Pusher protocol 7 features you use.
  The Laravel setup below uses the Laravel 11+ application structure.
- For private, presence or encrypted channels, an HTTP authorization endpoint and your app's
  authentication flow. Public channels do not need an authorizer.

## Installation

Add the GitHub dependency to your app's `pubspec.yaml`. It follows the `main` branch; your
`pubspec.lock` records the exact commit your app builds against:

```yaml
dependencies:
  dart_reverb_channels:
    git: https://github.com/mahmourad98/dart_reverb_channels.git
```

Then fetch it:

```bash
flutter pub get
```

To move to the latest `main` later:

```bash
flutter pub upgrade dart_reverb_channels
```

To stay on one commit instead, use the `git:` map form with `url:` and `ref: <commit sha>`.

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

The socket opens with the first subscription. To open it earlier (for example right after sign-in,
so a later screen only waits for its channel's authorization), call `reverb.connect()`. It completes
once connected and keeps the socket open with no channel until a subscription takes it over;
`closeAll()` ends that hold, and a failed connection ends it with the error `connect()` returns.

#### Authorization request and response

`HttpChannelAuthorizer` POSTs `socket_id` and the full `channel_name`, with JSON request encoding
by default. For form encoding, pass `body: ChannelAuthorizationBody.form`. It expects HTTP 200
with a raw JSON object containing a non-empty `auth` string:

```json
{"auth": "your-reverb-app-key:signature"}
```

Presence responses also contain `channel_data` (usually a JSON string containing `user_id` and
`user_info`); encrypted responses require `shared_secret`. If your endpoint wraps the response
(for example in your API's `data` envelope), read it with `responseParser` (below). Headers default
to `Accept: application/json` and the body's content type; your header builder can override them.

`clientFactory` must return a fresh `http.Client` for each request: the authorizer closes it on
completion or cancellation. Do not return a shared app-wide client.

`responseParser` reads a 200 response into a `ChannelAuthorization` when the body is not the raw
object above:

```dart
HttpChannelAuthorizer(
  endpoint: Uri.parse('https://api.example.com/broadcasting/auth'),
  responseParser: (http.Response response) {
    final envelope = jsonDecode(response.body) as Map<String, dynamic>;
    return ChannelAuthorization.fromJson(envelope['data'] as Map<String, dynamic>);
  },
)
```

Other statuses never reach the parser. A `ReverbAuthorizationException` thrown by the parser passes
through unchanged; any other error becomes a malformed response, which is not retried.

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
Neither indicates current channel readiness: `subscription.status` does (see step 5).
`reverb.state == ReverbConnectionState.connected` means the socket handshake succeeded; individual
channels may still be authorizing.

To show connection status, start with `reverb.state` and listen to `reverb.states` for subsequent
changes (`connecting`, `connected`, `reconnecting`, `unavailable`, `failed`, `paused`,
`disconnected`). The broadcast stream does not replay the current state. Cancel that listener
when its owner ends.

### 5. Follow a channel after `ready` (when you must not miss an event)

`ready` reports only the first outcome. After that, a subscription's `status` follows its channel:

| `status` | Meaning |
|---|---|
| `pending` | Not subscribed yet (connecting, authorizing, retrying). |
| `subscribed` | Events are delivered. |
| `resubscribing` | It was subscribed; the socket dropped (or `reconnect()` / `pause()` ran) and the channel is being subscribed again. |
| `failed` | The attempts ran out or a failure was fatal; `subscription.error` says why. It recovers when the channel is subscribed again. |
| `cancelled` | `cancel()` or `closeAll()` ended it. |

Events published while a channel is not subscribed are lost (Reverb does not replay them). When
`statusChanges` reports `subscribed` after `resubscribing` or `failed`, re-read what you may have
missed:

```dart
ReverbSubscriptionStatus previous = subscription.status;
final statusListener = subscription.statusChanges.listen((ReverbSubscriptionStatus status) {
  final cameBack = status == ReverbSubscriptionStatus.subscribed &&
      (previous == ReverbSubscriptionStatus.resubscribing || previous == ReverbSubscriptionStatus.failed);
  if (cameBack) refreshOrderFromApi(); // events published during the gap did not arrive
  if (status == ReverbSubscriptionStatus.failed) showOfflineBanner(subscription.error);
  previous = status;
});
```

`statusChanges` does not replay the current status; read `subscription.status` first. It is done
after `cancel()` or `closeAll()`; cancel the listener when its owner ends.

### 6. Reconnect on demand

When a cycle runs out of attempts (`unavailable`), nothing retries on its own until the next
`subscribe`. `reconnect()` drops the socket, connects again at once with a fresh set of attempts and
re-subscribes every live channel, failed ones included. Call it when connectivity returns or from a
retry button:

```dart
// With package:connectivity_plus:
Connectivity().onConnectivityChanged.listen((List<ConnectivityResult> results) {
  if (!results.contains(ConnectivityResult.none)) reverb.reconnect();
});

// Or await it behind a retry button:
try {
  await reverb.reconnect(); // completes once connected
} on ReverbException catch (error) {
  // The new cycle ran out of attempts too.
}
```

Awaiting it is optional; an unawaited failure is never reported as an uncaught error. While the
client is paused it does nothing (`resume()` reconnects).

### 7. Pause in the background (recommended on mobile)

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

### 8. Log out and dispose

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

## Event names

Events arrive under their wire name:

- With `broadcastAs()`, the name you return (`order.shipped`).
- Without it, the event's class name (`App\Events\OrderShipped`).

`laravelEventName` resolves names the way Laravel Echo does:

```dart
subscription.on(laravelEventName('OrderShipped'));  // App\Events\OrderShipped
subscription.on(laravelEventName('.order.shipped')); // order.shipped
```

`on(name)` matches exactly; it does not apply `laravelEventName` automatically. You can pass a
custom `namespace: r'Domain\Events'` to the helper, or a leading backslash for a fully qualified
name. `subscription.events` listens to all delivered channel events, including presence changes.

`event.data` is always a `Map<String, dynamic>`: objects pass through, JSON strings are decoded
once, a raw null or empty string becomes `{}`, and scalar/list values are wrapped in `{'data': value}`.
`event.rawData` preserves the original wire payload (ciphertext on an encrypted channel).
`event.channel` / `event.name` identify the source; `event.userId` is set when the server includes
a sender id. Protocol bookkeeping such as `pusher_internal:subscription_succeeded` is consumed
internally rather than delivered as an application event.

## Presence channels and client events

Presence channels (`presence-…`) track who is in them. Return the member's data from your
`routes/channels.php` callback (return `false` to deny), and broadcast with
`new PresenceChannel('chat.'.$roomId)`. The membership check belongs to your application:

```php
Broadcast::channel('chat.{roomId}', function ($user, $roomId) {
    if (! $user->rooms()->whereKey($roomId)->exists()) {
        return false;
    }

    return ['name' => $user->name];
});
```

This assumes your user model has a `rooms()` relationship. Read the roster after `ready`:

```dart
final room = reverb.subscribe('presence-chat.1');
room.on(PusherEvents.memberAdded).listen((event) => print('joined: ${event.data['user_id']}'));
room.on(PusherEvents.memberRemoved).listen((event) => print('left: ${event.data['user_id']}'));

await room.ready;
print(room.members.keys); // member ids
print(room.me?.id);       // this user
```

`members` is an unmodifiable snapshot keyed by string ids; `PresenceMember.info` contains the
server's member data. The roster clears on a disconnect/pause and is rebuilt after authorization
on the next socket. `me` identifies this client when authorization includes `user_id`.
Keep and cancel the presence listener handles as in the screen example.

Client events (whispers) go to other clients on a private or presence channel. Check your server's
client-event policy. Current Reverb configuration uses
`'accept_client_events_from' => 'members'` (the default) to permit subscribed senders; see the
[Reverb app configuration](https://github.com/laravel/reverb/blob/main/config/reverb.php).
Use a JSON-encodable payload:

```dart
room.trigger('typing', {'name': 'Ada'});              // sent as client-typing
room.on('client-typing').listen((event) => print(event.data));
```

`trigger` adds `client-` unless already present. It returns `false` while disconnected,
paused or not yet subscribed, and throws `StateError` after cancellation or on public/encrypted
channels. `true` means the client attempted to send; there is no server delivery acknowledgement.
Use your API for durable actions.

## Encrypted channels

`private-encrypted-…` channels need both a `ChannelAuthorizer` and a `ReverbChannelDecrypter`.
The server must publish encrypted payloads and return a base64 `shared_secret` from authorization;
a channel name alone does not enable encryption on your backend.

The package ships no cipher. For [Pusher's encryption scheme](https://pusher.com/docs/channels/using_channels/encrypted-channels/),
base64-decode the key, nonce and ciphertext, authenticate/decrypt using NaCl secretbox, and return
the decrypted JSON string or decoded object. This is an adapter sketch: `yourSecretboxOpen` is
your implementation, not a function supplied by the package.

```dart
final class SecretboxDecrypter implements ReverbChannelDecrypter {
  @override
  Object? decrypt({
    required String channel,
    required String event,
    required String sharedSecret,
    required Object? data,
  }) {
    final payload = data! as Map<String, dynamic>; // {ciphertext, nonce}, base64
    return yourSecretboxOpen(payload['ciphertext'], payload['nonce'], sharedSecret); // JSON string
  }
}

final reverb = ReverbClient(
  options: options,
  connector: const IOWebSocketConnector(), // import io.dart on native platforms
  authorizer: authorizer,
  decrypter: SecretboxDecrypter(),
);
final encrypted = reverb.subscribe('private-encrypted-orders.7');
```

An event that fails to decrypt is dropped and reported through `onError` as a
`ReverbDecryptionException`. There is no automatic key refresh/retry on decryption failure;
fresh authorization happens on a new subscription attempt or socket. Client events are not
supported on encrypted channels.

## Configuration reference

`ReverbOptions` is immutable; derive variants with `copyWith`. Null arguments to `copyWith`
preserve existing values, so construct new options to clear `port`, `socketPath`, `headersBuilder`
or `retryIf`. A client's `options` are final; create another client to change its configuration.
The constructor validates options and can throw `ArgumentError` before any subscription.

| Option | Default | Meaning |
|---|---|---|
| `host`, `appKey` | required | Server host (no scheme) and `REVERB_APP_KEY`. |
| `port` | 443 (80 without TLS) | Server port. |
| `useTls` | `true` | `wss` when true, `ws` when false (local development). |
| `pathPrefix` | `''` | Reverb's `REVERB_SERVER_PATH`, for a server behind a proxy path (`/ws` → `/ws/app/{key}`). |
| `socketPath` | none | Replaces the whole socket path; include any required app key yourself. |
| `protocolVersion` | `7` | `protocol` query parameter. |
| `clientName` | `dart_reverb_channels` | `client` query parameter. |
| `clientVersion` | `1.1.0` | `version` query parameter; exported as `dartReverbChannelsVersion`. |
| `queryParameters` | `{}` | Extra URL parameters; duplicate keys override `protocol`, `client` or `version`. |
| `headers` | `{}` | Headers for the WebSocket upgrade, such as `Origin`. |
| `headersBuilder` | none | Headers built per connection attempt, merged over `headers`. |
| `connectTimeout` | 10 s | One connection attempt: socket, upgrade and handshake. |
| `subscriptionTimeout` | 10 s | One channel attempt: authorization and the server's acknowledgement. |
| `connectRetry` | 3 attempts, 1 s then 2 s | Attempts per connection cycle, including the first. |
| `subscribeRetry` | 3 attempts, 1 s then 2 s | Attempts per channel. |
| `retryIf` | `error.isRetryable` | Decides which failures are retried. |
| `activityTimeout` | 120 s | The longest silence before a keep-alive ping; the server's shorter `activity_timeout` wins. |
| `pongTimeout` | 30 s | How long a ping may go unanswered before the socket counts as dead. |
| `reconnect` | `true` | Reconnect and re-subscribe after an open socket drops. |
| `reconnectBackoff` | 1 s | Wait before reconnecting after a 4100–4199 close (server asked to back off). |
| `closeWhenIdle` | `true` | Close the socket once no channel is left. |
| `idleTimeout` | 0 s | With `closeWhenIdle`, how long an idle socket stays open first. |

`options.socketUri` shows the resulting URL, normally
`wss://ws.example.com:443/app/your-reverb-app-key?protocol=7&client=dart_reverb_channels&version=1.1.0`.
Use `pathPrefix: '/ws'` for `/ws/app/{key}`; `socketPath` replaces that entire path.

For an app on hosted Pusher Channels, `ReverbOptions.pusherCluster(cluster: 'eu', appKey: '…')`
sets the host to `ws-eu.pusher.com` with TLS on port 443; adjust the rest with `copyWith`.

### Retry policies

`RetryPolicy.none` allows one attempt. `RetryPolicy(maxAttempts: 3, delays: [...])` counts the
first attempt as 1; the last delay repeats, and an empty delay list retries immediately. Set
`maxAttempts: null` for no attempt limit. `jitter` adds a random fraction of the base delay.

For example, give each connection cycle five attempts, without retrying channel authorization:

```dart
final options = ReverbOptions(
  host: 'ws.example.com',
  appKey: 'your-reverb-app-key',
  connectRetry: RetryPolicy.exponential(maxAttempts: 5),
  subscribeRetry: RetryPolicy.none,
);
```

`RetryPolicy.exponential()` defaults to unlimited attempts, doubling from 1 s to a 30 s base
delay, with up to 25% extra jitter. Pass `maxAttempts` to keep it bounded.
`retryIf` replaces the default retry predicate for connection/channel attempts; fatal protocol
codes 4000–4099 still prohibit reconnecting. The initial reconnect after an established socket
drops is controlled by `reconnect` and the close code, not by `retryIf`.

### Transport and client hooks

Native transport (`IOWebSocketConnector`) options: `httpClientFactory` (your `HttpClient`, for
example with a pinned `SecurityContext`), `pingInterval` (WebSocket-level pings for aggressive
proxies) and `compression`. Its factory must return a fresh `HttpClient` per connection attempt;
the connector closes it after the upgrade or on cancellation.

The `ReverbClient` constructor also accepts `authorizer`, `decrypter`, `onError`, `onLog` and an
optional `math.Random` (`random`) for repeatable retry jitter in tests. For a custom transport,
implement `ReverbConnector.connect(ReverbConnectRequest)` and return a `ReverbSocket`. Honor the
request's cancellation token, expose the socket's close code/reason, and close abandoned sockets.
`WebSocketChannelConnector(factory: ...)` / `WebSocketChannelSocket` adapt an existing
`web_socket_channel` transport.

### Client and subscription reference

| Member | Behavior |
|---|---|
| `reverb.subscribe(channel)` | Returns a new owner handle; shares the wire subscription with other owners. |
| `reverb.state` / `states` | Current connection state / broadcast stream of changes. |
| `reverb.errors` / `events` | Broadcast streams of every handled failure (the reports `onError` receives) / every event delivered to any handle, once per event. Both only observe. |
| `reverb.socketId` | Current socket id, or `null` without an established socket. Changes on reconnect. |
| `reverb.channels` | Unmodifiable snapshot of channel names with live handles, including pending/failed ones. |
| `reverb.pause()` / `resume()` | Close and keep handles / reconnect after an explicit pause. |
| `reverb.connect()` | Open the socket before the first subscription and hold it open until one takes it over; completes once connected. |
| `reverb.reconnect()` | Drop the socket and connect again at once, re-subscribing every live channel (failed ones included); completes once connected. |
| `reverb.closeAll()` / `dispose()` | End every handle / permanently release the client as well. |
| `subscription.channel` | Full wire channel name. |
| `subscription.ready` / `isReady` | One-time readiness result / whether that result succeeded. |
| `subscription.status` / `statusChanges` | Current status (`pending`, `subscribed`, `resubscribing`, `failed`, `cancelled`) / broadcast stream of changes. |
| `subscription.error` | The failure behind `failed`; `null` in any other status. |
| `subscription.events` / `on(name)` | All delivered events / exact-name filter, both broadcast streams. |
| `subscription.members` / `me` | Presence roster / own member; empty or `null` when unavailable. |
| `subscription.subscriptionCount` | Connections on the channel as last reported by `pusher:subscription_count` (also delivered as that event); `null` until reported. Hosted Pusher sends it when enabled; Reverb does not. |
| `subscription.trigger(name, data)` | Attempt a client event on a private/presence channel. |
| `subscription.cancel()` / `isCancelled` | Release this handle / whether it has ended. |

For Laravel's `toOthers()`, send the current `reverb.socketId` as `X-Socket-ID` on the **HTTP
request that triggers the broadcast**. This package does not add it to your API requests.
Read it at request time, since it changes after reconnecting. The event must use Laravel's
`InteractsWithSockets` trait; see [Only to Others](https://laravel.com/docs/12.x/broadcasting#only-to-others).

## How it behaves

1. `subscribe` never throws. Problems such as an invalid channel name or a private channel without
   an authorizer come back as a subscription whose `ready` fails with
   `ReverbConfigurationException`.
2. `ready` completes exactly once, with the first outcome. Everything after it (a drop, a
   re-subscription, a failure after `ready`) shows in `status` and `statusChanges`.
3. `events` is a broadcast stream. It ends only after `cancel()` or `closeAll()`, and delivers
   nothing after cancellation. The stream does not buffer or replay events for listeners added later.
   Failures are delivered through `ready`, `status` / `error` and `onError`, rather than stream errors.
4. A connection cycle gets `connectRetry` attempts. If they all fail, every pending `ready` fails,
   every subscription reads `failed` and the socket stays closed; the next `subscribe`, or
   `reconnect()`, starts a fresh cycle. A channel that exhausts its own retries stays `failed` until
   a new handle for that channel, a new socket or `reconnect()` starts fresh attempts. Retained
   handles receive again when the channel recovers, and `statusChanges` reports it; an
   already-failed `ready` remains failed.
5. If an open socket drops, a new cycle starts at once (after `reconnectBackoff` for 4100–4199) and
   every live channel is re-subscribed with fresh authorization; its subscriptions read
   `resubscribing` until then. Close codes 4000–4099 mean "do not reconnect".
6. After `activity_timeout` of silence the client sends `pusher:ping`. No answer within
   `pongTimeout` counts as a drop. The `pinging` debug line is this keep-alive, not a problem.
7. Events published while the socket is down are lost: Reverb does not replay them. If you must
   not miss an outcome, re-check through your API when `statusChanges` reports the channel back
   (see [step 5](#5-follow-a-channel-after-ready-when-you-must-not-miss-an-event)), and bound your
   wait with a timeout.
8. `cancel`, `closeAll` and a new socket make any work still in flight for the old state (an
   authorization request, a late acknowledgement, a retry timer) do nothing.

## Errors

`ready` failures and `onError` reports carry a `ReverbException`. It is a sealed class, so you can
`switch` over it:

| Type | Meaning | Retried by default |
|---|---|---|
| `ReverbConnectionException` | The socket could not open or dropped. `cause` is the raw error (`SocketException`, TLS failure…), `rootCause` without wrappers. | Yes |
| `ReverbTimeoutException` | An attempt ran past its deadline. | Yes |
| `ReverbProtocolException` | `pusher:error` or a 4000–4299 close `code`. | Any code outside 4000–4099 (including unknown/null); see below |
| `ReverbHeartbeatException` | No pong in time. | Yes |
| `ReverbAuthorizationException` | The auth endpoint failed. `statusCode`, `body`, `malformedResponse`. | 5xx, 429, 408 and transport failures |
| `ReverbSubscriptionException` | The server refused the subscription (`pusher:subscription_error`). | 5xx and 429 |
| `ReverbFormatException`, `ReverbDecryptionException` | A frame or payload could not be read. | No |
| `ReverbCancelledException` | Cancelled before ready. | No |
| `ReverbConfigurationException` | The request cannot work with this setup. | No |

`onError` reports runtime failures as a `ReverbErrorReport` with the `stage`
(`connect`, `connection`, `subscribe`, `frame`, `protocol`), `channel`, `attempt` and `willRetry`.
The same reports arrive on `reverb.errors`, a broadcast stream for any number of listeners (for
example a logger and an offline banner).
Configuration failures returned by `subscribe` are exposed through `ready` and warning logs;
cancelling a pending subscription fails `ready` without an `onError` report. Do not rely solely on
`onError` to detect an invalid setup.

A `pusher:error` on an already-established socket is reported with stage `protocol` and changes
no connection/channel state by itself. A close frame drives reconnect behavior. Failed channel
reauthorization after a reconnect is reported through `onError` even though `ready` may have
succeeded earlier.

On native platforms, import `io.dart` to use `isTlsFailure`. For example, set this on
`ReverbOptions` to decline retries after a TLS failure:

```dart
retryIf: (ReverbException error) => !isTlsFailure(error) && error.isRetryable,
```

## Testing your code

`package:dart_reverb_channels/testing.dart` replaces the network with in-memory fakes, so your
tests drive the server side directly:

```dart
import 'package:dart_reverb_channels/testing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('shows shipped orders', () async {
    final connector = FakeReverbConnector(autoHandshake: true, autoAcknowledge: true);
    final reverb = ReverbClient(
      options: const ReverbOptions(host: 'test', appKey: 'key'),
      connector: connector,
      authorizer: FakeChannelAuthorizer(),
    );

    final subscription = reverb.subscribe('private-users.1');
    await subscription.ready;

    final next = subscription.on('order.shipped').first;
    connector.socket.event('private-users.1', 'order.shipped', {'order_id': 7});

    expect((await next).data['order_id'], 7);
    await reverb.dispose();
  });
}
```

`FakeReverbConnector` can also hold or fail connection attempts (`hold`, `failNext`) and records
every request; `FakeReverbSocket` can send `refuse`, `protocolError` and `serverClose`, and records
what the client sent (`sentNamed`). `FakeChannelAuthorizer` can hold, fail or script
authorizations.

## Platform notes

### Android, iOS and desktop

Use `IOWebSocketConnector`. It sends upgrade headers and can abort pending upgrades. As described
in [Flutter's networking setup](https://docs.flutter.dev/data-and-backend/networking):

- Android: declare `<uses-permission android:name="android.permission.INTERNET" />` inside the
  `<manifest>` in `android/app/src/main/AndroidManifest.xml`, including release builds.
- macOS: add `<key>com.apple.security.network.client</key><true/>` inside the entitlement
  dictionaries in both `macos/Runner/DebugProfile.entitlements` and
  `macos/Runner/Release.entitlements`.
- Use trusted HTTPS/WSS endpoints in production. If local HTTP is blocked, configure a
  development-only exception under your platform's network policy:
  [Android](https://developer.android.com/training/articles/security-config#CleartextTrafficPermitted)
  / [Apple](https://developer.apple.com/documentation/bundleresources/information-property-list/nsapptransportsecurity).

### Flutter web

Import the core and use the default connector; do not import `io.dart` into a web entry point:

```dart
import 'package:dart_reverb_channels/dart_reverb_channels.dart';

final reverb = ReverbClient(
  options: const ReverbOptions(host: 'ws.example.com', appKey: 'your-reverb-app-key'),
);
```

Browsers cannot send custom WebSocket upgrade headers; the default connector ignores `headers`
and sends the browser's own origin. Allow that origin in Reverb's `allowed_origins`. For
cross-origin HTTP authorization, configure CORS on your API for the auth endpoint, including
`Authorization` / `Content-Type` headers. An HTTPS app needs a secure `wss` endpoint.

For cookie-based authentication, supply a browser HTTP client with the appropriate credential
behavior and handle your backend's CSRF flow. The default authorizer does not bootstrap a Sanctum
session or manage CSRF tokens for you.

### Custom certificates

Socket TLS trust and authorization HTTP TLS trust are separate. On native platforms, configure
both factories with your `SecurityContext`:

```dart
import 'dart:io';

import 'package:dart_reverb_channels/io.dart';
import 'package:http/io_client.dart';

ReverbClient createTrustedClient(SecurityContext context, Uri authEndpoint) {
  return ReverbClient(
    options: const ReverbOptions(host: 'ws.example.com', appKey: 'your-reverb-app-key'),
    connector: IOWebSocketConnector(
      httpClientFactory: () => HttpClient(context: context),
    ),
    authorizer: HttpChannelAuthorizer(
      endpoint: authEndpoint,
      clientFactory: () => IOClient(HttpClient(context: context)),
      // Add your authorization headers here for authenticated channels.
    ),
  );
}
```

If importing `package:http` in your app, declare `http` as a direct dependency
(`flutter pub add http`). Supply your app's token headers in addition to the certificate trust.

### Local development

A default `php artisan reverb:start` accepts unencrypted connections on port 8080. For a direct
connection, use `useTls: false, port: 8080`:

| App target | Host to reach a server on your development machine |
|---|---|
| Android emulator | `10.0.2.2` |
| iOS simulator / desktop on the same machine | `localhost` |
| Physical phone | Your machine's reachable LAN IP or development DNS name |
| Flutter web | A browser-reachable host, with origin/CORS and TLS configured as above |

The authorization endpoint must also use an address reachable from the device. Reverb's listening
address `0.0.0.0` is not a client destination.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `ReverbProtocolException` 4001 | Wrong `appKey`. |
| `ReverbProtocolException` 4009 before connecting | The server refused the connection, usually `Origin` not in Reverb's `allowed_origins`. Set `headers: {'Origin': …}`. |
| `ready` fails with `ReverbTimeoutException` for a private channel | Reverb answers a bad signature with `pusher:error` 4009 without a channel, so it surfaces as a timeout. Check that the auth endpoint and Reverb share the same app key and secret. |
| `ReverbAuthorizationException` 401/403 | The auth endpoint did not accept the token, or `routes/channels.php` returned false. |
| Subscribed, but no events | The broadcast queue worker is not running, the event name does not match (see [Event names](#event-names)), or the event goes to another channel. |
| `pinging` lines on a quiet channel | Normal keep-alive after the shorter of the server's `activity_timeout` and your `activityTimeout`. |
| `HandshakeException` / `CertificateException` | TLS trust: the device does not trust the server certificate, or your custom `SecurityContext` lacks it. |
| Authorization 200 followed by `malformedResponse` | Return the raw `auth` object, not an HTML login page or a wrapped API response. Encrypted channels also need `shared_secret`. |
| Authorization 404/419, or browser CORS error | Check the exact route/prefix, middleware, CSRF/credential strategy and API CORS policy. |
| `unavailable` / `failed` persists | Automatic attempts ended or the server refused the connection. Fix the cause, cancel the old handle and subscribe again. |
| A release build cannot connect | Check Android internet permission, macOS release entitlements, device-reachable hosts, proxy upgrades and trusted TLS. |

Use `php artisan reverb:start --debug` to inspect traffic and `php artisan route:list --path=broadcasting`
to verify the auth route. After server configuration changes, refresh cached Laravel configuration
as appropriate and restart the Reverb process.

## Running the example

[example/main.dart](example/main.dart) is a Flutter entry point, not a standalone app with platform
runners. Create a Flutter app, add the dependency from [Installation](#installation), and copy that
file into the app's `lib/main.dart`:

```bash
flutter create reverb_demo
cd reverb_demo
# Add the dependency and copy example/main.dart into lib/main.dart.
flutter pub get
flutter run \
  --dart-define=REVERB_HOST=ws.example.com \
  --dart-define=REVERB_APP_KEY=your-reverb-app-key \
  --dart-define=REVERB_CHANNEL=news
```

The example lists all channel events and reports initial readiness. It reads only those three
defines and uses TLS on port 443. For local port 8080, edit its `ReverbOptions` to set
`useTls: false, port: 8080`. It uses the native connector; for web, remove the `io.dart` import and the connector
argument. Add an authorizer before using a private or presence channel.

## Developing this package

From a checkout, run the same checks as [CI](.github/workflows/ci.yml):

```bash
flutter pub get
dart format --output=none --set-exit-if-changed lib test example
flutter analyze
flutter test
```

CI resolves both `flutter pub upgrade` and `flutter pub downgrade` to check the declared dependency
ranges. The tests include fake-socket behavior, local WebSocket transport tests, HTTP authorization,
and [the README's test example](test/readme_example_test.dart). They do not require a running Laravel
server; verify your own backend, permissions and deployment with an integration smoke test.

## Credits and license

The design draws on [laravel_reverb](https://pub.dev/packages/laravel_reverb) (reference-counted
channels, lifecycle handling) and [pusher_reverb_flutter](https://pub.dev/packages/pusher_reverb_flutter)
(custom transports and paths, encrypted channels), and follows the
[Pusher channels protocol](https://pusher.com/docs/channels/library_auth_reference/pusher-websockets-protocol/).

Released under the [MIT License](LICENSE).
