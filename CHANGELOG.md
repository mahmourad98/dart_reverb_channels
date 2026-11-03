## 1.3.0

- Every answer to a subscribe now reaches the subscribe it answers, which removes the trade-off
  1.2.1 documented. The protocol has no request ids, so while a subscribe awaits its answer the
  client sends a `pusher:ping` before its next subscribe or client event, and the server's in-order
  replies tell the answers apart. As a result:
  - a late answer to a subscribe released during its back-off no longer completes the `ready` of
    the next subscription to that channel;
  - a subscribe Reverb refuses without naming the channel (a bad signature) no longer holds up the
    next subscription to that channel for about 11 s;
  - a late refusal of a timed-out subscribe no longer fails its retry.
- A released channel now unsubscribes whenever the server may still subscribe it, including while
  its retry authorizes and after its attempts ran out by timeouts.
- README: behaviour rule 9 explains the matching and its limit under Reverb's rate limiting.

## 1.2.1

- A dropped socket now honors `ReverbOptions.retryIf`: when it refuses the cause (for example a TLS
  failure on an established socket), the client no longer reconnects once anyway. The drop is
  reported with `willRetry: false`, subscriptions read `failed` and `reconnect()` brings them back.
  With the default `retryIf` nothing changes: every cause of a drop is retryable.
- Tests for four cases the suite did not cover: a server ping without `data` (the frame Reverb
  sends), cancelling one of two channels while connecting, an error answering a released subscribe,
  and a no-channel 4009 answering a pending subscribe.

## 1.2.0

- `ReverbClient.connect()`: opens the socket before the first subscription and keeps it open with
  no channel until a subscription takes it over (or `closeAll`); completes once connected.
- `ReverbClient.errors` and `ReverbClient.events`: broadcast streams of every handled failure (the
  reports `onError` receives) and of every event delivered to a subscription, for any number of
  listeners.
- `HttpChannelAuthorizer(responseParser: …)`: reads authorization responses that are not the raw
  `{auth}` body, such as one wrapped in an API envelope.
- `ReverbOptions.pusherCluster(cluster:, appKey:)`: options for a hosted Pusher Channels app
  (`ws-{cluster}.pusher.com`, TLS on port 443).
- Subscription counts: `pusher_internal:subscription_count` updates
  `ReverbSubscription.subscriptionCount` and is delivered as `pusher:subscription_count`. Hosted
  Pusher sends it when the app enables it; Laravel Reverb does not.
- `ReverbClient.isDisposed`.

## 1.1.0

- `ReverbSubscription.status`, `statusChanges` and `error`: a subscription now reports what happens
  after `ready` (`pending`, `subscribed`, `resubscribing`, `failed`, `cancelled`). A `subscribed`
  after `resubscribing` or `failed` marks a gap in which events were not delivered.
- A refused re-subscription after a drop (for example an expired token) now shows on the
  subscription as `failed` with its error, not only in `onError`.
- A failed subscription whose channel is subscribed again (after a new connection cycle or another
  subscriber of the same channel) now says so through `statusChanges`; before, it started receiving
  events again without any signal.
- `ReverbClient.reconnect()`: drops the socket and connects again at once with a fresh set of
  attempts, re-subscribing every live channel, failed ones included. Completes once connected.
- README: documents status and `reconnect()`; the install snippet follows `main` instead of pinning
  a commit.

## 1.0.0

First release.

- `ReverbClient`: one shared socket with reference-counted channel subscriptions; the socket
  opens on the first subscription and closes when the last channel is released.
- Bounded, configurable retries (`RetryPolicy`) for the connection cycle and for each channel,
  with per-attempt deadlines and a `retryIf` override.
- Automatic reconnect and re-subscription with fresh authorization after a drop; keep-alive
  ping/pong with dead-socket detection; Pusher close-code handling (4000–4299).
- Public, private, presence and encrypted (bring your own decrypter) channels; client events.
- Pluggable transport (`IOWebSocketConnector`, `WebSocketChannelConnector`), authorization
  (`HttpChannelAuthorizer` or your own `ChannelAuthorizer`), error reports and logging.
- `ReverbLifecycleBinding` to pause in the background; `laravelEventName` for Echo-style names.
- In-memory test doubles in `package:dart_reverb_channels/testing.dart`.
