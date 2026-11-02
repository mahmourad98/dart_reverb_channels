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
