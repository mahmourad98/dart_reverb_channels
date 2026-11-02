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
