/// A Pusher-protocol channels client for Laravel Reverb.
///
/// Combines reference-counted, generation-guarded subscriptions with bounded
/// connection and subscription retries, and keeps every edge pluggable: the
/// transport (`ReverbConnector`), channel authorization (`ChannelAuthorizer`),
/// payload decryption (`ReverbChannelDecrypter`), error reporting and logging.
///
/// Native hosts import `package:dart_reverb_channels/io.dart` for
/// `IOWebSocketConnector`; tests import `package:dart_reverb_channels/testing.dart`.
library;

export 'src/cancel_token.dart' show ReverbCancelToken;
export 'src/exceptions.dart';
export 'src/options.dart'
    show dartReverbChannelsVersion, ReverbHeadersBuilder, ReverbOptions, ReverbRetryPredicate, RetryPolicy;
export 'src/protocol.dart'
    show
        PusherEvents,
        PusherFrame,
        ReverbChannelKind,
        decodePusherData,
        encodePusherFrame,
        laravelEventName,
        validateChannelName;
