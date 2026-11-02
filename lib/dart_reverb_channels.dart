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

export 'src/authorizer.dart'
    show
        ChannelAuthorization,
        ChannelAuthorizationBody,
        ChannelAuthorizationHeadersBuilder,
        ChannelAuthorizationRequest,
        ChannelAuthorizer,
        HttpChannelAuthorizer;
export 'src/cancel_token.dart' show ReverbCancelToken;
export 'src/client.dart' show ReverbClient, ReverbErrorHandler, ReverbLogHandler, ReverbSubscription;
export 'src/connector.dart'
    show
        ReverbConnectRequest,
        ReverbConnector,
        ReverbSocket,
        WebSocketChannelConnector,
        WebSocketChannelFactory,
        WebSocketChannelSocket;
export 'src/event.dart'
    show
        PresenceMember,
        ReverbChannelDecrypter,
        ReverbConnectionState,
        ReverbErrorReport,
        ReverbErrorStage,
        ReverbEvent,
        ReverbLogLevel,
        ReverbLogRecord,
        ReverbSubscriptionStatus;
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
