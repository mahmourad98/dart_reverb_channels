import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import 'cancel_token.dart';
import 'exceptions.dart';

/// What an authorizer is asked to sign.
final class ChannelAuthorizationRequest {
  /// A request.
  const ChannelAuthorizationRequest({
    required this.channel,
    required this.socketId,
    required this.cancelToken,
  });

  /// The channel to authorize, with its prefix.
  final String channel;

  /// The socket the signature binds to.
  final String socketId;

  /// Cancelled when the result is no longer wanted (cancel, `closeAll`, a new
  /// socket, the attempt deadline). Abort the request on it.
  final ReverbCancelToken cancelToken;
}

/// The signature Laravel's broadcasting auth endpoint returns.
final class ChannelAuthorization {
  /// An authorization.
  const ChannelAuthorization({required this.auth, this.channelData, this.sharedSecret});

  /// Reads the endpoint's JSON body. Throws [FormatException] without a string
  /// `auth`.
  factory ChannelAuthorization.fromJson(Map<String, dynamic> json) {
    final auth = json['auth'];
    if (auth is! String || auth.isEmpty) {
      throw const FormatException('Authorization response has no auth string');
    }
    final channelData = json['channel_data'];
    final sharedSecret = json['shared_secret'];
    return ChannelAuthorization(
      auth: auth,
      channelData: channelData is String ? channelData : (channelData == null ? null : jsonEncode(channelData)),
      sharedSecret: sharedSecret is String ? sharedSecret : null,
    );
  }

  /// The `{appKey}:{signature}` string sent with `pusher:subscribe`.
  final String auth;

  /// The presence member data, for presence channels.
  final String? channelData;

  /// The base64 key of an encrypted channel.
  final String? sharedSecret;
}

/// Signs channel subscriptions. Implement it to reuse your own HTTP stack,
/// tokens and certificate trust; the client makes no requests itself.
///
/// Throw [ReverbAuthorizationException] for a rejection so the retry policy
/// can tell it from a transport failure; any other error counts as a
/// transport failure.
abstract interface class ChannelAuthorizer {
  /// Signs [request].
  Future<ChannelAuthorization> authorize(ChannelAuthorizationRequest request);
}

/// Builds the headers of one authorization request. Throw
/// [ReverbAuthorizationException] to refuse without sending it (for example a
/// missing access token).
typedef ChannelAuthorizationHeadersBuilder =
    FutureOr<Map<String, String>> Function(ChannelAuthorizationRequest request);

/// How [HttpChannelAuthorizer] encodes the request body.
enum ChannelAuthorizationBody {
  /// `application/json`: `{"socket_id": …, "channel_name": …}`.
  json,

  /// `application/x-www-form-urlencoded`, as pusher-js sends it.
  form,
}

/// The default authorizer: POSTs the socket id and channel name to an endpoint
/// such as Laravel's `/broadcasting/auth` and reads the raw `{auth}` body.
///
/// Each request gets its own client from `clientFactory`, closed when the
/// request ends. Cancellation closes it early, which aborts the request; the
/// abandoned result is ignored.
final class HttpChannelAuthorizer implements ChannelAuthorizer {
  /// An authorizer for [endpoint].
  HttpChannelAuthorizer({
    required this.endpoint,
    this.headers,
    this.body = ChannelAuthorizationBody.json,
    http.Client Function()? clientFactory,
  }) : _clientFactory = clientFactory ?? http.Client.new;

  /// The authorization endpoint.
  final Uri endpoint;

  /// Builds the per-request headers (tokens and the like).
  final ChannelAuthorizationHeadersBuilder? headers;

  /// The body encoding.
  final ChannelAuthorizationBody body;

  final http.Client Function() _clientFactory;

  @override
  Future<ChannelAuthorization> authorize(ChannelAuthorizationRequest request) async {
    final channel = request.channel;
    final extraHeaders = await headers?.call(request) ?? const <String, String>{};
    if (request.cancelToken.isCancelled) throw const ReverbCancelledException('Authorization cancelled');

    final client = _clientFactory();
    final cancelled = request.cancelToken.whenCancelled.then((_) => client.close());

    final http.Response response;
    try {
      response = await client.post(
        endpoint,
        headers: <String, String>{
          'Accept': 'application/json',
          'Content-Type': body == ChannelAuthorizationBody.json
              ? 'application/json'
              : 'application/x-www-form-urlencoded',
          ...extraHeaders,
        },
        body: body == ChannelAuthorizationBody.json
            ? jsonEncode(<String, String>{'socket_id': request.socketId, 'channel_name': channel})
            : <String, String>{'socket_id': request.socketId, 'channel_name': channel},
      );
    } catch (error, stackTrace) {
      if (request.cancelToken.isCancelled) throw const ReverbCancelledException('Authorization cancelled');
      throw ReverbAuthorizationException(
        'Authorization request failed: $error',
        channel: channel,
        cause: error,
        stackTrace: stackTrace,
      );
    } finally {
      client.close();
      unawaited(cancelled);
    }

    if (response.statusCode != 200) {
      throw ReverbAuthorizationException(
        'Authorization rejected with status ${response.statusCode}',
        channel: channel,
        statusCode: response.statusCode,
        body: response.body,
      );
    }

    try {
      final decoded = jsonDecode(response.body);
      if (decoded is! Map) throw const FormatException('Authorization response is not a JSON object');
      return ChannelAuthorization.fromJson(decoded.cast<String, dynamic>());
    } catch (error, stackTrace) {
      throw ReverbAuthorizationException(
        'Authorization response is unreadable',
        channel: channel,
        statusCode: response.statusCode,
        body: response.body,
        cause: error,
        stackTrace: stackTrace,
        malformedResponse: true,
      );
    }
  }
}
