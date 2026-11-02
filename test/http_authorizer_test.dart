import 'dart:async';
import 'dart:convert';

import 'package:dart_reverb_channels/dart_reverb_channels.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

ChannelAuthorizationRequest _request([ReverbCancelToken? token]) => ChannelAuthorizationRequest(
  channel: 'private-users.1',
  socketId: '123.456',
  cancelToken: token ?? ReverbCancelToken(),
);

void main() {
  final endpoint = Uri.parse('https://api.test/broadcasting/auth');

  test('posts JSON with the per-request headers and reads the raw auth body', () async {
    late http.Request seen;
    final authorizer = HttpChannelAuthorizer(
      endpoint: endpoint,
      headers: (ChannelAuthorizationRequest request) => <String, String>{'Authorization': 'Bearer token'},
      clientFactory: () => MockClient((http.Request request) async {
        seen = request;
        return http.Response('{"auth":"key:signature","channel_data":{"user_id":1}}', 200);
      }),
    );

    final authorization = await authorizer.authorize(_request());

    expect(seen.method, 'POST');
    expect(seen.url, endpoint);
    expect(seen.headers['Authorization'], 'Bearer token');
    expect(seen.headers['Accept'], 'application/json');
    expect(seen.headers['Content-Type'], startsWith('application/json'));
    expect(jsonDecode(seen.body), <String, String>{'socket_id': '123.456', 'channel_name': 'private-users.1'});
    expect(authorization.auth, 'key:signature');
    expect(authorization.channelData, '{"user_id":1}');
  });

  test('can post a form body, as pusher-js does', () async {
    late http.Request seen;
    final authorizer = HttpChannelAuthorizer(
      endpoint: endpoint,
      body: ChannelAuthorizationBody.form,
      clientFactory: () => MockClient((http.Request request) async {
        seen = request;
        return http.Response('{"auth":"a"}', 200);
      }),
    );
    await authorizer.authorize(_request());
    expect(seen.bodyFields, <String, String>{'socket_id': '123.456', 'channel_name': 'private-users.1'});
  });

  test('a non-200 status throws with the status and body', () async {
    final authorizer = HttpChannelAuthorizer(
      endpoint: endpoint,
      clientFactory: () => MockClient((_) async => http.Response('{"message":"Forbidden"}', 403)),
    );
    await expectLater(
      authorizer.authorize(_request()),
      throwsA(
        isA<ReverbAuthorizationException>()
            .having((e) => e.statusCode, 'statusCode', 403)
            .having((e) => e.isRetryable, 'isRetryable', isFalse),
      ),
    );
  });

  test('an unreadable 200 body is a malformed response, not retried', () async {
    for (final body in <String>['<html>', '{"auth":5}', '[]']) {
      final authorizer = HttpChannelAuthorizer(
        endpoint: endpoint,
        clientFactory: () => MockClient((_) async => http.Response(body, 200)),
      );
      await expectLater(
        authorizer.authorize(_request()),
        throwsA(
          isA<ReverbAuthorizationException>()
              .having((e) => e.malformedResponse, 'malformedResponse', isTrue)
              .having((e) => e.isRetryable, 'isRetryable', isFalse),
        ),
        reason: body,
      );
    }
  });

  test('a transport failure keeps its cause and is retryable', () async {
    final authorizer = HttpChannelAuthorizer(
      endpoint: endpoint,
      clientFactory: () => MockClient((_) async => throw http.ClientException('offline')),
    );
    await expectLater(
      authorizer.authorize(_request()),
      throwsA(
        isA<ReverbAuthorizationException>()
            .having((e) => e.statusCode, 'statusCode', isNull)
            .having((e) => e.rootCause, 'rootCause', isA<http.ClientException>())
            .having((e) => e.isRetryable, 'isRetryable', isTrue),
      ),
    );
  });

  test('a headers builder may refuse without sending a request', () async {
    var sent = false;
    final authorizer = HttpChannelAuthorizer(
      endpoint: endpoint,
      headers: (ChannelAuthorizationRequest request) =>
          throw ReverbAuthorizationException('No token', channel: request.channel, statusCode: 401),
      clientFactory: () => MockClient((_) async {
        sent = true;
        return http.Response('{"auth":"a"}', 200);
      }),
    );
    await expectLater(authorizer.authorize(_request()), throwsA(isA<ReverbAuthorizationException>()));
    expect(sent, isFalse);
  });

  test('cancelling closes the client, aborting the request, and every client is closed', () async {
    final clients = <_TrackingClient>[];
    final hold = Completer<http.StreamedResponse>();
    final authorizer = HttpChannelAuthorizer(
      endpoint: endpoint,
      clientFactory: () {
        final client = _TrackingClient(hold);
        clients.add(client);
        return client;
      },
    );

    final token = ReverbCancelToken();
    final pending = authorizer.authorize(_request(token));
    await Future<void>.delayed(Duration.zero);
    token.cancel(const ReverbCancelledException());
    await Future<void>.delayed(Duration.zero);

    expect(clients.single.closed, isTrue);
    hold.completeError(http.ClientException('Client closed'));
    await expectLater(pending, throwsA(isA<ReverbCancelledException>()));
  });
}

final class _TrackingClient extends http.BaseClient {
  _TrackingClient(this._response);

  final Completer<http.StreamedResponse> _response;
  bool closed = false;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) => _response.future;

  @override
  void close() => closed = true;
}
