@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dart_reverb_channels/io.dart';
import 'package:flutter_test/flutter_test.dart';

/// Loopback checks of the real `dart:io` path: headers on the upgrade, the
/// injected `HttpClient`, unwrapped connect errors and aborts.
void main() {
  const fastRetry = RetryPolicy(delays: <Duration>[Duration(milliseconds: 10), Duration(milliseconds: 20)]);

  ReverbOptions optionsFor(
    int port, {
    bool useTls = false,
    RetryPolicy retry = fastRetry,
    ReverbRetryPredicate? retryIf,
  }) {
    return ReverbOptions(
      host: '127.0.0.1',
      port: port,
      appKey: 'app-key',
      useTls: useTls,
      headers: const <String, String>{'Origin': 'https://app.test'},
      connectTimeout: const Duration(seconds: 2),
      connectRetry: retry,
      retryIf: retryIf,
    );
  }

  test('the upgrade carries the Origin header over the injected HttpClient, and a channel subscribes', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    String? origin;
    String? path;
    server.listen((HttpRequest request) async {
      origin = request.headers.value('origin');
      path = request.uri.path;
      // The server closes with the test; the socket goes with it.
      // ignore: close_sinks
      final socket = await WebSocketTransformer.upgrade(request);
      socket.add(
        jsonEncode(<String, Object?>{
          'event': 'pusher:connection_established',
          'data': jsonEncode(<String, Object?>{'socket_id': '1.2', 'activity_timeout': 30}),
        }),
      );
      socket.listen((Object? raw) {
        final frame = jsonDecode(raw! as String) as Map<String, dynamic>;
        if (frame['event'] == 'pusher:subscribe') {
          final channel = (frame['data'] as Map)['channel'];
          socket.add(
            jsonEncode(<String, Object?>{
              'event': 'pusher_internal:subscription_succeeded',
              'channel': channel,
              'data': '{}',
            }),
          );
          socket.add(jsonEncode(<String, Object?>{'event': 'hello', 'channel': channel, 'data': '{"n":1}'}));
        }
      });
    });

    var clientsCreated = 0;
    final client = ReverbClient(
      options: optionsFor(server.port),
      connector: IOWebSocketConnector(
        httpClientFactory: () {
          clientsCreated++;
          return HttpClient();
        },
      ),
    );
    addTearDown(client.dispose);

    final subscription = client.subscribe('feed');
    final event = subscription.events.first;
    await subscription.ready;

    expect(origin, 'https://app.test');
    expect(path, '/app/app-key');
    expect(clientsCreated, 1);
    expect(client.socketId, '1.2');
    expect((await event).data, <String, dynamic>{'n': 1});
  });

  test('a refused connection gets exactly three attempts and reports the raw SocketException', () async {
    final probe = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = probe.port;
    await probe.close();

    final reports = <ReverbErrorReport>[];
    final client = ReverbClient(
      options: optionsFor(port),
      connector: const IOWebSocketConnector(),
      onError: reports.add,
    );
    addTearDown(client.dispose);

    final subscription = client.subscribe('feed');
    await expectLater(subscription.ready, throwsA(isA<ReverbConnectionException>()));

    expect(reports, hasLength(3));
    expect((reports.last.error as ReverbConnectionException).rootCause, isA<SocketException>());
  });

  test('a refused upgrade fails with the HTTP status and no escaped async error', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    server.listen((HttpRequest request) {
      request.response.statusCode = HttpStatus.forbidden;
      unawaited(request.response.close());
    });

    final client = ReverbClient(
      options: optionsFor(server.port, retry: RetryPolicy.none),
      connector: const IOWebSocketConnector(),
    );
    addTearDown(client.dispose);

    final subscription = client.subscribe('feed');
    final error = await subscription.ready.then<Object?>((_) => null, onError: (Object e) => e);

    expect(error, isA<ReverbConnectionException>());
    final root = (error! as ReverbConnectionException).rootCause;
    expect(root, isA<WebSocketException>().having((e) => e.httpStatusCode, 'httpStatusCode', 403));
  });

  test('closeAll aborts an upgrade the server never answers', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    final arrived = Completer<void>();
    server.listen((HttpRequest request) {
      if (!arrived.isCompleted) arrived.complete();
      // Never answer.
    });

    final client = ReverbClient(options: optionsFor(server.port), connector: const IOWebSocketConnector());
    addTearDown(client.dispose);
    final subscription = client.subscribe('feed');
    await arrived.future;

    await client.closeAll();
    await expectLater(subscription.ready, throwsA(isA<ReverbCancelledException>()));
  });

  test('wss against a plain TCP port is a TLS failure: one attempt when retryIf refuses TLS', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    server.listen((HttpRequest request) => unawaited(request.response.close()));

    final reports = <ReverbErrorReport>[];
    final client = ReverbClient(
      options: optionsFor(
        server.port,
        useTls: true,
        retryIf: (ReverbException error) => !isTlsFailure(error) && error.isRetryable,
      ),
      connector: const IOWebSocketConnector(),
      onError: reports.add,
    );
    addTearDown(client.dispose);

    final subscription = client.subscribe('feed');
    await expectLater(subscription.ready, throwsA(isA<ReverbConnectionException>()));

    expect(reports, hasLength(1));
    expect(reports.single.willRetry, isFalse);
    expect(isTlsFailure(reports.single.error), isTrue);
    expect((reports.single.error as ReverbConnectionException).rootCause, isA<HandshakeException>());
  });

  group('WebSocketChannelConnector (the default, platform-neutral connector)', () {
    test('connects, subscribes and receives an event without upgrade headers', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      server.listen((HttpRequest request) async {
        // The server closes with the test; the socket goes with it.
        // ignore: close_sinks
        final socket = await WebSocketTransformer.upgrade(request);
        socket.add(
          jsonEncode(<String, Object?>{
            'event': 'pusher:connection_established',
            'data': jsonEncode(<String, Object?>{'socket_id': '7.7', 'activity_timeout': 30}),
          }),
        );
        socket.listen((Object? raw) {
          final frame = jsonDecode(raw! as String) as Map<String, dynamic>;
          if (frame['event'] != 'pusher:subscribe') return;
          final channel = (frame['data'] as Map)['channel'];
          socket
            ..add(jsonEncode(<String, Object?>{'event': 'pusher_internal:subscription_succeeded', 'channel': channel}))
            ..add(jsonEncode(<String, Object?>{'event': 'hello', 'channel': channel, 'data': '{"n":2}'}));
        });
      });

      final client = ReverbClient(options: optionsFor(server.port));
      addTearDown(client.dispose);

      final subscription = client.subscribe('feed');
      final event = subscription.events.first;
      await subscription.ready;

      expect(client.socketId, '7.7');
      expect((await event).data, <String, dynamic>{'n': 2});
    });

    test('closeAll abandons an upgrade the server never answers', () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      addTearDown(() => server.close(force: true));
      final arrived = Completer<void>();
      server.listen((HttpRequest request) {
        if (!arrived.isCompleted) arrived.complete();
      });

      final client = ReverbClient(options: optionsFor(server.port), connector: const WebSocketChannelConnector());
      addTearDown(client.dispose);
      final subscription = client.subscribe('feed');
      await arrived.future;

      await client.closeAll();
      await expectLater(subscription.ready, throwsA(isA<ReverbCancelledException>()));
    });
  });
}
