import 'package:dart_reverb_channels/testing.dart';
import 'package:flutter_test/flutter_test.dart';

// The "Testing your code" example from README.md, kept compiling and passing.
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
