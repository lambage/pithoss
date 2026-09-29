import 'package:flutter_test/flutter_test.dart';
import 'package:pithoss/pitboss.dart';

class _FakeTransport extends PitBossTransport {
  bool connected = false;
  String? lastMethod;
  Map<String, dynamic>? lastParams;

  @override
  Future<void> connect() async {
    connected = true;
  }

  @override
  Future<void> disconnect() async {
    connected = false;
  }

  @override
  bool get isConnected => connected;

  @override
  Future<Map<String, dynamic>> sendCommand(
    String method,
    Map<String, dynamic> params, {
    Duration? timeout,
  }) async {
    lastMethod = method;
    lastParams = params;
    return {'ok': true, 'method': method};
  }
}

void main() {
  test('PitBossClient routes commands through the selected transport', () async {
    final transport = _FakeTransport();
    final client = PitBossClient(transport, model: 'PBV4PS2');

    await client.connect();
    final result = await client.sendCommand('RPC.Ping', {});

    expect(client.isConnected, isTrue);
    expect(result['method'], 'RPC.Ping');
    expect(transport.lastMethod, 'RPC.Ping');
  });
}
