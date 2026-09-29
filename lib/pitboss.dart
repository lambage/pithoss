import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

class PitBossException implements Exception {
  const PitBossException(this.message, [this.code]);

  final String message;
  final int? code;

  @override
  String toString() => 'PitBossException(code: $code, message: $message)';
}

abstract class PitBossTransport {
  Future<void> connect();
  Future<void> disconnect();
  bool get isConnected;

  Future<Map<String, dynamic>> sendCommand(
    String method,
    Map<String, dynamic> params, {
    Duration? timeout,
  });
}

class HttpPitBossTransport extends PitBossTransport {
  HttpPitBossTransport(
    String host, {
    this._port = 80,
    http.Client? client,
  })  : _host = host,
        _client = client ?? http.Client();

  final String _host;
  final int _port;
  final http.Client _client;
  bool _connected = false;
  int _nextId = 1;

  Uri get _url => Uri.parse('http://$_host:$_port/rpc');

  @override
  Future<void> connect() async {
    _connected = true;
    await sendCommand('RPC.Ping', {});
  }

  @override
  Future<void> disconnect() async {
    _connected = false;
  }

  @override
  bool get isConnected => _connected;

  @override
  Future<Map<String, dynamic>> sendCommand(
    String method,
    Map<String, dynamic> params, {
    Duration? timeout,
  }) async {
    if (!_connected) {
      await connect();
    }

    final command = {
      'id': _nextId++,
      'method': method,
      'params': params,
    };

    final response = await _client
        .post(
          _url,
          headers: {'Content-Type': 'application/json'},
          body: jsonEncode(command),
        )
        .timeout(timeout ?? const Duration(seconds: 30));

    if (response.statusCode != 200) {
      throw PitBossException(
        'HTTP RPC failed with status ${response.statusCode}: ${response.body}',
        response.statusCode,
      );
    }

    final payload = jsonDecode(response.body);
    if (payload is! Map) {
      return {'result': payload};
    }

    final map = Map<String, dynamic>.from(payload);
    if (map.containsKey('error')) {
      final error = map['error'];
      if (error is Map) {
        final details = Map<String, dynamic>.from(error);
        throw PitBossException(
          details['message']?.toString() ?? 'PitBoss RPC error',
          details['code'] is int ? details['code'] as int : null,
        );
      }
      throw PitBossException('PitBoss RPC error');
    }

    final result = map['result'];
    if (result is Map) {
      return Map<String, dynamic>.from(result);
    }
    if (result == null) {
      return {};
    }
    return {'value': result};
  }
}

class WebSocketPitBossTransport extends PitBossTransport {
  WebSocketPitBossTransport(
    this.grillId, {
    this._baseUrl = 'wss://socket.dansonscorp.com',
  });

  final String grillId;
  final String _baseUrl;
  WebSocketChannel? _channel;
  bool _connected = false;
  final Map<int, Completer<Map<String, dynamic>>> _pending = {};
  int _nextId = 1;

  @override
  Future<void> connect() async {
    if (_channel != null) {
      _connected = true;
      return;
    }

    _channel = WebSocketChannel.connect(Uri.parse('$_baseUrl/to/$grillId'));
    _connected = true;

    _channel!.stream.listen((message) {
      final decoded = jsonDecode(message);
      if (decoded is! Map) {
        return;
      }

      final map = Map<String, dynamic>.from(decoded);
      final idValue = map['id'];
      if (idValue is int) {
        final completer = _pending.remove(idValue);
        if (completer != null && !completer.isCompleted) {
          completer.complete(map['result'] is Map
              ? Map<String, dynamic>.from(map['result'])
              : {'value': map['result']});
        }
      }
    }, onError: (_) {
      _connected = false;
    }, onDone: () {
      _connected = false;
    });
  }

  @override
  Future<void> disconnect() async {
    _connected = false;
    final channel = _channel;
    _channel = null;
    await channel?.sink.close();
  }

  @override
  bool get isConnected => _connected && _channel != null;

  @override
  Future<Map<String, dynamic>> sendCommand(
    String method,
    Map<String, dynamic> params, {
    Duration? timeout,
  }) async {
    if (_channel == null) {
      await connect();
    }

    final requestId = _nextId++;
    final completer = Completer<Map<String, dynamic>>();
    _pending[requestId] = completer;

    _channel!.sink.add(
      jsonEncode({'id': requestId, 'method': method, 'params': params}),
    );

    return completer.future.timeout(timeout ?? const Duration(seconds: 30));
  }
}

class BluetoothPitBossTransport extends PitBossTransport {
  BluetoothPitBossTransport(this.deviceId);

  final String deviceId;
  bool _connected = false;

  @override
  Future<void> connect() async {
    if (deviceId.trim().isEmpty) {
      throw const PitBossException('Bluetooth device ID is required');
    }
    _connected = true;
  }

  @override
  Future<void> disconnect() async {
    _connected = false;
  }

  @override
  bool get isConnected => _connected;

  @override
  Future<Map<String, dynamic>> sendCommand(
    String method,
    Map<String, dynamic> params, {
    Duration? timeout,
  }) async {
    if (!_connected) {
      await connect();
    }

    return {
      'method': method,
      'params': params,
      'deviceId': deviceId,
      'status': 'queued',
    };
  }
}

class PitBossClient {
  PitBossClient(
    this.transport, {
    required this.model,
    this.password = '',
  });

  final PitBossTransport transport;
  final String model;
  final String password;

  bool get isConnected => transport.isConnected;

  Future<void> connect() => transport.connect();
  Future<void> disconnect() => transport.disconnect();

  Future<Map<String, dynamic>> sendCommand(
    String method,
    Map<String, dynamic> params, {
    Duration? timeout,
  }) =>
      transport.sendCommand(method, params, timeout: timeout);

  Future<Map<String, dynamic>> ping() => sendCommand('RPC.Ping', {});

  Future<Map<String, dynamic>> getState() => sendCommand('PB.GetState', {});

  Future<Map<String, dynamic>> getVirtualData() =>
      sendCommand('PB.GetVirtualData', {});

  Future<Map<String, dynamic>> setVirtualData(Map<String, dynamic> data) =>
      sendCommand('PB.SetVirtualData', data);

  Future<Map<String, dynamic>> listRpcs() => sendCommand('RPC.List', {});
}
