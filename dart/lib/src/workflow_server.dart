/// Servidor WebSocket embarcado que recebe eventos do logger plugin do
/// Snakemake (`snakemake-logger-plugin-dart`) rodando como cliente.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_web_socket/shelf_web_socket.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'workflow_events.dart';

/// Hospeda o endpoint WebSocket de loopback ao qual o plugin se conecta.
///
/// ```dart
/// final server = WorkflowServer(token: WorkflowServer.generateToken());
/// await server.start();
/// server.events.listen(state.apply);
/// // passe server.boundPort + server.token para SnakemakeRunner.start(...)
/// ```
class WorkflowServer {
  WorkflowServer({this.token, this.port = 0});

  /// Bearer token que o plugin deve apresentar; `null` desativa a checagem
  /// (só faz sentido em experimentos locais).
  final String? token;

  /// Porta solicitada; 0 = efêmera. Depois de [start], veja [boundPort].
  final int port;

  HttpServer? _http;
  WebSocketChannel? _plugin;
  final _events = StreamController<WorkflowEvent>.broadcast();
  int _lastSeq = 0;
  String? _runId;

  /// Stream de eventos decodificado e deduplicado para a camada de UI.
  Stream<WorkflowEvent> get events => _events.stream;

  /// Porta efetivamente vinculada (use-a para montar o --logger-dart-address do plugin).
  int get boundPort => _http!.port;

  /// Endereço `ws://` para entregar ao plugin.
  String get address => 'ws://127.0.0.1:$boundPort';

  bool get pluginConnected => _plugin != null;

  static String generateToken() {
    final rng = Random.secure();
    return base64UrlEncode(List.generate(32, (_) => rng.nextInt(256)));
  }

  Future<void> start() async {
    final handler = const Pipeline()
        .addMiddleware(_checkToken)
        .addHandler(webSocketHandler(_onConnection));
    // Loopback apenas — nunca exponha o feed do workflow na rede.
    _http = await shelf_io.serve(handler, InternetAddress.loopbackIPv4, port);
  }

  Handler _checkToken(Handler inner) => (Request request) {
    if (token != null && request.headers['authorization'] != 'Bearer $token') {
      return Response(401, body: 'invalid token');
    }
    return inner(request);
  };

  void _onConnection(WebSocketChannel channel, String? protocol) {
    // A última conexão do plugin vence; um plugin reconectando substitui a si mesmo.
    _plugin?.sink.close();
    _plugin = channel;
    channel.stream.listen(
      _onFrame,
      onDone: () {
        if (identical(_plugin, channel)) _plugin = null;
      },
      onError: (_) {
        if (identical(_plugin, channel)) _plugin = null;
      },
    );
  }

  void _onFrame(dynamic raw) {
    // Frames binários não fazem parte do protocolo. Uma exceção lançada aqui
    // dentro do onData NÃO é capturada pelo onError do listen — viraria erro
    // assíncrono não tratado no app —, então todo o parse fica no try.
    if (raw is! String) return;
    final WorkflowEvent event;
    try {
      event = WorkflowEvent.fromJson(jsonDecode(raw) as Map<String, dynamic>);
    } catch (_) {
      return; // ignora frames malformados
    }

    if (event is HelloEvent) {
      // Os números de sequência reiniciam por run_id, então um novo run precisa
      // zerar o cursor de dedup — senão os eventos iniciais dele (seq <= máximo
      // do run anterior) são descartados e o replay pedido começa tarde demais.
      if (event.runId != _runId) {
        _runId = event.runId;
        _lastSeq = 0;
      }
      // Nova conexão: pede ao plugin para reenviar o que perdemos.
      requestReplay(sinceSeq: _lastSeq);
      _events.add(event);
      return;
    }
    // Entrega at-least-once: descarta duplicatas pelo número de sequência.
    if (event.seq <= _lastSeq) return;
    _lastSeq = event.seq;
    _events.add(event);
  }

  /// Pede ao plugin o reenvio dos eventos em buffer com `seq > sinceSeq`.
  void requestReplay({required int sinceSeq}) =>
      _send({'type': 'command', 'cmd': 'replay', 'since_seq': sinceSeq});

  /// Sonda de vivacidade; o plugin responde com um evento `pong`.
  void ping() => _send({'type': 'command', 'cmd': 'ping'});

  void _send(Map<String, dynamic> command) =>
      _plugin?.sink.add(jsonEncode(command));

  Future<void> close() async {
    await _plugin?.sink.close();
    await _http?.close(force: true);
    await _events.close();
  }
}
