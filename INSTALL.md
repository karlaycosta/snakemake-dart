# Compilar, instalar e testar

Guia prático para: (1) instalar o plugin no Snakemake, (2) gerar o pacote
distribuível, (3) rodar os testes Python e Dart, e (4) validar a ponte
ponta-a-ponta com um workflow real. Todos os comandos abaixo foram executados
e validados neste repositório (Snakemake 9.23, Python 3.13, Dart SDK 3.12).

## Pré-requisitos

| Ferramenta | Versão mínima | Verificação |
| --- | --- | --- |
| Python | 3.11 | `python3 --version` |
| Snakemake | 9.0 | `snakemake --version` |
| Dart SDK | 3.0 | `dart --version` |

> **Importante:** o plugin precisa estar instalado **no mesmo ambiente Python
> do Snakemake** (mesmo venv/conda). O Snakemake descobre plugins pelo nome do
> pacote (`snakemake-logger-plugin-*`) via entry points do ambiente ativo.

---

## 1. Instalar o plugin no Snakemake

```bash
# ative o ambiente onde o Snakemake está (ou será) instalado
python3 -m venv .venv && source .venv/bin/activate   # exemplo com venv
pip install snakemake

# instale o plugin a partir do código-fonte
cd snakemake-dart/python
pip install .
```

### Verificar a instalação

O Snakemake deve passar a anunciar as flags do plugin:

```bash
snakemake --help | grep logger-dart
#   --logger-dart-address VALUE
#   --logger-dart-token VALUE
#   --logger-dart-flush-timeout VALUE
```

Se o `grep` não retornar nada, o plugin foi instalado em outro ambiente —
confira com `pip show snakemake snakemake-logger-plugin-dart` se ambos
apontam para o mesmo `Location`.

### Usar num run

```bash
snakemake --cores 4 \
    --logger dart \
    --logger-dart-address ws://127.0.0.1:<porta-do-app>
# token (opcional, recomendado): via env var, fora do `ps`
export SNAKEMAKE_LOGGER_DART_TOKEN=<token-gerado-pelo-app>
```

## 2. Gerar o pacote distribuível (wheel)

Para distribuir sem enviar o código-fonte (ex.: instalar em outra máquina):

```bash
cd snakemake-dart/python
pip install build
python -m build            # gera dist/*.whl e dist/*.tar.gz
pip install dist/snakemake_logger_plugin_dart-0.1.0-py3-none-any.whl
```

### Instalação editable (desenvolvimento)

```bash
pip install -e '.[dev]'    # inclui o pytest
```

> **Nota:** em ambientes com `hatchling` antigo, o modo editable pode falhar
> com `has no attribute 'prepare_metadata_for_build_editable'`. Contornos:
> `pip install -U hatchling` ou, sem instalar nada, rodar direto do fonte com
> `PYTHONPATH=src`.

## 3. Testes do plugin (Python)

```bash
cd snakemake-dart/python
pip install pytest 'websockets>=12' 'snakemake-interface-logger-plugins>=1.2.0,<3'
pytest -v                      # com o plugin instalado
# ou, sem instalar o pacote:
PYTHONPATH=src pytest -v
```

Suíte (10 testes): tradução de `LogEvent`s → payloads JSON e integração do
transporte contra um servidor WebSocket real — envelope/`seq`, replay,
autenticação por token e garantia de que `send()` nunca bloqueia com o
servidor fora do ar.

## 4. Testes do módulo Dart

```bash
cd snakemake-dart/dart
dart pub get                   # baixa shelf, web_socket_channel, test, etc.
dart analyze                   # análise estática — deve terminar sem issues
dart test                      # suíte de testes
dart test -r expanded          # saída detalhada, um teste por linha
```

Suíte (3 testes): rejeição de conexão sem token (401), decodificação +
deduplicação por `seq` + reducer de estado (na ordem real dos eventos, com
`job_started` antes de `job_info`), e forward-compat de tipos desconhecidos.

> Os testes usam apenas o Dart SDK (`dart test`).

## 5. Validação ponta-a-ponta com um workflow real

Duas abas de terminal:

```bash
# aba 1 — o "app" (monitor headless de exemplo)
cd snakemake-dart/dart
SNAKEMAKE_LOGGER_DART_TOKEN=dev dart run example/monitor_cli.dart
# imprime: LISTENING port=NNNNN address=ws://127.0.0.1:NNNNN
```

```bash
# aba 2 — um workflow qualquer (no ambiente com o plugin instalado)
cd /caminho/do/workflow
export SNAKEMAKE_LOGGER_DART_TOKEN=dev
snakemake --cores 2 \
    --logger dart \
    --logger-dart-address ws://127.0.0.1:NNNNN   # porta da aba 1
```

Saída esperada na aba 1: `HelloEvent`, `WorkflowStartedEvent`,
`RuleGraphEvent` (com o DAG em formato node-link), depois o fluxo de
`JobStartedEvent`/`JobInfoEvent`/`JobFinishedEvent`/`ProgressEvent` e, ao
final, `ByeEvent` seguido de:

```
DONE status=finished jobs=N progress=1.0
```

O `monitor_cli` encerra sozinho ao receber o `bye`. Se nada aparecer na aba 1,
os suspeitos usuais são: token diferente entre as abas, porta errada, ou o
plugin instalado num ambiente Python que não é o do `snakemake` em uso.

## Resumo rápido

```bash
# plugin
cd snakemake-dart/python && pip install . && pytest -v

# dart
cd ../dart && dart pub get && dart analyze && dart test
```
