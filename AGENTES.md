# Agentes de IA no pepeuhacker

Os agentes leem o dashboard pelos mesmos números da tela, sem navegador. Nada
aqui recalcula métrica: cada tela, pedida em modo JSON, roda a mesma função de
rota e serializa o que ia desenhar. Código em `agent_api.py`; ponte em
`tools/mcp_local.py`.

## As duas portas

**1. `?format=json` em qualquer tela.** `/leaders/forecast?preset=current_q`
vira `/leaders/forecast?date_from=2026-10-01&date_to=2026-12-31&format=json`.
Volta `filtros`, `parametros`, `contexto` (inclui `freshness`, de quando é o
dado) e `dados` (os blocos que a tela desenha). Opcionais: `campos=kpis,by_ae`
pra pedir só alguns blocos e `max_linhas=N` (padrão 500) por tabela. Os drills
(`/…/deals`) já respondem JSON.

**2. Servidor MCP em `/mcp`.** Três ferramentas, todas só leitura:

| ferramenta | pra quê |
| --- | --- |
| `listar_telas` | catálogo: caminho, título e o que cada tela mostra; `incluir_drills` traz os drills e os parâmetros que cada um lê (tirados do código) |
| `opcoes_de_filtro` | líderes, AEs, regiões e presets de período com as datas de cada um |
| `ler_tela` | os dados de uma tela ou drill; aceita a URL copiada do navegador; `preset` vira datas aqui |

`refresh=1` é bloqueado (forçaria BigQuery em todas as fontes a cada chamada).
Resposta passa de 60 mil caracteres: listas são cortadas e o agente é avisado
pra pedir `campos`.

## Onde os agentes entram

| agente roda | como | precisa de |
| --- | --- | --- |
| no Mac, com o repo | ponte em modo **local** (padrão) | venv do projeto + login ADC |
| em qualquer máquina | ponte em modo **remoto** → `pepeuhacker-api` | só `tools/mcp_local.py`, `pip install google-auth requests` e login ADC @tractian.com |
| na nuvem (n8n, Vertex, script) | POST direto em `https://pepeuhacker-api-199700483619.us-central1.run.app/mcp` | service account com `run.invoker` no `pepeuhacker-api`; token ID com audience = essa URL |

**Por que existe o `pepeuhacker-api`.** O principal fica atrás do IAP, que só
aceita login de navegador: token Google de linha de comando volta `401 Invalid
JWT audience` (testado em 19/10/2026). A lista de clientes programáticos do IAP
não é aceita no nível do serviço, só no do projeto, e o `tractian-bi` é
compartilhado (o `fse-frontend`, de outro time, também está atrás do IAP). O
gêmeo é o mesmo app com a MESMA imagem, sem IAP, protegido pelo IAM do Cloud
Run: `run.invoker` para as pessoas de `acesso.txt` (o mesmo público do IAP,
sincronizado por `./setup_acesso.sh`) e o
audience do ADC do gcloud aceito como custom audience, pra ponte de cada pessoa
entrar com o próprio login. Sem token: 403.

O gêmeo roda com `PEPEU_PAPEL=api`: sem pré-aquecimento e sem os laços que
gravam as fotos do forecast (quem grava é o principal). `min-instances 0`: não
custa nada parado, e a 1ª tela de cada tipo depois de um tempo parado leva de
8 a 15 s (cache frio). **Depois de cada `./deploy_cloudrun.sh`, rodar
`./deploy_api.sh`** — ele copia a imagem que está no ar, sem build.

Conector de claude.ai na web continua fora: ele exige MCP público com OAuth.

## Ligar no Claude Desktop

Em `~/Library/Application Support/Claude/claude_desktop_config.json`, dentro de
`mcpServers` (tirar `"--modo", "remoto"` pra rodar local):

```json
"pepeuhacker": {
  "command": "/Users/pedromendes/.pepeuhacker-tools/venv/bin/python",
  "args": ["/Users/pedromendes/Documents/pepeuhacker/tools/mcp_local.py", "--modo", "remoto"]
}
```

Reiniciar o Claude Desktop. No Claude Code: `claude mcp add pepeuhacker --scope
user -- /Users/pedromendes/.pepeuhacker-tools/venv/bin/python
/Users/pedromendes/Documents/pepeuhacker/tools/mcp_local.py --modo remoto`.

## Mandar pra alguém

`./tools/kit_conector/montar.sh` gera `dist/pepeuhacker-conector.zip`: a ponte,
um `instalar.sh` e um `LEIA-ME.md`. Quem recebe roda `bash
~/Downloads/pepeuhacker-conector/instalar.sh`, que cria um Python isolado em
`~/.pepeuhacker-conector`, faz o login ADC se faltar, testa o acesso ao gêmeo e
registra o servidor no Claude Desktop (e no Claude Code, se tiver). Precisa de
Mac, conta @tractian.com e `gcloud`. Fora do repositório a ponte já sobe em modo
remoto. Remontar o zip depois de mexer na ponte.

## Modo local

A ponte carrega o app dentro do próprio processo e lê o BigQuery com o login ADC
de quem roda. Não passa pelo Cloud Run. Útil sem rede até o Google Cloud ou pra
testar código ainda não publicado; precisa do repositório, do venv e de leitura
no BigQuery `tractian-bi`.

## Testar

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | ~/.pepeuhacker-tools/venv/bin/python tools/mcp_local.py --modo remoto
```
