"""Acesso de agentes de IA ao pepeuhacker, sem navegador.

Duas portas para os MESMOS números:

1. `?format=json` em qualquer tela devolve o que a tela usa pra se desenhar (os
   argumentos do `render_template`) em JSON. Não existe segunda implementação de
   métrica nenhuma: é a mesma função de rota, só que o último passo serializa em
   vez de renderizar. Se a tela mudar, o agente acompanha sozinho.

2. `/mcp` é um servidor MCP (Model Context Protocol, transporte Streamable HTTP,
   sem estado, respostas em JSON) com três ferramentas — `listar_telas`,
   `opcoes_de_filtro` e `ler_tela` — que por baixo chamam as telas em modo JSON.

AUTENTICAÇÃO: nenhuma aqui dentro, de propósito. O app inteiro fica atrás do IAP
(setup_sso.sh), então quem chega em /mcp já passou pelo login @tractian.com. A
ponte `tools/mcp_local.py` é quem pega o token Google de quem usa e fala com o
/mcp de produção; ver AGENTES.md.
"""

import dataclasses
import datetime as dt
import decimal
import inspect
import json
import math
import re
import urllib.parse

import numpy as np
import pandas as pd
from flask import Response, has_request_context, request
from markupsafe import Markup

PROTOCOLOS = ("2025-06-18", "2025-03-26", "2024-11-05")
MAX_LINHAS_HTTP = 500           # ?format=json direto: quem chama decide com max_linhas
MAX_LINHAS_MCP = 100
# Teto da resposta de UMA ferramenta. Clientes MCP cortam ou recusam saída
# grande (o Claude Code avisa a partir de ~25 mil tokens); 60 mil caracteres
# dão ~15 mil tokens e cabem em qualquer um.
ORCAMENTO_MCP = 60_000
# `refresh=1` força releitura do BigQuery em todas as fontes da tela. Um agente
# em loop com isso ligado vira custo de BQ e fila no app; o cache de 1h basta.
_PARAMS_BLOQUEADOS = {"refresh", "format", "max_linhas", "campos"}

# Chaves do base_context() que são só da barra de filtros/navegação. O resto do
# ctx (páginas que penduram números nele) vai junto em "contexto".
_CTX_DESCARTAR = {
    "df", "df_all", "options", "presets", "active_chips", "querystring",
    "extra_filters", "view_url_ae", "view_url_leaders", "view_url_sdr",
    "view_url_mx", "leader_tabs", "sdr_tabs", "months_per_year",
    "money_url_mrr", "money_url_arr", "q_urls", "q_opts", "q_presets",
    "period_locked", "filters",
}


# ── Serialização ──────────────────────────────────────────────────────────────

def _chave(k):
    if isinstance(k, (dt.date, dt.datetime, pd.Timestamp)):
        return k.isoformat()
    if isinstance(k, tuple):
        return " · ".join(str(x) for x in k)
    return str(k)


def _lista(seq, max_linhas, prof):
    itens = [_jsonavel(v, max_linhas, prof + 1) for v in seq[:max_linhas]]
    if len(seq) > max_linhas:
        return {"_truncado": True, "total": len(seq), "itens": itens}
    return itens


def _tabela(df, max_linhas, prof):
    """DataFrame → {colunas, linhas, total_linhas}. Índice com significado (não
    RangeIndex) vira coluna, senão o nome do AE/estágio some."""
    if not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [" · ".join(str(x) for x in c if str(x)) for c in df.columns]
    cols = [str(c) for c in df.columns]
    linhas = [{c: _jsonavel(v, max_linhas, prof + 1) for c, v in zip(cols, row)}
              for row in df.head(max_linhas).itertuples(index=False, name=None)]
    out = {"_tabela": True, "colunas": cols, "total_linhas": len(df), "linhas": linhas}
    if len(df) > max_linhas:
        out["_truncado"] = True
    return out


def _jsonavel(o, max_linhas=MAX_LINHAS_HTTP, prof=0):
    """Qualquer coisa que uma rota passa pro template → JSON puro e válido.

    NaN/±inf viram null (o mesmo cuidado do `_JsonSemNaN` do app). Callables
    somem: o template recebe funções de formatação que não são dado."""
    if prof > 16:
        return str(o)[:200]
    if isinstance(o, Markup):           # antes de str: Markup É str, mas com HTML
        return o.striptags()
    if o is None or isinstance(o, (str, bool, int)):
        return o
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        f = float(o)
        return f if math.isfinite(f) else None
    if o is pd.NaT or o is pd.NA:
        return None
    if isinstance(o, decimal.Decimal):
        return float(o)
    if isinstance(o, (pd.Timestamp, dt.datetime, dt.date, dt.time)):
        return o.isoformat()
    if isinstance(o, (pd.Timedelta, dt.timedelta)):
        return o.total_seconds()
    if isinstance(o, pd.Period):
        return str(o)
    if isinstance(o, pd.DataFrame):
        return _tabela(o, max_linhas, prof)
    if isinstance(o, pd.Series):
        if isinstance(o.index, pd.RangeIndex):
            return _lista(o.tolist(), max_linhas, prof)
        return _jsonavel(o.to_dict(), max_linhas, prof)
    if isinstance(o, np.ndarray):
        return _lista(o.tolist(), max_linhas, prof)
    if isinstance(o, dict):
        return {_chave(k): _jsonavel(v, max_linhas, prof + 1)
                for k, v in o.items() if not callable(v)}
    if isinstance(o, (set, frozenset)):
        return _lista(sorted(o, key=str), max_linhas, prof)
    if isinstance(o, (list, tuple)):
        return _lista(list(o), max_linhas, prof)
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return _jsonavel(dataclasses.asdict(o), max_linhas, prof)
    if callable(o):
        return None
    return str(o)[:300]


def _dump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


# ── Porta 1: ?format=json ─────────────────────────────────────────────────────

def quer_json() -> bool:
    return has_request_context() and request.args.get("format") == "json"


def _int(v, padrao, lo=1, hi=5000):
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return padrao


def _campos_pedidos():
    raw = request.args.get("campos", "")
    return [c.strip() for c in raw.split(",") if c.strip()]


def resposta_da_tela(template: str, kw: dict) -> Response:
    """O que vai no lugar do HTML quando a tela é pedida com ?format=json."""
    max_linhas = _int(request.args.get("max_linhas"), MAX_LINHAS_HTTP)
    ctx = kw.get("ctx") if isinstance(kw.get("ctx"), dict) else {}
    dados = {k: v for k, v in kw.items() if k != "ctx"}
    disponiveis = sorted(dados)
    campos = _campos_pedidos()
    if campos:
        dados = {k: v for k, v in dados.items() if k in campos}
    corpo = {
        "tela": request.path,
        "template": template,
        "filtros": {k: v for k, v in (ctx.get("filters") or {}).items() if v},
        # Tudo que veio na URL — inclusive o que não é filtro global (quarters,
        # q_from, grain, base...), pra quem chamou conferir o que a tela recebeu.
        "parametros": {k: v for k, v in request.args.items() if v and k not in _PARAMS_BLOQUEADOS},
        "contexto": {k: v for k, v in ctx.items() if k not in _CTX_DESCARTAR},
        "campos_disponiveis": disponiveis,
        "dados": dados,
    }
    return Response(_dump(_jsonavel(corpo, max_linhas)), mimetype="application/json")


# ── Catálogo de telas ─────────────────────────────────────────────────────────

# Caminho-base (sem /leaders) → (título, o que mostra). Leaders é a mesma tela
# agregada por líder; quando a tela só existe no Leaders, a chave é o caminho
# completo.
_TELAS = {
    "/overview": ("Sales Overview",
                  "Realizado (PO Won) × meta, atingimento, new logos, aquisição × expansão, ticket médio, "
                  "ciclo de vendas, pipeline aberto e projeção no período."),
    "/overview/ae": ("AE Overview", "Ranking de receita realizada e de new logos por vendedor."),
    "/leaders/overview/team": ("Leaders Overview", "Ranking por líder (soma do time): realizado, meta e new logos."),
    "/overview/overtime": ("Sales Overtime",
                           "Série por trimestre: meta × realizado, % de atingimento, new logos, ticket médio, "
                           "unit price e conversão. O período aqui é por trimestre: quarters=N|all ou q_from/q_to "
                           "(ex. 2025-Q3)."),
    "/demand": ("Geração de Demanda",
                "Opps geradas por AE no período: geradas pelo próprio AE × recebidas, aquisição × expansão, "
                "canal pela propriedade motion."),
    "/sdr/effort": ("Pré Sales BR", "Placeholder, ainda sem dados."),
    "/pipeline": ("Pipeline · Cobertura",
                  "Deals abertos por AE em dois grupos (early = Scoping; late = Negotiation + Decision + "
                  "Procurement) contra o que falta da meta. O período filtra o close date."),
    "/pipeline/total": ("Pipeline Total",
                        "Todo o dinheiro aberto no funil por estágio, cortável por deal type (dtype), source, "
                        "produto (prod), forecast category (fcat), close date em botões (cd=lista separada por vírgula de "
                        "late = atrasados, sem = sem close date, AAAA-Qn = trimestre atual e os 2 seguintes; "
                        "vários = união) e customer owner (cown=com|sem). Não usa date_from/date_to nem preset."),
    "/pipeline/stalled": ("Pipeline · Stale",
                          "Deals parados por tempo no estágio (SLA por estágio) e a movimentação domingo × "
                          "domingo anterior."),
    "/pipeline/stalled-activity": ("Pipeline · Inatividade",
                                   "Deals sem retorno de contato (e-mail, ligação, WhatsApp, reunião do Gong) há mais de 7 dias, "
                                   "por estágio, e a movimentação semanal."),
    "/pipeline/funnel": ("Funil WoW", "Quantidade e valor por estágio no último domingo × domingo retrasado."),
    "/pipeline/funnel-ii": ("Funil II",
                            "Conversão por etapa (→ próxima, → Won, → Lost). base=created conta pela criação do "
                            "deal; dtype filtra o deal type."),
    "/pipeline/legal": ("Legal",
                        "Ticket médio e conversão de deals fechados com jurídico envolvido × sem. env = definição "
                        "de envolvimento; coh = coorte."),
    "/pipeline/wow/early": ("PoP Early",
                            "Pipe early em dois domingos e o que aconteceu deal a deal: novo, cresceu, encolheu, "
                            "lost, won."),
    "/pipeline/wow/late": ("PoP Late", "O mesmo da PoP Early, para o pipe late."),
    "/pipeline/wow/history": ("PoP Histórico", "Movimento semanal do pipe early ou late, time e pessoa a pessoa. Pipe total "
                              "(qualquer close date); com cd=q, só close date no trimestre vigente e a "
                              "cobertura histórica."),
    "/pipeline/wow/quarter": ("PoP Histórico Q",
                              "Pipe da semana com close date no trimestre e a cobertura histórica "
                              "(pipe ÷ quota − realizado), por AE; com ae= a meta é a quota do AE."),
    "/leaders/pipeline/wow/quarter": ("PoP Histórico Q",
                                      "Pipe da semana com close date no trimestre e a cobertura histórica "
                                      "(pipe ÷ meta − realizado)."),
    "/pipeline/visits": ("Visitas", "Visitas de campo e ao IA Center por vendedor."),
    "/leaders/pipeline/dod": ("Pipeline DoD", "Destino do pipe de cada estágio entre a meia-noite de ontem e a de hoje."),
    "/leaders/pipeline/wow": ("Pipeline WoW", "Destino do pipe de cada estágio entre a meia-noite de duas segundas."),
    "/forecast": ("Forecast",
                  "Deals com close date no trimestre (estágios 1 a 6 + won), ponderados pela forecast category "
                  "(100/75/50/25/0%): realizado + ponderado contra a meta."),
    "/forecast/model": ("Forecast · Modelo Analytics",
                        "Projeção = PO Won no período + deals abertos ponderados pela conversão histórica "
                        "(segmento × estágio × tipo)."),
    "/forecast/history": ("Histórico do Forecast",
                          "Como o forecast do trimestre andou semana a semana, nos dois modelos (diretores e "
                          "Analytics)."),
    "/forecast/wow": ("Forecast WoW", "Migração do pipeline entre as faixas de probabilidade entre dois domingos."),
    "/leaders/forecast/dod": ("Forecast DoD", "Destino de cada faixa do forecast entre a meia-noite de ontem e a de hoje."),
    "/mx/overview": ("Sales MX · Overview", "Realizado × meta por vendedor e pipeline aberto (ARR em US$)."),
    "/mx/pipeline": ("Sales MX · Pipeline", "ARR aberto por estágio, cortável por deal type."),
    "/mx/forecast": ("Sales MX · Forecast", "Pipeline com close date no período, ponderado."),
    "/mx/funnel": ("Sales MX · Funil", "Conversão de estágio a estágio e win rate."),
    "/mx/coverage": ("Sales MX · Cobertura", "Pipeline para cada dólar que falta da meta."),
    "/mx/stale": ("Sales MX · Inatividade", "Pipeline aberto por tempo sem atividade."),
    "/mx/pipeline/wow/early": ("Sales MX · PoP Early", "PoP Early do funil Sales MX."),
    "/mx/pipeline/wow/late": ("Sales MX · PoP Late", "PoP Late do funil Sales MX."),
}

_FILTROS_COMUNS = ["date_from", "date_to", "preset", "leader", "ae", "region", "money"]
_ENDPOINTS_FORA = {"static", "mcp", "access_link", "home"}
# Drill = rota que já nasce JSON (o modal de deals da tela).
_DRILL = re.compile(r"(deals|/moves|/cobertura|/funnel-stages)$")


def _visao(caminho):
    for p, v in (("/leaders/", "leaders"), ("/mx/", "mx"), ("/sdr/", "sdr")):
        if caminho.startswith(p):
            return v
    return "ae"


def _doc(fn, n=700):
    d = inspect.getdoc(fn) or ""
    return re.sub(r"\s+", " ", d).strip()[:n]


def _params_lidos(fn):
    """Parâmetros de URL que a função do drill lê — tirados do próprio código,
    pra lista nunca desatualizar."""
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        return []
    achados = re.findall(r"request\.args\.get\(\s*[\"']([a-z_0-9]+)[\"']", src)
    return sorted({p for p in achados if p not in _PARAMS_BLOQUEADOS})


def _catalogo(app):
    telas, drills = [], []
    for rule in sorted(app.url_map.iter_rules(), key=lambda r: r.rule):
        if rule.endpoint in _ENDPOINTS_FORA or "GET" not in (rule.methods or ()):
            continue
        caminho = re.sub(r"<(?:[^:<>]+:)?([^<>]+)>", r"{\1}", rule.rule)
        fn = app.view_functions.get(rule.endpoint)
        if _DRILL.search(rule.rule):
            drills.append({"caminho": caminho, "visao": _visao(caminho),
                           "o_que_faz": _doc(fn), "parametros": _params_lidos(fn)})
            continue
        base = caminho.replace("/leaders/", "/", 1) if caminho.startswith("/leaders/") else caminho
        titulo, desc = _TELAS.get(caminho) or _TELAS.get(base) or (caminho, _doc(fn, 300))
        item = {"caminho": caminho, "titulo": titulo, "visao": _visao(caminho), "o_que_mostra": desc}
        if caminho.startswith("/leaders/"):
            item["o_que_mostra"] += " Visão Leaders: agregado por líder."
        telas.append(item)
    return telas, drills


# ── Execução das ferramentas ──────────────────────────────────────────────────

class ErroFerramenta(Exception):
    """Erro que o agente consegue corrigir (caminho errado, filtro inválido)."""


def _normalizar(app, caminho, filtros):
    # Aceita a URL copiada da barra do navegador: o host some e a querystring
    # vira filtro (o que vier em `filtros` ganha).
    partes = urllib.parse.urlsplit((caminho or "").strip())
    caminho = "/" + partes.path.lstrip("/")
    if partes.query:
        filtros = {**dict(urllib.parse.parse_qsl(partes.query)), **(filtros or {})}
    adapter = app.url_map.bind("localhost")
    try:
        endpoint, _ = adapter.match(caminho, method="GET")
    except Exception:
        raise ErroFerramenta(f"'{caminho}' não é uma tela do dashboard. Use listar_telas para ver os caminhos.")
    if endpoint in _ENDPOINTS_FORA - {"home"}:
        raise ErroFerramenta(f"'{caminho}' não é uma tela de dados.")
    qs = {}
    for k, v in (filtros or {}).items():
        if k in _PARAMS_BLOQUEADOS or v is None or v == "":
            continue
        qs[str(k)] = ("1" if v else "0") if isinstance(v, bool) else str(v)
    return caminho, qs


def _contexto_base(app, base_context, caminho, qs):
    with app.test_request_context(caminho, query_string=qs):
        return base_context()


def _aplicar_preset(app, base_context, caminho, qs):
    """`preset` só acende o chip; quem recorta são date_from/date_to, que o
    navegador manda junto. Sem isto, 'trimestre atual' devolveria tudo."""
    chave = qs.get("preset")
    if not chave or qs.get("date_from") or qs.get("date_to"):
        return qs
    presets = _contexto_base(app, base_context, caminho, qs).get("presets") or []
    p = next((p for p in presets if p.get("key") == chave), None)
    if p is None:
        validos = ", ".join(p.get("key", "") for p in presets) or "nenhum nesta tela"
        raise ErroFerramenta(f"preset '{chave}' não existe em {caminho}. Válidos: {validos}.")
    if p.get("from"):
        qs["date_from"] = str(p["from"])
    if p.get("to"):
        qs["date_to"] = str(p["to"])
    return qs


def ler_tela(app, base_context, caminho, filtros=None, campos=None, max_linhas=MAX_LINHAS_MCP):
    caminho, qs = _normalizar(app, caminho, filtros)
    qs = _aplicar_preset(app, base_context, caminho, qs)
    chamada = {**qs, "format": "json", "max_linhas": str(max_linhas)}
    if campos:
        chamada["campos"] = ",".join(campos)
    with app.test_client() as c:
        r = c.get(caminho, query_string=chamada, follow_redirects=True)
    if r.status_code >= 400:
        texto = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r.get_data(as_text=True)))[:600]
        raise ErroFerramenta(f"A tela respondeu {r.status_code}: {texto}")
    corpo = r.get_json(silent=True)
    if corpo is None:
        raise ErroFerramenta(f"{caminho} não devolveu dados (content-type {r.content_type}).")
    if not (isinstance(corpo, dict) and "dados" in corpo and "tela" in corpo):
        # Drill: a rota já nasce JSON e não conhece max_linhas — corta aqui.
        corpo = {"tela": caminho, "filtros": qs, "dados": _jsonavel(corpo, max_linhas)}
    return corpo


def opcoes_de_filtro(app, base_context, caminho="/overview", filtros=None):
    caminho, qs = _normalizar(app, caminho or "/overview", filtros)
    ctx = _contexto_base(app, base_context, caminho, qs)
    opts = ctx.get("options") or {}
    out = {
        "tela": caminho,
        "o_que_o_periodo_filtra": ctx.get("period_label"),
        "lideres": opts.get("leaders"),
        "rotulos_dos_lideres": opts.get("leader_labels"),
        "aes": opts.get("aes"),
        "regioes": opts.get("regions"),
        "papeis": opts.get("roles"),
        "presets": [{"preset": p.get("key"), "rotulo": p.get("label"),
                     "date_from": p.get("from"), "date_to": p.get("to")}
                    for p in (ctx.get("presets") or [])],
        "filtros_extras_da_tela": ctx.get("extra_filters"),
        "moeda": {"money=mrr": "MRR em R$ (padrão do Brasil)", "money=arr": "ARR em US$ (padrão do México)"},
    }
    if ctx.get("period_mode") == "quarter":
        out["periodo_por_trimestre"] = {
            "quarters": [o.get("key") if isinstance(o, dict) else o for o in (ctx.get("q_presets") or [])],
            "q_from_q_to": [o.get("key") for o in (ctx.get("q_opts") or [])],
        }
        out["presets"] = []
    return _jsonavel(out, 2000)


def _encolher(o, n):
    """Corta toda lista/tabela em n itens, sem chamar a tela de novo."""
    if isinstance(o, dict):
        if o.get("_tabela"):
            linhas = o.get("linhas") or []
            out = {**o, "linhas": [_encolher(x, n) for x in linhas[:n]]}
            if len(linhas) > n or o.get("_truncado"):
                out["_truncado"] = True
            return out
        if o.get("_truncado") and "itens" in o:
            return {**o, "itens": [_encolher(x, n) for x in o["itens"][:n]]}
        return {k: _encolher(v, n) for k, v in o.items()}
    if isinstance(o, list):
        itens = [_encolher(x, n) for x in o[:n]]
        return {"_truncado": True, "total": len(o), "itens": itens} if len(o) > n else itens
    return o


def _caber(corpo):
    """Garante que a resposta cabe no orçamento do cliente MCP."""
    texto = _dump(corpo)
    if len(texto) <= ORCAMENTO_MCP:
        return texto
    for n in (50, 20, 10, 5, 2):
        menor = _encolher(corpo, n)
        texto = _dump({**menor, "aviso": f"Resposta grande: listas e tabelas cortadas em {n} itens. "
                                          "Peça menos com 'campos' ou filtre por líder/AE/período."})
        if len(texto) <= ORCAMENTO_MCP:
            return texto
    dados = corpo.get("dados")
    tamanhos = ({k: len(_dump(v)) for k, v in dados.items()} if isinstance(dados, dict) else {})
    return _dump({"tela": corpo.get("tela"), "filtros": corpo.get("filtros"),
                  "aviso": "A tela é grande demais para uma resposta só. Chame ler_tela de novo com "
                           "'campos' escolhendo alguns blocos abaixo (o número é o tamanho em caracteres).",
                  "blocos": tamanhos})


# ── Porta 2: servidor MCP ─────────────────────────────────────────────────────

_INSTRUCOES = (
    "Dashboard de vendas da Tractian (pepeuhacker): Sales BR em R$ MRR e Sales MX em US$ ARR, "
    "lido do HubSpot e do BigQuery do BI. Comece por listar_telas; para filtrar por pessoa ou "
    "período, pegue os valores válidos em opcoes_de_filtro; depois ler_tela devolve os mesmos números "
    "da tela. Telas em /leaders/ são a mesma conta agregada por líder. Os drill-downs (incluir_drills) "
    "listam os deals por trás de cada número. O campo contexto.freshness diz de quando é o dado."
)

FERRAMENTAS = [
    {
        "name": "listar_telas",
        "title": "Listar telas do dashboard",
        "description": "Catálogo das telas do dashboard de vendas (Sales BR, Leaders e Sales MX): caminho, "
                       "título e o que cada uma mostra. Com incluir_drills=true traz também os drill-downs "
                       "(listas de deals por trás dos números) e os parâmetros que cada um lê.",
        "inputSchema": {"type": "object", "properties": {
            "incluir_drills": {"type": "boolean", "default": False},
            "visao": {"type": "string", "enum": ["ae", "leaders", "mx", "sdr"],
                      "description": "Só as telas desta visão."},
        }},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "opcoes_de_filtro",
        "title": "Valores válidos de filtro",
        "description": "Líderes, AEs, regiões e presets de período (com as datas de cada um) aceitos por "
                       "uma tela. Use antes de filtrar por pessoa ou período em ler_tela.",
        "inputSchema": {"type": "object", "properties": {
            "caminho": {"type": "string", "default": "/overview", "description": "Tela, ex. /forecast."},
            "filtros": {"type": "object", "description": "region ou leader já escolhidos (a lista de AEs "
                                                         "segue a cascata região → líder → AE)."},
        }},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "ler_tela",
        "title": "Ler dados de uma tela",
        "description": "Devolve os dados de uma tela ou drill-down com os mesmos números da tela. 'dados' "
                       "traz os blocos que a tela desenha (KPIs, tabelas por AE/líder, séries); "
                       "'contexto.freshness' diz de quando é o dado; tabelas vêm como {colunas, linhas, "
                       "total_linhas}. Valores em R$ são MRR, salvo money=arr (ARR em US$).",
        "inputSchema": {"type": "object", "required": ["caminho"], "properties": {
            "caminho": {"type": "string",
                        "description": "Caminho da tela, ex. /overview, /leaders/forecast, /pipeline/total, "
                                       "/forecast/deals. Aceita também a URL copiada do navegador."},
            "filtros": {"type": "object",
                        "description": "Parâmetros da tela. Comuns: date_from e date_to (AAAA-MM-DD), preset "
                                       "(ex. current_q, convertido em datas aqui), leader, ae, region, money "
                                       "(mrr|arr). Sales Overtime: quarters (N ou all), q_from, q_to "
                                       "(ex. 2025-Q3). Drill-downs: os parâmetros listados em listar_telas.",
                        "additionalProperties": {"type": ["string", "number", "boolean"]}},
            "campos": {"type": "array", "items": {"type": "string"},
                       "description": "Só estes blocos de 'dados' (nomes em campos_disponiveis). Use quando "
                                      "a resposta vier grande ou cortada."},
            "max_linhas": {"type": "integer", "minimum": 1, "maximum": 2000, "default": MAX_LINHAS_MCP,
                           "description": "Teto de linhas por tabela/lista."},
        }},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
]


def _resultado(texto, erro=False):
    return {"content": [{"type": "text", "text": texto}], "isError": erro}


def _chamar_ferramenta(app, base_context, nome, args):
    args = args or {}
    try:
        if nome == "listar_telas":
            telas, drills = _catalogo(app)
            visao = args.get("visao")
            if visao:
                telas = [t for t in telas if t["visao"] == visao]
                drills = [d for d in drills if d["visao"] == visao]
            out = {"filtros_comuns": _FILTROS_COMUNS, "telas": telas}
            if args.get("incluir_drills"):
                out["drills"] = drills
            return _resultado(_caber(out))
        if nome == "opcoes_de_filtro":
            return _resultado(_caber(opcoes_de_filtro(app, base_context, args.get("caminho"),
                                                      args.get("filtros"))))
        if nome == "ler_tela":
            if not args.get("caminho"):
                raise ErroFerramenta("Informe 'caminho' (ex. /overview).")
            corpo = ler_tela(app, base_context, args["caminho"], args.get("filtros"),
                             args.get("campos"), _int(args.get("max_linhas"), MAX_LINHAS_MCP, hi=2000))
            return _resultado(_caber(corpo))
    except ErroFerramenta as e:
        return _resultado(str(e), erro=True)
    except Exception as e:   # erro da tela em si: devolve pro agente em vez de derrubar o protocolo
        app.logger.exception("mcp: %s falhou", nome)
        return _resultado(f"Falha ao executar {nome}: {type(e).__name__}: {e}", erro=True)
    return None


def _erro_rpc(id_, codigo, msg):
    return {"jsonrpc": "2.0", "id": id_, "error": {"code": codigo, "message": msg}}


def _tratar(app, base_context, versao_app, m):
    if not isinstance(m, dict) or m.get("jsonrpc") != "2.0":
        return _erro_rpc(m.get("id") if isinstance(m, dict) else None, -32600, "Requisição JSON-RPC inválida")
    if "method" not in m:          # resposta do cliente a algo nosso: não pedimos nada
        return None
    id_, metodo, params = m.get("id"), m["method"], m.get("params") or {}
    notificacao = "id" not in m
    if metodo.startswith("notifications/"):
        return None
    if metodo == "initialize":
        pedida = params.get("protocolVersion")
        res = {
            "protocolVersion": pedida if pedida in PROTOCOLOS else PROTOCOLOS[0],
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "pepeuhacker", "title": "Dashboard de vendas Tractian (pepeuhacker)",
                           "version": versao_app or "dev"},
            "instructions": _INSTRUCOES,
        }
    elif metodo == "ping":
        res = {}
    elif metodo == "tools/list":
        res = {"tools": FERRAMENTAS}
    elif metodo == "tools/call":
        res = _chamar_ferramenta(app, base_context, params.get("name"), params.get("arguments"))
        if res is None:
            return _erro_rpc(id_, -32602, f"Ferramenta desconhecida: {params.get('name')}")
    elif metodo in ("resources/list", "prompts/list"):
        res = {metodo.split("/")[0]: []}
    else:
        return None if notificacao else _erro_rpc(id_, -32601, f"Método não suportado: {metodo}")
    return None if notificacao else {"jsonrpc": "2.0", "id": id_, "result": res}


def init(app, base_context, versao_app=""):
    """Registra /mcp. Chamar DEPOIS de todas as rotas (o catálogo lê o url_map)."""

    @app.route("/mcp", methods=["GET", "POST", "DELETE"], endpoint="mcp")
    def mcp():
        # Sem stream SSE nem sessão: GET/DELETE respondem 405, como a spec permite
        # a servidor sem estado.
        if request.method != "POST":
            return Response(status=405, headers={"Allow": "POST"})
        # Proteção contra DNS rebinding pedida pela spec: navegador manda Origin;
        # agente não. Origin de outro host = página de terceiro tentando usar a
        # sessão de quem está logado.
        origem = request.headers.get("Origin")
        if origem and urllib.parse.urlsplit(origem).netloc != request.host:
            return Response(_dump(_erro_rpc(None, -32600, "Origin não permitido")), status=403,
                            mimetype="application/json")
        msg = request.get_json(force=True, silent=True)
        if msg is None:
            return Response(_dump(_erro_rpc(None, -32700, "JSON inválido")), status=400,
                            mimetype="application/json")
        if isinstance(msg, list):
            respostas = [r for r in (_tratar(app, base_context, versao_app, x) for x in msg) if r]
            return Response(_dump(respostas), mimetype="application/json") if respostas else Response(status=202)
        r = _tratar(app, base_context, versao_app, msg)
        if r is None:
            return Response(status=202)
        return Response(_dump(r), mimetype="application/json")
