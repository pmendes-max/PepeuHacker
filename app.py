"""Pepeu Hacker — Sales performance dashboard for Tractian.
Flask app serving on localhost. Data source: BigQuery (tractian-bi)."""
import math
import os
import re
import socket
import subprocess
import threading
import time
import urllib.parse
from datetime import date, datetime, timedelta, timezone

# FUSO DE BRASÍLIA, FIXO. O container do Cloud Run roda em UTC, e o código deste
# arquivo sempre falou em horário de Brasília — "12:00 BRT" em `_last_two_sundays`,
# "hoje" em `_today()`. Nas três horas entre 21h e meia-noite os dois discordavam:
# o dashboard já virava o dia (e, em 30/09 e 31/12, o TRIMESTRE) enquanto ainda
# era ontem pra quem estava olhando. Descoberto em 15/09/2026 pelo selo de
# frescor, que mostrava "dado de 17:02" às 14h.
#
# Offset fixo e não `ZoneInfo("America/Sao_Paulo")` de propósito: o Brasil
# acabou com o horário de verão em 2019, então -03:00 vale o ano inteiro, e a
# imagem `python:3.11-slim` não garante o banco de fusos do sistema — um
# ZoneInfo sem tzdata levanta exceção no import e derruba o app inteiro.
import numpy as np
import pandas as pd
from flask import Flask, request, redirect, jsonify
from flask import render_template as _flask_render_template
from flask import url_for as _flask_url_for

import agent_api
import bq
import hubspot

app = Flask(__name__)


class _JsonSemNaN(app.json_provider_class):
    """`jsonify` que escreve NaN/±inf como `null`.

    O `json` do Python escreve `NaN` cru, que NÃO é JSON: o `JSON.parse` do
    navegador aborta a resposta inteira e o drill morre com "Erro ao carregar".
    A causa de sempre é `r.get("x") or "—"` sobre célula vazia do pandas —
    `float('nan')` é truthy e passa pelo `or`. O padrão está em ~25 drills
    (`_txt_cel` conserta um a um, mas o próximo drill reabre o bug); em
    12/10/2026 derrubou o drill do Unit Price (venda sem AE). Saneando aqui,
    nenhuma resposta sai inválida, e o drill já mostra `null` como "—"."""

    @staticmethod
    def _limpa(o):
        if isinstance(o, float):
            return o if math.isfinite(o) else None
        if isinstance(o, dict):
            return {k: _JsonSemNaN._limpa(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_JsonSemNaN._limpa(v) for v in o]
        return o

    def dumps(self, obj, **kwargs):
        return super().dumps(self._limpa(obj), **kwargs)


app.json = _JsonSemNaN(app)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True
app.jinja_env.globals.update(zip=zip, enumerate=enumerate)


def render_template(template_name, **context):
    """O `render_template` do Flask com uma saída a mais: `?format=json` devolve
    os dados que a tela desenharia, em vez do HTML. É a porta dos agentes de IA
    (agent_api.py) — mesma função de rota, mesmos números, nenhuma conta repetida."""
    if agent_api.quer_json():
        return agent_api.resposta_da_tela(template_name, context)
    return _flask_render_template(template_name, **context)


def _asset_v(filename: str) -> str:
    """mtime do arquivo em /static — usado pra cache-bust automático."""
    try:
        path = os.path.join(app.root_path, "static", filename)
        return str(int(os.path.getmtime(path)))
    except OSError:
        return "0"

app.jinja_env.globals["asset_v"] = _asset_v


# ---- Idioma da interface (26/10/2026): PT-BR, ES, EN ----
# A tradução acontece no navegador (static/i18n.js + o dicionário do idioma em
# static/i18n/<lang>.js, gerado por tools/i18n/gerar.py). Aqui só: qual idioma
# a pessoa escolheu (cookie `lang`, gravado pelo seletor do cabeçalho) e a lista
# de nomes de gente que o tradutor nunca toca. PT é o original: nada carrega.
IDIOMAS = (("pt", "PT-BR"), ("es", "ES"), ("en", "EN"))
_IDIOMA_HTML = {"pt": "pt-BR", "es": "es", "en": "en"}


def _ui_lang() -> str:
    from flask import has_request_context
    if not has_request_context():
        return "pt"
    v = (request.cookies.get("lang") or "pt").lower()
    return v if v in _IDIOMA_HTML else "pt"


_I18N_NOMES = {"t": 0.0, "v": []}


def _i18n_nomes() -> list:
    """Nomes de pessoas que aparecem nas telas (roster, líderes, México, dono de
    qualquer deal) e a forma curta deles ("A. Hernández"). O tradutor troca
    cada um por {nome} antes de procurar a frase no dicionário — então
    "Fulano · Saldo" traduz pra qualquer Fulano, e nome nunca é traduzido.
    Recalcula de hora em hora (gente nova entra no HubSpot)."""
    if _I18N_NOMES["v"] and time.time() - _I18N_NOMES["t"] < 3600:
        return _I18N_NOMES["v"]
    nomes = set(LEADER_ORDER) | set(LEADER_ROLE)
    for ros in ROSTERS_BR.values():
        nomes |= set(ros)
    nomes |= set(MX_ROSTER) | set(MX_DISPLAY.values())
    for fn in (_get_pipeline_deals, _get_demand_deals, _get_forecast_deals, _mx_deals):
        try:
            nomes |= set(fn()["account_executive"].dropna().astype(str).unique())
        except Exception:  # noqa: BLE001 — sem a fonte, segue com o que tem
            pass
    nomes = {n.strip() for n in nomes if isinstance(n, str) and len(n.strip()) > 3}
    nomes |= {short_name(n) for n in nomes if " " in n}
    _I18N_NOMES.update(t=time.time(), v=sorted(n for n in nomes if len(n) > 3))
    return _I18N_NOMES["v"]


app.jinja_env.globals.update(ui_lang=_ui_lang, IDIOMAS=IDIOMAS, IDIOMA_HTML=_IDIOMA_HTML,
                             i18n_nomes=_i18n_nomes)


# Allowlist: only these leaders' data is shown anywhere in the dashboard.
# Hides US/MX leaders + BR leaders not in this list (Francisco Maia, Pedro Mendes,
# Victor Ferraz, João Vitor Granzotti Machado, "No leader assigned").
# Líderes válidos POR PAÍS. Isto NÃO pode ser um set global, e o motivo é um bug
# que aconteceu: eu tinha posto o gestor do México num único set compartilhado,
# achando que o nome "não existe no funil BR". Só que a curada
# `account_executives_performance` atravessa países — e esse gate era a ÚNICA
# coisa segurando o México fora do Brasil. Resultado: 22 linhas de vendedores
# mexicanos entraram no dashboard do BR (dropdown de AE, ranking, quota).
#
# Agora o conjunto responde à requisição: numa tela do BR só líder brasileiro
# passa; numa do MX, só o gestor mexicano. Assim o filtro que já existia em ~30
# lugares vira, sozinho, a fronteira entre os dois países — sem editar nenhum
# deles e sem depender de alguém lembrar de filtrar região na tela nova.
_ALLOWED_LEADERS_BR = frozenset({
    "Leonardo Scopel",
    "Fernando Barati",
    "Bernardo Agosti",
    "João Tavares",
    "Gustavo Giovani",
    "Arthur Tkotz",        # RSM Mid Market (player-coach) — pod sob Fernando (Q3/2026)
})
# México: um gestor só, sem camada intermediária (ver ORG_CHART).
_ALLOWED_LEADERS_MX = frozenset({"Mauricio Del Hierro"})


class _LideresDaRegiao:
    """Conjunto de líderes válidos na requisição atual.

    Se comporta como set (é usado em `.isin(...)`, `sorted(...)`, `&`, `|`), mas
    resolve o conteúdo na hora da chamada. Escolhido em vez de trocar os ~30
    `isin(ALLOWED_LEADERS)` por uma função: cada um desses seria uma chance de
    esquecer, e esquecer aqui é silencioso — vira dado de outro país na tela."""

    def _s(self) -> frozenset:
        return _ALLOWED_LEADERS_MX if _is_mx() else _ALLOWED_LEADERS_BR

    def __iter__(self):      return iter(self._s())
    def __contains__(self, x): return x in self._s()
    def __len__(self):       return len(self._s())
    def __or__(self, o):     return self._s() | set(o)
    def __ror__(self, o):    return set(o) | self._s()
    def __and__(self, o):    return self._s() & set(o)
    def __rand__(self, o):   return set(o) & self._s()
    def __sub__(self, o):    return self._s() - set(o)
    def __rsub__(self, o):   return set(o) - self._s()
    def __eq__(self, o):     return self._s() == o
    def __hash__(self):      return hash(self._s())
    def __repr__(self):      return f"LideresDaRegiao({sorted(self._s())})"


ALLOWED_LEADERS = _LideresDaRegiao()

# AEs desligados — escondidos dos charts e dropdowns (não aparecem em lugar nenhum).
DEPARTED_AES = {
    "Paulo de Paula",
    # Time do Bernardo
    "Lucas Feu",
    "Lucas Omar",
    "Paula Teófilo",
    "Thiago Libanore",
    # Time do Fernando
    "João Cadorin",
    "Matheus Siqueira",
    # Time do Leonardo
    "Diego Santos",
    # Saídas adicionais
    "Carlos Franco",
}

# Pessoas que NÃO devem aparecer como AE em nenhum chart, mesmo tendo deals
# históricos atribuídos: líderes (VP/Diretores/Gerentes), pessoas de outros times,
# ou usuários técnicos que viraram "qualifier" temporariamente. Aplicado junto
# com DEPARTED_AES como blacklist universal.
NON_AE_USERS = {
    # Hierarquia (devem aparecer só como `leader`, nunca como AE)
    "Pedro Mendes",        # VP
    "Leonardo Scopel",     # Diretor Enterprise
    "Fernando Barati",     # Diretor Mid Market
    "Bernardo Agosti",     # Diretor Commercial
    "Gustavo Giovani",     # Gerente Enterprise
    "João Tavares",        # Gerente Commercial
}

# União usada nos filtros — qualquer pessoa nesse set fica escondida dos charts.
class _EscondidosDaRegiao:
    """Quem some das visões por AE, no país da requisição.

    No Brasil: quem saiu + os diretores/gerentes. No México: o gestor do país —
    quem "saiu" lá continua visível de propósito, porque é ele que carrega o
    histórico (o roster mexicano é o funil inteiro, não uma lista curada)."""

    def _s(self) -> frozenset:
        return frozenset(NON_AE_USERS_MX) if _is_mx() else frozenset(DEPARTED_AES | NON_AE_USERS)

    def __iter__(self):        return iter(self._s())
    def __contains__(self, x): return x in self._s()
    def __len__(self):         return len(self._s())
    def __or__(self, o):       return self._s() | set(o)
    def __ror__(self, o):      return set(o) | self._s()
    def __and__(self, o):      return self._s() & set(o)
    def __rand__(self, o):     return set(o) & self._s()
    def __sub__(self, o):      return self._s() - set(o)
    def __rsub__(self, o):     return set(o) - self._s()
    def __repr__(self):        return repr(sorted(self._s()))


HIDDEN_FROM_AE_VIEWS = _EscondidosDaRegiao()


# ------------------------------------------------------------------
# Duas exclusões DIFERENTES, que por muito tempo foram a mesma lista
# ------------------------------------------------------------------
# `HIDDEN_FROM_AE_VIEWS` juntava dois motivos incompatíveis de esconder alguém:
#
#   · DEPARTED_AES  — saiu da empresa. O deal dele não é de ninguém: some de
#                     tudo, inclusive dos totais.
#   · NON_AE_USERS  — gestor/diretor que fecha deal próprio. O deal EXISTE e é
#                     do time dele; ele só não é um vendedor.
#
# Tratar os dois igual apagava o funil dos gestores das telas de pipeline: o
# ganho aparecia (o realizado atribui o líder a ele mesmo), mas os R$ 357 mil
# abertos em nome deles não apareciam em lugar nenhum — o deal só existia no dia
# em que fechava. Regra correta, pedida em 24/08/2026: aparece no dashboard do
# LÍDER, nunca no ranking de VENDEDORES.
#
# Por isso as duas funções abaixo, e por isso elas têm nomes que dizem a
# intenção: no frame de dados usa-se `drop_departed_people` (o gestor FICA); só
# onde a saída é uma linha/barra POR AE usa-se `drop_non_ae_people`.

def drop_departed_people(df: pd.DataFrame) -> pd.DataFrame:
    """Tira quem saiu da empresa. Gestor/diretor CONTINUA no frame — o deal dele
    é do time e precisa aparecer em pipeline, forecast e totais."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns:
        return df
    return df[~df["account_executive"].isin(DEPARTED_AES)]


def _nao_vendedores() -> set:
    """Quem nunca é linha/barra POR AE — gestores, no país da requisição."""
    return NON_AE_USERS_MX if _is_mx() else NON_AE_USERS


def drop_non_ae_people(df: pd.DataFrame) -> pd.DataFrame:
    """Tira gestor/diretor. Usar SÓ imediatamente antes de agregar por AE (ranking
    de vendedores, barra por pessoa, dropdown de AE) — nunca no frame que alimenta
    total, pipeline ou visão por líder, senão o deal do gestor some de novo."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns:
        return df
    return df[~df["account_executive"].isin(_nao_vendedores())]

# AEs que saíram A PARTIR de um quarter específico: mantêm o histórico ANTES do
# cutoff, mas somem do cutoff em diante. Diferente de DEPARTED_AES (que apaga o
# histórico inteiro). Chave = nome EXATO do BQ; valor = 1º dia do quarter a partir
# do qual a pessoa some. Aplicado nos loaders de dados (propaga p/ todas as telas).
DEPARTED_FROM = {
    "Victor Paravani": date(2026, 7, 1),   # sai no Q3/2026 — mantém Q2
    "Raul Tezza":      date(2026, 7, 1),
    # Fora do roster do Q4/2026 (planilha "Brazil BP Q4 2026") — mantêm o Q3.
    # Em 08/10/2026 Diego, Victor Hugo, Gabriel Faria e Raphael ainda tinham
    # ~R$ 1,6 mi de funil aberto no nome deles no HubSpot; esses deals somem das
    # telas até serem redistribuídos (aí aparecem sozinhos no novo dono).
    "Diego Iasbeck":       date(2026, 10, 1),
    "Victor Hugo Borges":  date(2026, 10, 1),
    "Gabriel Faria":       date(2026, 10, 1),
    "Raphael Boccaletti":  date(2026, 10, 1),
    "Rayssa Holanda":      date(2026, 10, 1),
    "Flavio Correa":       date(2026, 10, 1),
    # NÃO saiu: segue RSM do pod no Q4 (líder no roster e no ORG_CHART). Está
    # aqui porque, sem quota no Q4, o Pedro pediu em 19/10/2026 que ele suma de
    # todo dado em que aparece como VENDEDOR (dono do deal) dali em diante. O
    # corte é por dono do deal, então o time dele continua inteiro.
    # EXCEÇÃO (ver DEPARTED_FROM_KEEP_WON): venda FECHADA dele continua contando.
    "Arthur Tkotz":        date(2026, 10, 1),
}

# Exceção ao corte acima para VENDA FECHADA (closedwon). Pedido do Pedro em
# 04/11/2026: a Crown Embalagens (Arthur, R$ 80k, PO 26/10/2026) sumia de todas
# as telas por causa do corte de 19/10. O pipeline aberto dele segue fora (o
# pedido de 19/10 continua valendo pra funil); o que ele FECHA entra como venda
# de gestor — líder = ele mesmo, fora das barras de vendedor, dentro do pod
# dele, do Fernando (ORG_CHART) e do total.
DEPARTED_FROM_KEEP_WON = {"Arthur Tkotz"}


def drop_departed_from(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Remove linhas de AEs em DEPARTED_FROM cujo `date_col` >= o cutoff deles,
    preservando o histórico anterior. Linhas sem data (NaT) são mantidas."""
    if (df is None or len(df) == 0 or "account_executive" not in df.columns
            or date_col not in df.columns or not DEPARTED_FROM):
        return df
    col = df[date_col]
    if pd.api.types.is_datetime64_any_dtype(col):
        col = col.dt.date                       # normaliza p/ comparar com date
    drop = pd.Series(False, index=df.index)
    for ae, cutoff in DEPARTED_FROM.items():
        m = df["account_executive"] == ae
        if m.any():
            drop |= m & col.notna() & (col >= cutoff)
    return df[~drop]

# Nomes duplicados do MESMO AE na fonte: o mapa e a canonização moram em
# `bq.AE_NAME_ALIASES` / `bq.canon_ae_names`, aplicados dentro de `bq.query()` —
# ou seja, TODA leitura já chega canonizada aqui (ver o comentário longo em bq.py
# com o caso do João Pedro Vellardo). Auditado: ele é o único caso no escopo BR.
def canon_ae_names(df: pd.DataFrame) -> pd.DataFrame:
    """Cópia do frame com nomes de AE canonizados. Redundante depois que o data
    layer passou a canonizar na leitura, mas mantida nos loaders porque eles
    precisavam da cópia de qualquer forma — e garante o invariante se um dia
    algum frame chegar por outro caminho."""
    if df is None or len(df) == 0:
        return df
    return bq.canon_ae_names(df.copy())


def merge_ae_quarter_dups(df: pd.DataFrame) -> pd.DataFrame:
    """Funde as linhas que `canon_ae_names` deixou duplicadas no MESMO (AE,
    quarter) na curated — cada nome da fonte carregava um pedaço da pessoa (um a
    quota/realizado, o outro o pipeline), e a fusão remonta o AE inteiro.

    Obrigatório rodar ANTES de `apply_q3_roster_curated`: com 2 linhas ele
    gravaria a quota em cada uma e a meta do time sairia dobrada.

    Numérico soma (NaN é ignorado, então quota 90k + NaN = 90k); o resto pega o
    1º valor não-nulo, com as linhas ordenadas pra a que TEM user_id vir antes —
    senão `leader` poderia herdar o "No leader assigned" da linha órfã.
    Confirmado: a curated não tem nenhuma duplicata de (AE, quarter) por conta
    própria, então esta função é um no-op fora dos aliases."""
    keys = ["account_executive", "quarter"]
    if df is None or len(df) == 0 or not set(keys) <= set(df.columns):
        return df
    dup = df.duplicated(keys, keep=False)
    if not dup.any():
        return df
    d = df[dup]
    if "user_id" in d.columns:                       # identidade real primeiro
        d = d.assign(_ord=d["user_id"].isna()).sort_values("_ord").drop(columns="_ord")
    num = set(d.select_dtypes("number").columns) - set(keys)
    agg = {c: ("sum" if c in num else "first") for c in d.columns if c not in keys}
    merged = d.groupby(keys, as_index=False, dropna=False, sort=False).agg(agg)
    return pd.concat([df[~dup], merged], ignore_index=True)


# Override manual de líder/região (legado). Vazio: a atribuição de time passou a
# ser dirigida pelo ROSTERS_BR (abaixo), que é quarter-scoped na curated (preserva
# histórico) e global no deal-level. Mantido como no-op p/ os call sites.
LEADER_OVERRIDES: dict = {}


def apply_leader_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Sobrescreve leader/region de AEs em LEADER_OVERRIDES (legado, hoje vazio)."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns:
        return df
    for ae, ov in LEADER_OVERRIDES.items():
        mask = df["account_executive"] == ae
        if mask.any():
            for col, val in ov.items():
                if col in df.columns:
                    df.loc[mask, col] = val
    return df


# ============================================================
# Extensão do fechamento do quarter — SÓ para RECEITA REALIZADA.
# Quando a empresa estende o fechamento, o PO validado nos primeiros dias do
# quarter seguinte conta como realizado do quarter estendido, como se tivesse
# sido validado no último dia dele. É o que o registro de venda do Ops faz: o
# Embalatec (deal 62704492076) está como PO Won em 01/10/2026 no HubSpot e
# validado em 30/09/2026 no Ops.
#
# Todo o resto (pipeline, forecast dos abertos, ciclo, ticket, conversão,
# contagem de deals, demanda) segue o calendário normal: a extensão muda a data
# em que a RECEITA cai, não a data em que o deal aconteceu.
#
# Chave = início do quarter estendido; valor = último dia de validação aceito.
# ============================================================
REALIZED_QUARTER_EXTENSIONS: dict = {
    date(2026, 7, 1): date(2026, 10, 2),   # Q3/26: validado até 02/10/2026
}


def _rev_date(s):
    """Data de validação → data da RECEITA REALIZADA (aplica as extensões).

    Aceita Series datetime64 (devolve datetime64, preservando tz), Series de
    `date` ou um escalar (date/Timestamp). Validado dentro de uma extensão vira o
    último dia do quarter estendido; qualquer outra data passa intacta. Use em
    TODA janela, quarter ou atribuição de líder do realizado — e em nenhum outro
    lugar."""
    if not REALIZED_QUARTER_EXTENSIONS:
        return s
    if isinstance(s, pd.Series):
        if pd.api.types.is_datetime64_any_dtype(s):
            out = s.copy()
            tz = getattr(s.dt, "tz", None)
            for qs, ext_end in REALIZED_QUARTER_EXTENSIONS.items():
                q_end = pd.Timestamp(_quarter_end_of(qs))
                lim = pd.Timestamp(ext_end) + pd.Timedelta(days=1)
                if tz is not None:
                    q_end, lim = q_end.tz_localize(tz), lim.tz_localize(tz)
                m = (s >= q_end + pd.Timedelta(days=1)) & (s < lim)
                # Mantém a hora/tz do Timestamp original? Não: a receita cai no
                # DIA final do quarter; meia-noite basta pra qualquer janela.
                out = out.mask(m, q_end)
            return out
        return s.map(_rev_date)
    if s is None or (not isinstance(s, (date, pd.Timestamp))) or pd.isna(s):
        return s
    d = s.date() if isinstance(s, (pd.Timestamp, datetime)) else s
    for qs, ext_end in REALIZED_QUARTER_EXTENSIONS.items():
        q_end = _quarter_end_of(qs)
        if q_end < d <= ext_end:
            if isinstance(s, pd.Timestamp):
                return pd.Timestamp(q_end, tz=s.tz)
            if isinstance(s, datetime):
                return datetime.combine(q_end, datetime.min.time(), tzinfo=s.tzinfo)
            return q_end
    return s


def _rev_tag(d) -> str:
    """Selo curto pros drills: "conta no Q3/26" quando a data de validação caiu
    numa extensão de fechamento (a receita foi para o quarter anterior); "" no
    caso normal. A data exibida no drill continua sendo a real."""
    if d is None or pd.isna(d):
        return ""
    r = _rev_date(d)
    if r is None or pd.isna(r) or r == d:
        return ""
    rd = r.date() if isinstance(r, datetime) else r
    return f"conta no {_quarter_label(_quarter_start_of(rd))}"


def rev_ext_note(date_from=None, date_to=None) -> str:
    """Aviso de uma linha pro card de Realizado quando a janela encosta numa
    extensão de fechamento: o fim do quarter estendido está dentro dela (o
    realizado INCLUI validados depois do fim) ou ela começa nos dias da extensão
    (o realizado EXCLUI os validados nesses dias). "" no caso normal. Janela
    vazia = quarter de hoje, o default das telas."""
    try:
        lo = pd.to_datetime(date_from).date() if date_from else _quarter_start_of(_today())
        hi = pd.to_datetime(date_to).date() if date_to else _quarter_end_of(_today())
    except Exception:
        return ""
    if lo > hi:
        return ""
    for qs, ext_end in REALIZED_QUARTER_EXTENSIONS.items():
        q_end, lbl = _quarter_end_of(qs), _quarter_label(qs)
        # Janela que já contém o fim do quarter E os dias da extensão: mover a
        # data não muda nada, e o aviso só confundiria.
        if lo <= q_end <= hi < ext_end:
            return (f"{lbl} inclui PO validado até {ext_end:%d/%m} "
                    "(fechamento estendido)")
        if q_end < lo <= ext_end:
            return (f"validados até {ext_end:%d/%m} contam no {lbl} "
                    "(fechamento estendido)")
    return ""


app.jinja_env.globals["rev_ext_note"] = rev_ext_note


# ============================================================
# Rosters POR TRIMESTRE — fonte da verdade informada pela liderança (planilha).
# Cada um define TIME (leader = RSM/gerente direto), QUOTA (MRR, BRL) e META DE
# NEW LOGOS por AE do SEU trimestre. Chave = nome EXATO (canônico) no BigQuery.
# Aplicado:
#   • curated  → cada roster só nas linhas do próprio quarter (histórico intacto)
#   • deal-level → o roster do trimestre VIGENTE, global por nome (1 líder = time atual)
#   • telas com período → o roster do trimestre quando a janela cabe nele
#     (`_roster_window`); fora disso, a quota da curated.
# Mudou o time num trimestre novo? Crie o dict dele e registre em ROSTERS_BR —
# o dos trimestres anteriores NÃO se edita (é o que a tela mostra pro passado).
# ============================================================
# Q3/2026. Arthur Tkotz é player-coach (tem quota própria E gerencia um pod sob o Fernando).
Q3_START = date(2026, 7, 1)
Q3_END   = date(2026, 9, 30)
_Q3_ROSTER_BR = {
    # Bernardo Agosti (Commercial + Mid Market)
    "Felipe Martins":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Rafael Avellar":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 28000,  "new_logos": 2},
    # ATENÇÃO: na base ele é "Diego Peter", NÃO "Diego Loures" — o comentário
    # antigo aqui afirmava o contrário e por isso o erro passou. O casamento é
    # feito por bq.AE_NAME_ALIASES; sem ele esta chave não encontra deal nenhum.
    "Diego Loures":                        {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 5000,   "new_logos": 1},  # "Diego Loures Peter" — na base: "Diego Peter"
    "Pedro Ferri":                         {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 28000,  "new_logos": 2},
    "Matheus Bonfim":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 23666,  "new_logos": 2},
    "Matheus Stanford":                    {"leader": "Bernardo Agosti", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    # João Tavares (Commercial, sob Bernardo)
    # "Daniel Jordão" (planilha) = "Daniel Del Paschoa" (BigQuery) — mesma pessoa.
    # Time do João Tavares por instrução do usuário (05/ago/2026): ele NÃO reporta
    # direto ao Bernardo. Isso REVERTE as instruções de 15/jul e 31/jul, que
    # tinham fixado Bernardo aqui contra a planilha — agora a linha volta a
    # espelhar a coluna RSM da planilha de hierarquia Q3, e os 30 AEs ficam todos
    # consistentes com ela. O `leader` raw do BQ ainda diz Bernardo (fonte velha);
    # a instrução do usuário manda. Bernardo continua vendo o Daniel nas telas,
    # via ORG_CHART (João é subordinado dele, expand_leader é transitivo).
    #
    # NÃO precisa de alias em bq.AE_NAME_ALIASES. Verificado no BQ em 05/ago/2026:
    # "Daniel Jordão" NÃO EXISTE em nenhuma fonte — o nome só vive na planilha da
    # liderança. Na base ele é uma identidade única e limpa: um só nome, um só
    # id (89634343), 176 deals desde 2022-11 e a quota pendurada nesse mesmo id.
    # Os outros "Daniel*" do BQ são pessoas diferentes (Ito é US/Michael Smith;
    # Prates, Wilson, Tavares e Olalde são outros). Ou seja: não há nome partido
    # ao meio aqui, o caso não é o do João Pedro Vellardo.
    "Daniel Del Paschoa":                  {"leader": "João Tavares",    "tier": "Commercial", "quota": 28000,  "new_logos": 2},
    "Douglas Barbosa":                     {"leader": "João Tavares",    "tier": "Commercial", "quota": 28000,  "new_logos": 2},
    "Rayssa Holanda":                      {"leader": "João Tavares",    "tier": "Commercial", "quota": 23666,  "new_logos": 2},
    "Luca Dalmazi":                        {"leader": "João Tavares",    "tier": "Commercial", "quota": 0,      "new_logos": 0},
    "Diego Iasbeck":                       {"leader": "João Tavares",    "tier": "Mid Market", "quota": 70000,  "new_logos": 4},
    # Arthur Tkotz (Mid Market pod, sob Fernando) — player-coach
    "João Pedro Vellardo Kouyomdjian":     {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Julio Camargo":                       {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Arthur Tkotz":                        {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 125000, "new_logos": 4},
    "Breno Smanio":                        {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 9000,   "new_logos": 1},
    # Fernando Barati (Mid Market direto)
    "Flávio Domingues":                    {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Bruno Brunelli":                      {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Francisco Burns":                     {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Maria Eduarda Da Silva Pasqualinotto":{"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Victor Hugo Borges":                  {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 49500,  "new_logos": 2},
    "Gabriel Faria":                       {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 9000,   "new_logos": 1},
    # Gustavo Giovani (Enterprise) — reporta direto ao Pedro Mendes (VP)
    "Matheus Farias":                      {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 105000, "new_logos": 4},
    "Dantt Oliveira":                      {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 4},
    "Gabriel Martins":                     {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 4},
    "Thiago Ribeiro":                      {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 58335,  "new_logos": 2},
    "Flavio Correa":                       {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 58335,  "new_logos": 2},
    "Vinicius Borges":                     {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 4},
    # Leonardo Scopel (Enterprise direto)
    "Caio Mesquita":                       {"leader": "Leonardo Scopel", "tier": "Enterprise", "quota": 125000, "new_logos": 4},
    "Lucas Allgayer":                      {"leader": "Leonardo Scopel", "tier": "Enterprise", "quota": 125000, "new_logos": 4},
    "Raphael Boccaletti":                  {"leader": "Leonardo Scopel", "tier": "Enterprise", "quota": 87500,  "new_logos": 2},
}

# Q4/2026 — planilha "Brazil BP Q4 2026" (aba AE Capacity), recebida em 08/10/2026.
# `leader` = coluna RSM; a coluna "Líder" é o diretor, que o ORG_CHART já resolve
# (Bernardo ⊃ João, Fernando ⊃ Arthur, Pedro ⊃ Leonardo e Gustavo).
# Mudanças contra o Q3:
#   · saíram do time: Diego Iasbeck, Victor Hugo Borges, Gabriel Faria, Raphael
#     Boccaletti, Rayssa Holanda e Flavio Correa (ver DEPARTED_FROM);
#   · Arthur Tkotz deixa de carregar quota: fica só RSM do pod. Em 19/10/2026 o
#     Pedro pediu que o deal próprio dele saia de TUDO no Q4 (não conta nem como
#     venda de gestor): ver DEPARTED_FROM;
#   · 19/10/2026: Francisco Burns passa pro pod do Arthur e Julio Camargo vai
#     direto pro Fernando. Os dois têm R$ 90 mil de quota, então a capacity de
#     cada time e os Comp. Targets derivados não mudam;
#   · 21/10/2026: Rafael Avellar e Diego Peter (chave "Diego Loures") saem da
#     gestão direta do Bernardo e vão pro time do João Tavares, mesma quota. O
#     Comp. Target do João sobe com os R$ 77,5 mil de capacity; o do Bernardo
#     não muda (o João está dentro dele);
#   · entram Bruno Almeida, Pedro Capriotti e Gabriel Silva (rampa) e Thomas
#     Talarico / Luigi Mantoni, que começam até ~22/10 e ainda NÃO existem no
#     HubSpot — quota 0 no Q4; quando o owner aparecer, conferir se o nome casa
#     com a chave daqui (senão, alias em bq.AE_NAME_ALIASES).
# "Diego Peter" na planilha = "Diego Loures" aqui (alias em bq.py).
Q4_START = date(2026, 10, 1)
Q4_END   = date(2026, 12, 31)
_Q4_ROSTER_BR = {
    # Bernardo Agosti (Commercial + Mid Market)
    "Felipe Martins":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Pedro Ferri":                         {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Matheus Bonfim":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 42667,  "new_logos": 3},
    "Matheus Stanford":                    {"leader": "Bernardo Agosti", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Bruno Almeida":                       {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 5000,   "new_logos": 1},
    # João Tavares (RSM Commercial, sob o Bernardo)
    "Daniel Del Paschoa":                  {"leader": "João Tavares",    "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Douglas Barbosa":                     {"leader": "João Tavares",    "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Luca Dalmazi":                        {"leader": "João Tavares",    "tier": "Commercial", "quota": 23666,  "new_logos": 2},
    "Pedro Capriotti":                     {"leader": "João Tavares",    "tier": "Commercial", "quota": 5000,   "new_logos": 1},
    "Rafael Avellar":                      {"leader": "João Tavares",    "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Diego Loures":                        {"leader": "João Tavares",    "tier": "Commercial", "quota": 27500,  "new_logos": 2},
    # Arthur Tkotz (RSM Mid Market, sob o Fernando) — sem quota própria no Q4
    "João Pedro Vellardo Kouyomdjian":     {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Francisco Burns":                     {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Breno Smanio":                        {"leader": "Arthur Tkotz",    "tier": "Mid Market", "quota": 49500,  "new_logos": 3},
    # Fernando Barati (Mid Market)
    "Flávio Domingues":                    {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 49500,  "new_logos": 3},
    "Bruno Brunelli":                      {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Julio Camargo":                       {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Maria Eduarda Da Silva Pasqualinotto":{"leader": "Fernando Barati", "tier": "Mid Market", "quota": 90000,  "new_logos": 4},
    "Gabriel Silva":                       {"leader": "Fernando Barati", "tier": "Mid Market", "quota": 9000,   "new_logos": 1},
    # Gustavo Giovani (Enterprise, direto ao Pedro)
    "Matheus Farias":                      {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 2},
    "Dantt Oliveira":                      {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 2},
    "Gabriel Martins":                     {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 2},
    "Thiago Ribeiro":                      {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 106261, "new_logos": 2},
    "Vinicius Borges":                     {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 125000, "new_logos": 2},
    "Luigi Mantoni":                       {"leader": "Gustavo Giovani", "tier": "Enterprise", "quota": 0,      "new_logos": 0},
    # Leonardo Scopel (Enterprise, direto ao Pedro)
    "Caio Mesquita":                       {"leader": "Leonardo Scopel", "tier": "Enterprise", "quota": 125000, "new_logos": 2},
    "Lucas Allgayer":                      {"leader": "Leonardo Scopel", "tier": "Enterprise", "quota": 125000, "new_logos": 2},
    "Thomas Talarico":                     {"leader": "Leonardo Scopel", "tier": "Enterprise", "quota": 0,      "new_logos": 0},
}

# Trimestre → roster do BRASIL. A ordem não importa; o vigente é o mais recente
# que já começou (`_roster_quarter_current`).
ROSTERS_BR = {
    Q3_START: _Q3_ROSTER_BR,
    Q4_START: _Q4_ROSTER_BR,
}
# region do roster = BR (todo o time). Mapa nome→região p/ deal-level.
_ROSTER_REGION_BR = "BR"


def _roster_region() -> str:
    """Região que o roster estampa. Era a constante "BR" fixa, e por isso o
    Forecast do México mostrava 'BR' na coluna Região de 6 dos 8 vendedores
    mexicanos — e qualquer filtro de região passava a mentir."""
    return "MX" if _is_mx() else _ROSTER_REGION_BR


ROSTER_REGION = _ROSTER_REGION_BR

# Os rosters acima sao do BRASIL, e eram percorridos sem olhar o pais: na tela do
# Mexico eles injetavam os 30 brasileiros no ranking (e linhas sinteticas deles na
# curada). Mesmo remedio do ALLOWED_LEADERS — o conteudo passa a responder a
# requisicao, entao os ~20 usos espalhados nao precisam saber de pais nenhum.
#
# No MX o roster sai da tabela do BI (`_mx_roster`): o escopo de la e o funil
# inteiro (decisao do usuario), entao quem manda no universo de vendedores e a
# lista fixa MX_ROSTER, com a quota do trimestre em dolar.


def _roster_quarter_current() -> date:
    """Trimestre do roster VIGENTE: o mais recente que já começou. Num trimestre
    novo ainda sem roster, segue o último declarado (melhor time conhecido)."""
    hoje = _quarter_start_of(_today())
    started = [q for q in ROSTERS_BR if q <= hoje]
    return max(started) if started else min(ROSTERS_BR)


def _roster_of(q) -> dict:
    """Roster do trimestre `q` no país da requisição ({} se não houver)."""
    if q is None:
        return {}
    if _is_mx():
        return _mx_roster(q) if q in ROSTERS_BR else {}
    return ROSTERS_BR.get(q, {})


def _roster_window(from_d, to_d):
    """Início do trimestre cujo roster vale para a janela [from_d, to_d], ou None
    quando ela não cabe inteira num trimestre com roster (aí a quota sai da
    curated, somada nos trimestres que a janela toca)."""
    if from_d is None or to_d is None:
        return None
    q = _quarter_start_of(from_d)
    return q if (q in ROSTERS_BR and to_d <= _quarter_end_of(q)) else None


class _RosterDaRegiao(dict):
    """Dict do roster VIGENTE que resolve o conteudo na hora da chamada, por pais
    e por data (vira sozinho no primeiro dia de um trimestre com roster novo).

    Herda de dict pra continuar valendo em `in`, `[]`, `.items()`, `.get()`,
    `len()` e no teste de verdade — os usos existentes ficam intactos."""

    def _d(self) -> dict:
        return _roster_of(_roster_quarter_current())

    def __iter__(self):        return iter(self._d())
    def __len__(self):         return len(self._d())
    def __contains__(self, k): return k in self._d()
    def __getitem__(self, k):  return self._d()[k]
    def __bool__(self):        return bool(self._d())
    def get(self, k, d=None):  return self._d().get(k, d)
    def items(self):           return self._d().items()
    def keys(self):            return self._d().keys()
    def values(self):          return self._d().values()
    def __repr__(self):        return repr(self._d())


ROSTER = _RosterDaRegiao()

# ============================================================
# Câmbio p/ reporte global. O time BR é medido em MRR (BRL); a receita é reportada
# ao global em ARR (USD).  ARR_USD = MRR_BRL × 12 ÷ USD_BRL_RATE  (e o inverso).
# >>> Pra mudar a taxa do dólar: edite USD_BRL_RATE aqui (ou defina a env var
#     USD_BRL_RATE) e reinicie o app. Hoje: R$ 5,05 por US$.
# ============================================================
USD_BRL_RATE = float(os.environ.get("USD_BRL_RATE", "5.05"))
MONTHS_PER_YEAR = 12  # MRR → ARR


def mrr_brl_to_arr_usd(mrr_brl) -> float:
    """MRR em BRL → ARR em USD (anualiza ×12 e converte pela taxa do dólar).

    No México o valor JÁ chega em USD (a conversão de moeda acontece na volta da
    query, em bq.to_usd), então aqui só anualiza — dividir pelo câmbio de novo
    encolheria o número em 5x."""
    v = to_float(mrr_brl) * MONTHS_PER_YEAR
    if _is_mx():
        return v
    return (v / USD_BRL_RATE) if USD_BRL_RATE else 0.0


def arr_usd_to_mrr_brl(arr_usd) -> float:
    """ARR em USD → MRR em BRL (converte pela taxa e desanualiza ÷12)."""
    return to_float(arr_usd) * USD_BRL_RATE / MONTHS_PER_YEAR


# Meta de RECEITA do time no Q3 (top-down). ANCORADA em ARR USD — é o alvo
# reportado ao global (fonte da verdade); o valor em MRR BRL é DERIVADO pela taxa
# atual (muda junto se o câmbio mudar). É MAIOR que a soma das quotas individuais
# (R$2.041.002) — overassignment. Mostrada DIRETO no card "Meta de receita" na
# visão do time inteiro; ao filtrar por líder/AE cai na soma das quotas do escopo.
# >>> Pra mudar a meta: edite Q3_TEAM_REVENUE_META_ARR_USD (em ARR US$).
Q3_TEAM_REVENUE_META_ARR_USD = 5_950_000
# MRR BRL derivado (~2.503.958). Aritmética direta — não usa to_float (que só é
# definido mais abaixo) porque isto roda no import.
Q3_TEAM_REVENUE_META = Q3_TEAM_REVENUE_META_ARR_USD * USD_BRL_RATE / MONTHS_PER_YEAR

# Q4/2026: US$ 6.765.000 ARR — planilha "BP Q4 2026" (aba AE Capacity, abaixo do
# total da quota Q4: capacity US$ 4.422.263,72 → meta 152,98% dela). Mesma
# convenção do Q3: ancorada em ARR USD, MRR BRL derivado pelo câmbio.
Q4_TEAM_REVENUE_META_ARR_USD = 6_765_000

# Q2/2026: US$ 5.905.000 ARR — passada pelo Pedro em 13/10/2026 ("a quota do Q2
# era 5905"). O Q2 não tem roster no app: o Comp. Target de cada líder sai da
# capacity do time NAQUELA época (linhas curadas do Q2), ver `_comp_targets_from_roster`.
# Só a cobertura histórica do PoP Histórico Q lê esta entrada: as demais telas
# chegam à meta por `_roster_window`, que não devolve trimestre sem roster.
Q2_START = date(2026, 4, 1)
Q2_TEAM_REVENUE_META_ARR_USD = 5_905_000

# Trimestre → meta top-down do time (MRR BRL). Trimestre fora daqui não tem meta
# top-down: o card cai na soma das quotas do escopo.
TEAM_REVENUE_META = {
    Q2_START: Q2_TEAM_REVENUE_META_ARR_USD * USD_BRL_RATE / MONTHS_PER_YEAR,
    Q3_START: Q3_TEAM_REVENUE_META,
    Q4_START: Q4_TEAM_REVENUE_META_ARR_USD * USD_BRL_RATE / MONTHS_PER_YEAR,
}


def apply_q3_roster_curated(df: pd.DataFrame) -> pd.DataFrame:
    """Em cada quarter com roster, sobrescreve leader e quota_in_brl das linhas
    dos AEs dele. Quarter-scoped → preserva o histórico dos outros quarters.
    Requer coluna `quarter` (date). (Nome mantido pelos call sites; hoje vale
    pra todos os rosters de ROSTERS_BR, não só o do Q3.)

    Do Q4/2026 em diante o roster também é EXCLUSIVO na quota: AE fora dele no
    trimestre fica com quota 0 na curated. A tabela do BI segue com meta pra quem
    saiu ou virou gestor (Arthur Tkotz com R$ 125 mil no Q4), e essa meta vazava
    em toda janela que atravessa trimestres. O Q3 fica como sempre foi."""
    if df is None or len(df) == 0 or "quarter" not in df.columns:
        return df
    for q in ROSTERS_BR:
        roster = _roster_of(q)
        if not roster:
            continue
        is_q = df["quarter"] == q
        for ae, v in roster.items():
            m = is_q & (df["account_executive"] == ae)
            if m.any():
                df.loc[m, "leader"] = v["leader"]
                if "region" in df.columns:
                    df.loc[m, "region"] = _roster_region()
                if "quota_in_brl" in df.columns:
                    df.loc[m, "quota_in_brl"] = v["quota"]
        # Líder sem quota no trimestre (Arthur no Q4): a linha dele vira de
        # gestor — líder ele mesmo. O BI o pendura no Gustavo Giovani.
        for L in _leaders_without_quota(q):
            m = is_q & (df["account_executive"] == L)
            if m.any():
                df.loc[m, "leader"] = L
        if (q >= ROSTER_QUOTA_EXCLUSIVE_FROM and not _is_mx()
                and "quota_in_brl" in df.columns):
            out = is_q & ~df["account_executive"].isin(list(roster))
            if out.any():
                df.loc[out, "quota_in_brl"] = 0.0
                if "new_logos_quota" in df.columns:
                    df.loc[out, "new_logos_quota"] = 0
    return df


# A partir deste trimestre, quem não está no roster não carrega quota (ver acima).
ROSTER_QUOTA_EXCLUSIVE_FROM = Q4_START


# Marca de ORIGEM da linha na curada: False = medida pelo BI, True = declarada pelo
# roster do trimestre. Existe porque depois do concat "AE que o BI ainda não mediu" e "AE com
# realizado zero medido" ficam indistinguíveis, e as duas coisas se leem de forma
# oposta num ranking. Nunca recebe pd.NA: a coluna serve de máscara e `~pd.NA`
# propaga NA — um `df[~df[flag]]` devolveria frame VAZIO em vez das linhas reais.
# Sem underscore inicial de propósito: a curada é consumida por
# `itertuples(index=False)`, que renomeia coluna com underscore inicial para _1/_2.
_ROSTER_SYNTH_FLAG = "is_synthetic_roster_row"

# Colunas de REALIZADO que a linha sintética declara como 0. Critério estreito e
# verificável: entra só o que é ADITIVO e mede o PRÓPRIO trimestre, porque "nenhum
# deal ganho" (auditado na fonte deal-level) implica aritmeticamente 0 em cada uma
# — inclusive nas colunas em USD, já que 0 não depende de câmbio. Fica FORA de
# propósito tudo que é razão, ciclo, snapshot de pipeline ou janela multi-trimestre
# (ver o bloco 0-vs-NULO em `inject_roster_missing_curated`).
_ROSTER_SYNTH_ZERO = (
    "new_mrr_in_brl", "new_mrr_in_brl_first_po", "new_mrr_in_brl_expansion",
    "new_mrr_in_usd", "new_mrr_in_usd_first_po", "new_mrr_in_usd_expansion",
    "net_mrr_in_brl", "net_mrr_in_usd", "new_arr_in_usd", "net_arr_in_usd",
    "mrr_churned_usd", "arr_churned_usd", "mrr_smart_trac_usd",
    "mrr_energy_trac_usd", "mrr_tracos_usd", "mrr_first_po_usd",
    "mrr_expansion_usd", "qty_deals_sold", "qty_new_logos",
    "qty_deals_acquisition_sold", "qty_deals_expansion_sold", "deals_won",
    # 0.0 (e não nulo) blinda os dois consumidores de hold: `_identify_hold_deal_ids`
    # faz `if v and float(v) > 0` sobre o .max() do AE, e bool(pd.NA) LEVANTA
    # TypeError se essa coluna algum dia virar nullable na fonte.
    "total_mrr_on_hold_brl",
)


def inject_roster_missing_curated(df: pd.DataFrame) -> pd.DataFrame:
    """Cria na curada a linha que falta pra AE nomeado no roster de um trimestre
    (ROSTERS_BR; só os trimestres já começados).

    INVARIANTE: cada roster é a fonte da verdade de QUEM está no time no trimestre
    dele (o texto abaixo fala do Q3, onde isso nasceu — vale igual pro Q4); a
    curada é a fonte do HISTÓRICO. Onde as duas discordam sobre a EXISTÊNCIA da
    pessoa, o roster ganha — mas só dentro do trimestre dele. No fim, todo nome do
    roster tem exatamente UMA linha em Q3_START (a real, se houver; senão a
    sintética) e nenhuma linha nova em qualquer outro trimestre.

    Complementa `apply_q3_roster_curated` sem sobrepor: aquela SOBRESCREVE máscara
    de linha existente, esta só CRIA pra quem tem ZERO linha em Q3. Os dois
    conjuntos são disjuntos por construção — nenhum nome passa pelos dois caminhos,
    logo a quota do time não pode ser contada duas vezes. Genérico por nome do
    roster, sem exceção hardcoded: a próxima contratação é coberta ao ser adicionada
    lá, e o mecanismo se auto-aposenta por AE no dia em que a curada trouxer a linha
    real. Roda DEPOIS de `canon_ae_names`/`merge_ae_quarter_dups` porque o teste de
    ausência tem de ser feito contra o nome CANÔNICO: julgado pelo alias, um AE que
    existe só sob outro nome ganharia linha sintética AO LADO da real, duplicando a
    pessoa e dobrando a quota.

    Contratação nova só entra na curada no fechamento do trimestre, e até lá o AE
    existe no deal-level (`apply_roster_leader`) sem ter linha própria: os deals dele
    inflam o total do time e ele mesmo não aparece em dropdown, ranking nem quota
    individual. É essa assimetria que a linha sintética fecha — o caminho deal-level
    não é tocado, então nenhum deal é contado duas vezes.

    Quarter-scoped por CONSTRUÇÃO, não por cláusula defensiva: `quarter` é o literal
    Q3_START, sem loop de trimestre, reindex de calendário, ffill ou merge que possa
    fazer broadcast — o frame cresce em `len(missing)` linhas, todas em Q3_START, e
    não existe caminho de código capaz de produzir outro trimestre. Linha em
    trimestre anterior faria a série do /overview/overtime mostrar quota pra quem
    não estava na casa.

    0 vs NULO: 0 é uma AFIRMAÇÃO ("vendeu nada"); NULO é ausência de medição. Só
    recebe 0 o que "0 deal ganho" implica aritmeticamente (`_ROSTER_SYNTH_ZERO`);
    esse 0 vale como "a pessoa nasceu neste trimestre". Ticket médio, taxa de
    conversão e atingimento são razão de denominador vazio — 0 ali entra numa média
    de time como performance ruim MEDIDA, e pro AE de quota 0 é literalmente 0/0.
    Ciclo em dias é pior: 0 dia é o MELHOR valor da métrica, o inverso exato de
    "nunca fechou". Contador e valor de PIPELINE ficam nulos porque 0 seria mentira
    verificável (eles TÊM deals abertos, e a autoridade disso é o deal-level, não
    esta linha). Janela de 4 trimestres não existe pra quem entrou no meio do Q3.
    Quota em USD e o split expansão/first-PO ficam nulos porque o roster declara
    UMA quota (MRR BRL): derivar aqui gravaria o câmbio de hoje numa coluna que a
    fonte calcula com o câmbio da época, e criaria rateio que a liderança não
    declarou. `user_id` nulo é funcional, não cosmético — é o marcador natural de
    "esta linha não veio do BigQuery".
    """
    if (df is None or len(df) == 0
            or "quarter" not in df.columns
            or "account_executive" not in df.columns):
        return df
    df = df.copy()
    if _ROSTER_SYNTH_FLAG in df.columns:
        # Coluna homônima já no frame — ou a fonte passou a expor esse nome, ou
        # alguém chamou o helper duas vezes. Avisa alto em vez de reescrever calado,
        # mas não levanta: a curada alimenta TODAS as telas, e derrubar o dashboard
        # inteiro é pior que perder a rastreabilidade de origem.
        print(f"pepeuhacker · AVISO: coluna {_ROSTER_SYNTH_FLAG} já existia e foi"
              f" reescrita — esse nome é da marca de origem do roster", flush=True)
    df[_ROSTER_SYNTH_FLAG] = pd.array([False] * len(df), dtype="boolean")

    # Ausência testada por (nome, quarter), não "ausente da tabela inteira": o
    # invariante que as telas precisam é por trimestre, e assim o caso "AE antigo
    # que a curada ainda não fechou no trimestre" também fica coberto. Só
    # trimestres já começados: roster de trimestre futuro não cria linha.
    cur_q = _roster_quarter_current()
    missing, miss_q, miss_cfg = [], [], []
    for q in sorted(ROSTERS_BR):
        if q > cur_q:
            continue
        roster = _roster_of(q)
        have = set(df.loc[df["quarter"] == q, "account_executive"].dropna())
        for ae, cfg in roster.items():
            if ae not in have:
                missing.append(ae); miss_q.append(q); miss_cfg.append(cfg)
    if not missing:
        return df
    n = len(missing)

    # Molde tipado pelo PRÓPRIO frame: a fatia vazia reindexada nasce com todas as
    # colunas, na ordem da fonte, cada uma no seu dtype e já preenchida com o NA
    # CORRETO do dtype (NaN em float64, pd.NA em Int64/boolean). É o único jeito de
    # obter "linha vazia com a cara do frame" sem enumerar dtype na mão — e uma
    # coluna nova na curada passa a nascer nula de graça, em vez de virar KeyError.
    # `pd.concat` de dict/lista-de-dicts NÃO serve: derruba as colunas Int64 e
    # boolean pra float64/object, e a comparação/soma quebra silenciosamente em
    # outra tela.
    syn = df.iloc[:0].reindex(index=range(n))
    vals = {
        "account_executive": missing,
        # quarter/quarter_end como datetime.date: os loaders já converteram a coluna
        # (`.dt.date` → object-de-date) e é nesse tipo que `quarter == Q3_START`
        # casa. Timestamp aqui faria o recorte por trimestre (apply_filters,
        # apply_q3_roster_curated, _period_leader_map) deixar de casar. quarter_end
        # é obrigatório: é por ele que `apply_filters` corta a borda de baixo.
        "quarter": miss_q,
        "quarter_end": [_quarter_end_of(q) for q in miss_q],
        "leader": [c["leader"] for c in miss_cfg],
        "region": [_roster_region()] * n,
        "role": ["AE"] * n,
        "is_currently_active": [True] * n,
        "quota_in_brl": [c["quota"] for c in miss_cfg],
        "new_logos_quota": [c["new_logos"] for c in miss_cfg],
        _ROSTER_SYNTH_FLAG: [True] * n,
    }
    for c in _ROSTER_SYNTH_ZERO:
        vals[c] = [0] * n
    vals = {c: v for c, v in vals.items() if c in syn.columns}
    for c, v in vals.items():
        syn[c] = v
    # Atribuir lista Python rebaixa o dtype (boolean→bool, Int64→int64, e o 0 int
    # numa coluna float64 viraria int64); o astype devolve cada coluna PREENCHIDA ao
    # dtype do frame — e é ele que adapta o valor cru do roster ao tipo da fonte.
    # Restrito às preenchidas de propósito: astype nas que ficaram NA estouraria
    # TypeError tentando float(pd.NA).
    syn = syn.astype({c: df[c].dtype for c in vals})
    out = pd.concat([df, syn], ignore_index=True)
    drift = [c for c in df.columns if out[c].dtype != df[c].dtype]
    if drift:
        print(f"pepeuhacker · AVISO: dtype mudou no concat do roster: {drift}",
              flush=True)
    return out


def _leaders_without_quota(q=None) -> set:
    """Líderes da hierarquia (RSM/gerente) que NÃO carregam quota no roster do
    trimestre `q` (default: o vigente). O deal próprio deles vira venda de
    gestor: o líder é ele mesmo e ele não entra como vendedor. Nasceu pro Arthur
    Tkotz, player-coach no Q3/2026 e só RSM no Q4 — derivado do roster, sem
    lista à parte, pra que o próximo "virou só gestor" saia sozinho da planilha.
    Só BR: o México não tem camada intermediária."""
    if _is_mx():
        return set()
    roster = _roster_of(q if q is not None else _roster_quarter_current())
    if not roster:
        return set()
    return {L for L in _ALLOWED_LEADERS_BR if L not in roster} - NON_AE_USERS


def _managers_in(q=None) -> set:
    """Quem é GESTOR (não vendedor) no trimestre `q`: NON_AE_USERS + líderes sem
    quota no roster dele. Use onde a linha por AE marca `is_manager`."""
    return (NON_AE_USERS_MX if _is_mx() else NON_AE_USERS) | _leaders_without_quota(q)


def apply_roster_leader(df: pd.DataFrame) -> pd.DataFrame:
    """Deal-level: atribui leader/region do ROSTER vigente a todos os deals dos AEs do
    roster (deal-level usa 1 líder = time atual). Global por nome. Usado no
    /pipeline (visão corrente). Rotas com histórico (overview) reatribuem por
    quarter via _attr_deal_leader pra não vazar o roster pro passado.

    Deal de GESTOR/DIRETOR recebe ele mesmo como líder — a venda direta é do time
    dele. É a mesma regra que `_attr_deal_leader` já aplicava no realizado, mas
    ela vivia SÓ lá: nos frames de pipeline o gestor vinha com `leader` NULO
    (o join do BQ monta `ae_meta` com `role = 'AE'`, e gestor não é AE), então o
    gate seguinte — `leader in ALLOWED_LEADERS` — descartava o deal mesmo depois
    de tirado da lista de escondidos. Era esse o motivo real de o funil dos
    gestores não aparecer, e não a lista de exclusão sozinha."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns:
        return df
    if ROSTER:
        lead = df["account_executive"].map({a: v["leader"] for a, v in ROSTER.items()})
        m = lead.notna()
        if m.any():
            df.loc[m, "leader"] = lead[m]
            if "region" in df.columns:
                df.loc[m, "region"] = _roster_region()
    # Gestor → líder de si mesmo. `region` junto, senão o filtro de região
    # (que compara `== 'BR'`) derrubaria o deal por causa de um NULO.
    mg = df["account_executive"].isin(NON_AE_USERS | _leaders_without_quota())
    if mg.any():
        if "leader" not in df.columns:
            df["leader"] = pd.NA
        df.loc[mg, "leader"] = df.loc[mg, "account_executive"]
        if "region" in df.columns:
            df.loc[mg, "region"] = _roster_region()
    return df


# ------------------------------------------------------------------
# Regiao da requisicao: as MESMAS telas servem BR e MX
# ------------------------------------------------------------------
# O caminho decide o pipeline. `/mx/...` roda exatamente as views do BR — o que
# muda e (a) qual funil o BigQuery le e (b) a camada de time. Foi assim, e nao
# duplicando as telas, porque "tudo identico" so e sustentavel se houver UM
# codigo: tela duplicada diverge no primeiro ajuste que alguem esquecer de
# aplicar nos dois lados.
def _is_mx() -> bool:
    from flask import has_request_context
    return has_request_context() and request.path.startswith("/mx/")


# Endpoint do BR -> endpoint espelho do MX. Preenchido lá embaixo, junto das
# rotas; consultado em tempo de chamada, por isso pode nascer vazio aqui.
_MX_ENDPOINT_BY_TARGET: dict = {}


def url_for(endpoint, **values):
    """`url_for` ciente da região.

    Numa tela do México, um `url_for('pipeline_total_deals')` escrito no template
    do Brasil apontava pra `/pipeline/total/deals` — e o clique trazia os deals
    BRASILEIROS pra dentro da tela do México. Como as views são as MESMAS, o
    template não tem como saber em que país está; quem sabe é a requisição. Por
    isso o remapeamento mora aqui, num lugar só, e vale pro Python e pro Jinja.
    """
    if _is_mx():
        endpoint = _MX_ENDPOINT_BY_TARGET.get(endpoint, endpoint)
    return _flask_url_for(endpoint, **_drill_carry(endpoint, values))


def _mx_path(path: str) -> str:
    """Prefixa /mx nos caminhos que o código monta À MÃO (drill_path dos rank
    charts). Mesmo problema do url_for, em string crua."""
    if _is_mx() and path.startswith("/") and not path.startswith("/mx/"):
        return "/mx" + path
    return path


_MX_PIPELINE = "Sales MX"


def _mx_equivalente(path: str) -> str:
    """A tela do México que corresponde a `path` (do AE ou do Leaders do BR), ou
    o Overview do México se ela não existir lá (ex.: Sales Overtime)."""
    if path.startswith("/mx/"):
        return path
    for mx, br in _MX_PATH_TO_BR.items():
        if br == path:
            return mx
    sem_leaders = path.replace("/leaders/", "/", 1) if path.startswith("/leaders/") else path
    for mx, br in _MX_PATH_TO_BR.items():
        if br == sem_leaders:
            return mx
    return "/mx/overview/sales"


def _pl() -> str:
    """Pipeline do HubSpot a consultar nesta requisicao."""
    return _MX_PIPELINE if _is_mx() else bq.SALES_BR


def _apply_roster_current(df: pd.DataFrame, cut_from: date | None = None) -> pd.DataFrame:
    """Frames de SNAPSHOT vindos direto do BQ (pipeline stalled/funnel/wow) trazem
    o leader/region CRU da SQL (não-roster). Reatribui pelo ROSTER vigente e remove quem
    já saiu (cutoff <= hoje) — espelha o que _get_pipeline_deals faz. Visão corrente
    (não histórica), então usa o time atual global.

    `cut_from` troca "quem já saiu (cutoff <= hoje)" por "quem já tinha saído em
    DATA X" — é o que uma série HISTÓRICA precisa: DEPARTED_FROM é a lista de
    quem mantém o histórico ANTES do cutoff, e cortar por hoje apagava o passado
    inteiro dessas pessoas (media R$ 70 mil, 6,3% do último ponto do Q2/2026, só
    do Raul Tezza — e a aba Forecast, que corta por data, nunca fechava com a
    série). Usa-se o INÍCIO da janela, e não a data de cada linha, pra série não
    ganhar um degrau artificial: se a pessoa saiu no meio, o degrau seria a saída
    dela, não movimento de pipeline."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns:
        return df
    if _is_mx():
        # O MX nao tem roster nem hierarquia na base (metade do time esta como
        # "No leader assigned"). Em vez de abrir excecao em cada um dos ~30
        # lugares que filtram por ALLOWED_LEADERS, todo deal do MX recebe o
        # gestor do pais como lider: os gates passam, o escopo e o funil inteiro
        # e nenhuma tela do BR foi tocada.
        df = df.copy()
        df["leader"] = MX_LEADER
        if "region" in df.columns:
            df["region"] = "MX"
        return df
    df = apply_roster_leader(df.copy())
    _ref = cut_from or _today()
    _gone = {ae for ae, cut in DEPARTED_FROM.items() if cut <= _ref}
    if _gone:
        df = df[~df["account_executive"].isin(_gone)]
    return df


def _period_leader_map(force: bool = False) -> dict:
    """Mapa (account_executive, quarter_start:date) -> leader, do curated já com
    o roster Q3 aplicado (quarter-scoped) e histórico preservado. Base pra atribuir
    cada deal ao líder do SEU quarter (evita o split-brain quota×realizado)."""
    clean = _get_clean_ae_perf(force=force)
    return {(r.account_executive, r.quarter): r.leader
            for r in clean[["account_executive", "quarter", "leader"]].itertuples(index=False)}


def _attr_deal_leader(frame: pd.DataFrame, date_col: str, lmap: dict) -> list:
    """Líder de cada deal = líder do curated no quarter da data do deal (date_col).
    Fallback: gestor→ele mesmo; senão roster do quarter; senão o leader atual do frame.
    Corrige atribuição histórica (Q2 fica no líder de Q2, Q3 no do roster)."""
    if not len(frame):
        return []
    out = []
    for ae, dt, cur in zip(frame["account_executive"], frame[date_col], frame["leader"]):
        q = _quarter_start_of(dt.date()) if pd.notna(dt) else None
        ld = lmap.get((ae, q))
        if ld is None:
            if ae in NON_AE_USERS or ae in _leaders_without_quota(q):
                ld = ae                                   # venda direta do gestor
            elif ae in _roster_of(q if q in ROSTERS_BR else None):
                ld = _roster_of(q)[ae]["leader"]          # AE novo do roster sem linha curated
            else:
                ld = cur                                  # fallback: leader do snapshot
        out.append(ld)
    return out


def _identify_hold_deal_ids(deals: pd.DataFrame, force: bool = False) -> set:
    """Deal_ids em HOLD (em aprovação financeira). O `total_mrr_on_hold_brl` da
    curada é um snapshot no quarter corrente; aqui casamos o valor de cada AE (só
    ALLOWED_LEADERS) com seus closedwon+PO recentes (1 ou 2 deals, tolerância R$2)
    pra descobrir QUAIS deals compõem o hold. Retorna o set de deal_ids — eles são
    tratados como hold (não realizado), atribuídos ao quarter do PO do deal."""
    ids: set = set()
    aeh = _get_ae_perf_with_managers(force=force)
    if aeh is None or not len(aeh):
        return ids
    aeh = aeh[aeh["leader"].isin(ALLOWED_LEADERS)]
    onhold = {a: float(v) for a, v in
              aeh.groupby("account_executive")["total_mrr_on_hold_brl"].max().items()
              if v and float(v) > 0}
    if not onhold or "deal_id" not in deals.columns:
        return ids
    cw = deals[deals["is_won"] & deals["po_emission_date"].notna()].copy()
    if len(cw):   # só candidatos recentes (~2 quarters) — o hold é sempre recente
        _cut = cw["po_emission_date"].max() - pd.Timedelta(days=210)
        cw = cw[cw["po_emission_date"] >= _cut]
    for ae, H in onhold.items():
        rows = [(r.deal_id, float(r.amount))
                for r in cw[cw["account_executive"] == ae][["deal_id", "amount"]].itertuples(index=False)]
        match = None
        for did, amt in rows:                       # 1 deal
            if abs(amt - H) < 2:
                match = [did]; break
        if match is None:                           # par de deals
            for i in range(len(rows)):
                for j in range(i + 1, len(rows)):
                    if abs(rows[i][1] + rows[j][1] - H) < 2:
                        match = [rows[i][0], rows[j][0]]; break
                if match:
                    break
        if match:
            ids.update(match)
    return ids


# Organograma: VP → Diretores → Gerentes → AEs.
# Selecionar um líder no filtro inclui ele + todos os subordinados (transitivo).
ORG_CHART = {
    # Gustavo Giovani reporta DIRETO ao Pedro Mendes (VP) desde 15/jul/2026 —
    # não é mais subordinado do Leonardo Scopel.
    # Mauricio Del Hierro (México) reporta DIRETO ao Pedro Mendes (VP) — instrução
    # de 24/08/2026. É o único gestor dos 7 vendedores do MX; não há camada
    # intermediária no país, ao contrário do BR (diretor → gerente → AE).
    "Pedro Mendes":    ["Leonardo Scopel", "Fernando Barati", "Bernardo Agosti",
                        "Gustavo Giovani", "Mauricio Del Hierro"],
    "Leonardo Scopel": [],
    "Fernando Barati": ["Arthur Tkotz"],
    "Bernardo Agosti": ["João Tavares"],
    "Gustavo Giovani": [],
    "João Tavares":    [],
    "Arthur Tkotz":    [],
    "Mauricio Del Hierro": [],
}

# Ordem hierárquica do dropdown (VP → Diretores → Gerentes/RSMs).
LEADER_ORDER = [
    "Pedro Mendes",
    "Leonardo Scopel", "Fernando Barati", "Bernardo Agosti",
    "Gustavo Giovani", "Arthur Tkotz", "João Tavares",
    "Mauricio Del Hierro",
]

# Meta de RECEITA de cada diretor/gerente = "Comp. Target" (planilha da liderança).
# Substitui a soma das quotas dos AEs do time em TODAS as views de líder (overview
# ranking, cobertura de pipeline, forecast). Valores em MRR BRL.
#
# NÃO CONFUNDIR com "quota capacity" = a soma das quotas dos AEs do time. São dois
# números diferentes e o dash mostra os DOIS: capacity vem do BQ por AE (`quota_brl`
# / `quota_by_ae`, caminho intocado); o Comp. Target é só este dict e só aparece no
# grain de líder. Ponto de aviso do usuário em 05/ago/2026.
#
# Valores atualizados em 05/ago/2026 (planilha "Comp. Target" mandada pelo usuário).
# Os anteriores estavam TODOS ~16% altos e inflavam a meta de todo mundo:
#   Bernardo 532.565,15 · João 212.931,02 · Fernando 1.042.133,66
#   Arthur 446.730,33 · Leonardo 480.163,97 · Gustavo 848.887,22
# (a razão novo/antigo é 0,862319 idêntica nos 6 — foi revisão de fator, não
#  recálculo individual.)
#
# >>> INVARIANTE QUE FECHA ESTES NÚMEROS (o melhor teste de sanidade que existe
# aqui; era exatamente isto que estava QUEBRADO antes de 05/ago):
#
#   soma dos Comp. Targets dos 4 líderes de TOPO  ==  Q3_TEAM_REVENUE_META
#   (topo = Leonardo, Fernando, Bernardo, Gustavo — Arthur e João são
#    subordinados e NÃO entram na soma; é o que _leader_meta_total faz)
#
#   novos:    R$ 2.503.958,34  vs meta R$ 2.503.958,33  → fecha em 1 CENTAVO
#   antigos:  R$ 2.903.750,00  vs meta R$ 2.503.958,33  → R$ 399.791,67 ACIMA
#
# Ou seja: o dash mostrava "Meta de receita R$ 2,50M" no card do time e, na
# mesma tela, alvos de líder que somavam R$ 2,90M. Era essa a incoerência.
#
# E a mecânica da planilha, que explica o fator único:
#   Comp. Target(líder) = capacity(time dele) x (Q3_TEAM_REVENUE_META / capacity total)
#                       = capacity x 1,226828        (2.503.958,33 / 2.041.002)
# — a meta top-down é DISTRIBUÍDA proporcional à quota capacity de cada time.
# Confere com o fator medido nos 6 targets (spread 4e-08, só arredondamento).
#
# TRÊS COISAS DEIXAM ESTE DICT STALE, e nenhuma avisa sozinha:
#   1) mexer no roster do trimestre (mover AE, mudar quota) → muda a capacity;
#   2) mudar Q3_TEAM_REVENUE_META_ARR_USD → muda o numerador;
#   3) mudar USD_BRL_RATE → a meta em BRL é derivada do câmbio.
# Em qualquer um dos casos, recalcular pela fórmula acima e reconferir a soma.
#
# Bônus: foi por esse fator que se confirmou que o alvo do João já embute o
# Daniel Del Paschoa — com o Daniel o fator dele casa com os outros 5 na 7ª
# decimal, sem o Daniel dispara pra 1,509.
_LEADER_BOARD_TARGET_Q3 = {
    "Bernardo Agosti":   459240.97,
    "João Tavares":      183614.43,
    "Fernando Barati":   898651.49,
    "Arthur Tkotz":      385223.98,
    "Leonardo Scopel":   414054.44,
    "Gustavo Giovani":   732011.44,
}


def _comp_targets_from_roster(q) -> dict:
    """Comp. Target por líder DERIVADO do roster + meta top-down do trimestre —
    a mesma mecânica da planilha (ver acima): capacity(time) × meta / capacity
    total. Os 4 líderes de topo somam a meta exata por construção, então o
    invariante deste bloco não tem como quebrar ao mexer no roster. É o caminho
    do Q4/2026 em diante; o Q3 fica com os números digitados da planilha.

    Trimestre com meta e SEM roster (Q2/2026): a capacity sai das linhas curadas
    do trimestre, com o líder que cada AE tinha nelas — "proporcional à capacity
    do time na época", pedido do Pedro. A hierarquia entre líderes é a de hoje
    (Fernando ⊃ Arthur, Bernardo ⊃ João)."""
    roster, meta = ROSTERS_BR.get(q, {}), TEAM_REVENUE_META.get(q)
    if not roster and meta:
        cur = _get_clean_ae_perf()
        cur = cur[pd.to_datetime(cur["quarter"]).dt.date == q]
        roster = {a: {"quota": float(v), "leader": ld} for (a, ld), v in
                  cur.groupby(["account_executive", "leader"])["quota_in_brl"].sum().items()}
    total = sum(float(v["quota"]) for v in roster.values())
    if not roster or not meta or not total:
        return {}
    fator = float(meta) / total
    out = {}
    for L in _ALLOWED_LEADERS_BR:
        team = _expand_leader_em(L, q)
        cap = sum(float(v["quota"]) for v in roster.values() if v["leader"] in team)
        if cap:
            out[L] = round(cap * fator, 2)
    return out


# Q4/2026 (capacity R$ 1.858.094 · meta US$ 6,765 mi = R$ 2.846.937,50 · fator 1,532181),
# conferido em 08/10/2026:
#   Bernardo 680.032,77 (⊃ João 197.139,68) · Fernando 854.957,35 (⊃ Arthur 351.635,68)
#   Gustavo 928.901,97 · Leonardo 383.045,41  → topo soma R$ 2.846.937,50.
# 21/10/2026, com Rafael Avellar e Diego Peter no time do João: João 315.883,76
# (os outros não mudam; o topo segue somando a meta).
LEADER_TARGETS_BY_QUARTER = {Q3_START: _LEADER_BOARD_TARGET_Q3}
_LEADER_TARGETS_DERIVED: dict = {}


def _leader_targets(q) -> dict:
    """Comp. Target por líder do trimestre `q` ({} se não houver meta declarada:
    quem chama cai na soma das quotas do time)."""
    if q is None:
        return {}
    if _is_mx():
        # Um gestor só: a meta dele é a soma das metas declaradas dos 7 (o painel
        # do Mauricio). Sem meta declarada no trimestre, {} — quem chama cai na
        # soma das quotas do time, como no Brasil.
        quotas = _mx_quota_declarada(q)
        return {MX_LEADER: float(sum(quotas.values()))} if quotas else {}
    if q in LEADER_TARGETS_BY_QUARTER:
        return LEADER_TARGETS_BY_QUARTER[q]
    if q not in _LEADER_TARGETS_DERIVED:
        _LEADER_TARGETS_DERIVED[q] = _comp_targets_from_roster(q)
    return _LEADER_TARGETS_DERIVED[q]


class _MetaLiderVigente(dict):
    """LEADER_BOARD_TARGET = Comp. Target do trimestre CORRENTE pela data da
    RECEITA (na extensão de fechamento ainda é o anterior) — a mesma regra do
    `meta_ok` das telas de forecast. Resolve na hora da chamada, então vira
    sozinho na virada do trimestre. Telas com período usam `_leader_targets`."""

    def _d(self) -> dict:
        return _leader_targets(_quarter_start_of(_rev_date(_today())))

    def __iter__(self):        return iter(self._d())
    def __len__(self):         return len(self._d())
    def __contains__(self, k): return k in self._d()
    def __getitem__(self, k):  return self._d()[k]
    def __bool__(self):        return bool(self._d())
    def get(self, k, d=None):  return self._d().get(k, d)
    def items(self):           return self._d().items()
    def keys(self):            return self._d().keys()
    def values(self):          return self._d().values()
    def __repr__(self):        return repr(self._d())


LEADER_BOARD_TARGET = _MetaLiderVigente()

# Cargo de cada um — usado pra mostrar contexto no dropdown.
LEADER_ROLE = {
    "Pedro Mendes":    "VP",
    "Leonardo Scopel": "Diretor Enterprise",
    "Fernando Barati": "Diretor Mid Market",
    "Bernardo Agosti": "Diretor Commercial",
    "Gustavo Giovani": "Gerente Enterprise",
    "João Tavares":    "Gerente Commercial",
    "Arthur Tkotz":    "RSM Mid Market",
    "Mauricio Del Hierro": "Gerente México",
}


def expand_leader(leader: str) -> set:
    """Set de leader values na base que devem ser incluídos quando `leader` é
    selecionado no filtro. Inclui o próprio + subordinados transitivos."""
    if not leader: return set()
    out = {leader}
    for child in ORG_CHART.get(leader, []):
        out |= expand_leader(child)
    return out


# Hierarquia que valeu num trimestre passado e não vale mais — só o que MUDOU;
# o resto segue o ORG_CHART. Gustavo respondia ao Leonardo até 15/07/2026, então
# no Q2/26 o time do Leonardo inclui o dele (pedido do Pedro em 13/10/2026). Hoje
# só o PoP Histórico Q (meta, pipe, realizado, painéis e drills) lê isto.
ORG_CHART_BY_QUARTER = {
    date(2026, 4, 1): {"Leonardo Scopel": ["Gustavo Giovani"]},
}


def _expand_leader_em(leader: str, q) -> set:
    """`expand_leader` com a hierarquia do trimestre `q` (ORG_CHART_BY_QUARTER)."""
    if not leader:
        return set()
    extra = ORG_CHART_BY_QUARTER.get(q, {})
    out, fila = set(), [leader]
    while fila:
        x = fila.pop()
        if x in out:
            continue
        out.add(x)
        fila.extend(expand_leader(x) - {x})
        fila.extend(extra.get(x, []))
    return out


def _leader_meta_total(leader_names, targets=None) -> float:
    """Total da meta (Board Target) de um conjunto de líderes, contando só os de
    TOPO — exclui quem é subordinado de outro líder presente (evita somar o alvo
    do gerente dentro do do diretor). Ex.: {Fernando, Arthur} → só Fernando."""
    names = set(leader_names)
    top = [L for L in names if not any(L != o and L in expand_leader(o) for o in names)]
    t = LEADER_BOARD_TARGET if targets is None else targets
    return float(sum(t.get(L, 0.0) for L in top))


def _apply_leader_meta(rows, projection_key="projection", targets=None):
    """Põe a META de cada diretor/gerente em linhas JÁ agregadas por líder, com
    atingimento e gap. Meta = LEADER_BOARD_TARGET (o "Comp. Target" da planilha
    da liderança) — a MESMA fonte que a Cobertura de Pipeline e o Modelo
    Analytics usam, e NÃO a soma das quotas dos AEs do time (essa é a "quota
    capacity", outro número). Fallback: líder fora do dict mantém o que vier em
    `quota` (tipicamente a soma do time), pra nunca zerar a coluna."""
    for r in rows:
        t = LEADER_BOARD_TARGET if targets is None else targets
        r["quota"] = float(t.get(r["leader"], r.get("quota") or 0.0))
        proj = float(r.get(projection_key) or 0.0)
        r["attainment"] = (proj / r["quota"]) if r["quota"] else None
        r["gap"] = (r["quota"] - proj) if r["quota"] else None
    return rows


def _leader_rollup(ae_rows, leader_filter, sum_fields):
    """Agrega uma lista de linhas por-AE (dicts com 'leader'/'account_executive' e
    campos numéricos) em linhas por-LÍDER. Itera LEADER_ORDER (sem Pedro Mendes/VP);
    cada líder soma os AEs do seu time via expand_leader(L) (diretor engloba gerente).
    Respeita o filtro de líder (mostra ele + subordinados). Espelha o loop de
    leaders_demand(). Cada linha: name/short/leader/role/team_size + os sum_fields.
    account_executive = L (pros templates escritos p/ AE renderizarem sem mudança)."""
    scope = expand_leader(leader_filter) if leader_filter else (set(LEADER_ORDER) - {"Pedro Mendes"})
    out = []
    for L in [x for x in LEADER_ORDER if x != "Pedro Mendes" and x in scope]:
        subs = expand_leader(L)
        subset = [r for r in ae_rows if r.get("leader") in subs]
        if not subset:
            continue
        row = {
            "name": L, "short": short_name(L), "leader": L,
            "account_executive": L, "region": "BR",
            "role": LEADER_ROLE.get(L, "—"),
            "team_size": len({r.get("account_executive") for r in subset
                              if r.get("account_executive")}),
        }
        for fld in sum_fields:
            row[fld] = sum(float(r.get(fld) or 0) for r in subset)
        out.append(row)
    return out


def _explode_leaders(df, leader_filter):
    """Grain de líder p/ dfs deal-level: reescreve `account_executive` para o
    líder e replica cada linha uma vez por líder-ancestral (LEADER_ORDER, sem
    Pedro Mendes) cujo expand_leader contém o `leader` da linha. Assim qualquer
    pivot/groupby por account_executive sai POR LÍDER, com o diretor englobando o
    gerente. Respeita o filtro de líder. df precisa ter a coluna `leader`."""
    if not len(df):
        return df
    scope = expand_leader(leader_filter) if leader_filter else (set(LEADER_ORDER) - {"Pedro Mendes"})
    parts = []
    for L in [x for x in LEADER_ORDER if x != "Pedro Mendes" and x in scope]:
        part = df[df["leader"].isin(expand_leader(L))].copy()
        if len(part):
            part["account_executive"] = L
            parts.append(part)
    return pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0]


def _pipe_leader_df(by_ae_df, leader_filter, targets=None):
    """Rollup por LÍDER do by_ae de pipeline: cada líder soma seu time
    (expand_leader). remaining_quota recalculado no nível do líder (clip 0).
    Devolve DataFrame com as MESMAS colunas do by_ae por-AE (account_executive=L)
    pra reusar `_cov_items` sem mudança.

    `targets` = Comp. Target por líder do trimestre da janela (`_leader_targets`).
    Vazio/None (janela sem trimestre com meta declarada — ex. o Early no M3, que
    aponta pro próximo) soma a quota do time."""
    cols = ["account_executive", "leader", "region", "quota_brl", "early_amount",
            "late_amount", "early_deals", "late_deals", "validated", "hold",
            "acq", "exp", "remaining_quota"]
    scope = expand_leader(leader_filter) if leader_filter else (set(LEADER_ORDER) - {"Pedro Mendes"})
    rows = []
    for L in [x for x in LEADER_ORDER if x != "Pedro Mendes" and x in scope]:
        sub = by_ae_df[by_ae_df["leader"].isin(expand_leader(L))]
        if not len(sub):
            continue
        # Meta do líder = Board Target (planilha), não a soma das quotas do time.
        q = float((targets or {}).get(L, sub["quota_brl"].sum()))
        val = float(sub["validated"].sum()); hld = float(sub["hold"].sum())
        rows.append({
            "account_executive": L, "leader": L, "region": "BR", "quota_brl": q,
            "early_amount": float(sub["early_amount"].sum()), "late_amount": float(sub["late_amount"].sum()),
            "early_deals": int(sub["early_deals"].sum()), "late_deals": int(sub["late_deals"].sum()),
            "validated": val, "hold": hld,
            "acq": float(sub["acq"].sum()), "exp": float(sub["exp"].sum()),
            "remaining_quota": max(q - val - hld, 0.0),
        })
    return pd.DataFrame(rows, columns=cols)


# Nome curto para gráficos: inicial do primeiro nome + primeiro sobrenome real
# (pula partículas "de", "da", "do", "dos", "das", "e").
_NAME_PARTICLES = {"de", "da", "do", "dos", "das", "e"}

def short_name(full: str) -> str:
    if not full: return ""
    parts = full.split()
    if len(parts) == 1:
        return parts[0]
    first_initial = parts[0][0].upper()
    surname = parts[1]
    for w in parts[1:]:
        if w.lower() not in _NAME_PARTICLES:
            surname = w
            break
    return f"{first_initial}. {surname}"


# -------- helpers --------

def to_float(x):
    try:
        f = float(x)
        if pd.isna(f): return 0.0
        return f
    except (TypeError, ValueError):
        return 0.0


def fmt_brl(n):
    # Filtro mestre de moeda: em modo ARR US$, todo valor MRR R$ vira ARR US$
    # (×12 ÷ câmbio). Cobre KPIs, rótulos de barra, eixo dos rank charts e tooltips.
    if _money_mode() == "arr":
        return fmt_usd(mrr_brl_to_arr_usd(n))
    n = to_float(n)
    if abs(n) >= 1_000_000:
        return f"R$ {n/1_000_000:.2f}M".replace(".", ",")
    if abs(n) >= 1_000:
        return f"R$ {round(n/1_000)}k"
    return f"R$ {round(n)}"


def fmt_money(n):
    """Dinheiro em precisão cheia (com separador de milhar), sensível ao filtro
    mestre: 'R$ 1.234' (MRR) ou 'US$ 2.930' (ARR). Usado em tabelas onde antes
    era 'R$ {{ x|intg }}'."""
    if _money_mode() == "arr":
        return "US$ " + fmt_int(round(mrr_brl_to_arr_usd(n)))
    return "R$ " + fmt_int(round(to_float(n)))


def fmt_usd(n):
    n = to_float(n)
    if abs(n) >= 1_000_000:
        return f"US$ {n/1_000_000:.2f}M".replace(".", ",")
    if abs(n) >= 1_000:
        return f"US$ {round(n/1_000)}k"
    return f"US$ {round(n)}"


def fmt_int(n):
    try:
        return f"{int(n):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def fmt_pct(n, digits=1):
    return f"{to_float(n)*100:.{digits}f}%".replace(".", ",")


def fmt_pct_raw(n, digits=1):
    """When value is already a percentage (e.g. 158.9)."""
    return f"{to_float(n):.{digits}f}%".replace(".", ",")


# O Jinja tem o `url_for` dele, que ignora o wrapper do módulo — sem isto os
# templates continuariam gerando link do Brasil dentro do México.
app.jinja_env.globals["url_for"] = url_for

app.jinja_env.filters["brl"] = fmt_brl
app.jinja_env.filters["money"] = fmt_money
app.jinja_env.filters["usd"] = fmt_usd
app.jinja_env.filters["intg"] = fmt_int
app.jinja_env.filters["pct"] = fmt_pct
app.jinja_env.filters["pctraw"] = fmt_pct_raw
app.jinja_env.filters["short_name"] = short_name


def _fmt_mult(v):
    """Multiplicador de cobertura: 3.0 -> '3x', 2.5 -> '2,5x'."""
    v = float(v)
    if abs(v - round(v)) < 0.05:
        return f"{int(round(v))}x"
    return f"{v:.1f}".replace(".", ",") + "x"


def _nice_money_step(span, target=8):
    """Passo 'redondo' (1/2/2,5/5 × 10ⁿ) pra um eixo de R$ com ~`target` ticks.

    Existe pro eixo ASSINADO do Histórico PoP: dividir cada lado do zero em 4
    partes iguais espremia o lado menor — numa janela em que as perdas são 1/4
    dos ganhos, os 4 rótulos negativos saíam colados uns nos outros. Com um
    passo único, os dois lados usam a mesma régua e cada lado ganha o número de
    ticks que o seu tamanho comporta."""
    if span <= 0:
        return 1.0
    raw = span / max(target, 1)
    mag = 10.0 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


def _nice_int_axis(rawmax):
    """Ticks inteiros 0..topo p/ um eixo de contagem/multiplicador; o topo é
    arredondado PRA CIMA (nunca abaixo do máximo real, senão a barra estoura)."""
    top = int(rawmax) + (1 if rawmax > int(rawmax) else 0)
    top = max(1, top)
    if top <= 6:
        return top, list(range(top + 1))
    step = (top + 4) // 5
    top = ((top + step - 1) // step) * step
    return top, list(range(0, top + 1, step))


def _percentile(sv, p):
    """Percentil (0..1) de uma lista JÁ ORDENADA, com interpolação linear."""
    if not sv:
        return 0.0
    if len(sv) == 1:
        return float(sv[0])
    k = p * (len(sv) - 1)
    lo = int(k)
    hi = min(lo + 1, len(sv) - 1)
    return sv[lo] + (sv[hi] - sv[lo]) * (k - lo)


def _robust_top(vals):
    """Topo 'robusto' de uma lista: o MAIOR valor que NÃO é outlier pela cerca de
    Tukey (Q3 + 1.5·IQR). Assim 1 cobertura disparada (ex: quem já quase bateu a
    meta → multiplicador enorme) não achata o eixo — ela clampa no topo, mas o
    RÓTULO mostra o valor real. Funciona em qualquer tamanho de amostra (≠ percentil
    de índice, que virava no-op p/ n pequeno)."""
    sv = sorted(v for v in vals if v is not None)
    if not sv:
        return 1.0
    q1, q3 = _percentile(sv, 0.25), _percentile(sv, 0.75)
    fence = q3 + 1.5 * (q3 - q1)
    non_outliers = [v for v in sv if v <= fence]
    return max(non_outliers[-1] if non_outliers else sv[-1], 1.0)


def _build_rank_chart(items, drill_base, axis_mode="currency",
                      line_mode="percent", line_goal=1.0, drill_path="/overview",
                      goal_line=None, drill_key="ae", show_line=True, trend=False):
    """Geometria SVG do combo bar+line REUSÁVEL (padrão único de chart).

    `items`: lista JÁ ORDENADA (decrescente pela métrica da linha) de dicts com
    name/short/leader/realized/quota/attainment. Barras: Realizado (verde) +
    Quota/Meta (cinza); linha (eixo dir) = `attainment`.
    - axis_mode: eixo ESQUERDO das barras — 'currency' (R$) ou 'count' (inteiro).
    - line_mode: eixo DIREITO da linha — 'percent' (% atingimento, ex receita/new
      logos) ou 'multiplier' (Nx, ex cobertura de pipeline).
    - line_goal: limiar p/ colorir a linha de verde (hit). drill_path: rota do drill.
    - goal_line: valor (na unidade da linha) de uma META a desenhar como linha
      tracejada horizontal (ex: meta dinâmica de cobertura Late). O eixo é esticado
      p/ incluí-la; retorna `goal_y` (senão None).
    Cada row ganha `line_label` já formatado ("117%" ou "2,5x"). Retorna o dict `rk`.
    """
    _TOP, _PLOT, _BOT = 40, 400, 140
    _LEFT, _STEP, _BW, _GAP = 74, 104, 26, 20
    _base_y = _TOP + _PLOT
    _maxv = max([max(it["realized"], it["quota"]) for it in items], default=1.0) or 1.0
    _atts = [it["attainment"] for it in items if it["attainment"] is not None]
    _rawmax = max(_atts) if _atts else 1.0
    _goalv = float(goal_line) if goal_line else 0.0

    # Eixo esquerdo (barras): R$ (currency) ou contagem inteira (count).
    if axis_mode == "count":   # 'currency' (R$/US$ pelo toggle) · 'usd' (sempre US$)
        _axis_max, _ticks = _nice_int_axis(_maxv)
        _scale = float(_axis_max)
        yL = [{"label": fmt_int(t), "y": round(_base_y - t / _scale * _PLOT, 1)} for t in _ticks]
    else:
        _scale = _maxv
        # 'usd' = o valor JÁ é dólar e não deve passar pela conversão do
        # fmt_brl (que multiplica por 12 e divide pelo câmbio). É o modo do
        # Sales MX, onde a série nasce em ARR USD.
        _f = fmt_usd if axis_mode == "usd" else fmt_brl
        yL = [{"label": _f(_maxv * k / 4), "y": round(_base_y - k / 4 * _PLOT, 1)} for k in range(5)]

    # Eixo direito (linha): % de atingimento (percent) ou multiplicador Nx (multiplier).
    # A meta (goal_line) entra no topo do eixo p/ ficar sempre visível.
    if line_mode == "multiplier":
        # topo robusto (p95) — 1 outlier não achata o eixo; pontos acima são
        # clampados no topo (min(att,_maxa)) mas o rótulo mostra o valor real.
        _atop, _ra_ticks = _nice_int_axis(max(_robust_top(_atts), _goalv, 1.0))
        _maxa = float(_atop)
        yR = [{"label": _fmt_mult(t), "y": round(_base_y - t / _maxa * _PLOT, 1)} for t in _ra_ticks]
        _line_lbl = _fmt_mult
    else:
        _maxa = max(_rawmax, _goalv, 1.0)
        _maxa = (int(_maxa / 0.25 - 1e-9) + 1) * 0.25          # ceil a 25%
        yR = [{"label": str(int(round(_maxa * k / 4 * 100))) + "%", "y": round(_base_y - k / 4 * _PLOT, 1)} for k in range(5)]
        _line_lbl = lambda v: str(int(round(v * 100))) + "%"

    # show_line=False: gráfico só de barras (ex.: ticket médio, unit price) — sem
    # eixo direito nem linha. yR vazio faz o template não desenhar nada.
    if not show_line:
        yR = []

    rows, _pts = [], []
    for i, it in enumerate(items):
        _cx = _LEFT + i * _STEP + _STEP / 2
        _rh = (it["realized"] / _scale) * _PLOT if _scale else 0.0
        _qh = (it["quota"] / _scale) * _PLOT if _scale else 0.0
        att = it["attainment"]
        _qs = dict(drill_base); _qs[drill_key] = it["name"]
        row = {
            "name": it["name"], "short": it["short"], "leader": it["leader"],
            "realized": it["realized"], "quota": it["quota"],
            "attainment": att, "hit": (att is not None and att >= line_goal), "line_label": "—",
            "cx": round(_cx, 1),
            "rx": round(_cx - _GAP / 2 - _BW, 1), "rw": _BW, "rh": round(_rh, 1), "ry": round(_base_y - _rh, 1),
            "qx": round(_cx + _GAP / 2, 1), "qw": _BW, "qh": round(_qh, 1), "qy": round(_base_y - _qh, 1),
            "zx": round(_LEFT + i * _STEP, 1),   # x da zona de hover (largura = step)
            "drill_url": drill_path + "?" + urllib.parse.urlencode(_qs),
            # URL de drill própria do item (usada quando a barra NÃO é um AE/líder
            # — ex.: no Sales Overtime cada barra é um trimestre).
            "modal_url": it.get("modal_url"),
        }
        if att is not None:
            row["line_label"] = _line_lbl(att)
            _ay = _base_y - min(att, _maxa) / _maxa * _PLOT
            row["ay"] = round(_ay, 1)
            _pts.append(f"{round(_cx, 1)},{round(_ay, 1)}")
            # y do rótulo da linha — por padrão logo acima do ponto; se colidir com
            # o rótulo de valor de alguma barra DESTA coluna (barra curta + linha
            # baixa caem na mesma altura), sobe pra ficar acima dele (empilha).
            _aly = _ay - 12
            _btops = ([_base_y - _rh] if _rh > 0 else []) + ([_base_y - _qh] if _qh > 0 else [])
            for _t in _btops:
                _vtop = _t - 18                     # topo aprox. do box do rótulo de valor (font 12)
                if (_aly + 2) > _vtop and (_aly - 13) < (_t - 3):
                    _aly = min(_aly, _vtop - 4)      # sobe o rótulo da linha acima do de valor
            row["aly"] = round(_aly, 1)
        rows.append(row)

    _right_x = _LEFT + max(len(items), 1) * _STEP + 4
    _goal_y = round(_base_y - min(_goalv, _maxa) / _maxa * _PLOT, 1) if _goalv else None
    return {
        "rows": rows, "w": _right_x + 44, "h": _base_y + _BOT,
        "top": _TOP, "base_y": _base_y, "left": _LEFT, "right_x": _right_x, "step": _STEP,
        "plot_h": _PLOT, "line_pts": " ".join(_pts), "n": len(items),
        "yL": yL, "yR": yR, "goal_y": _goal_y,
        # Linhas de TENDÊNCIA (regressão linear) de cada série de barra, no eixo
        # esquerdo. Só quando trend=True — ex.: ticket médio e unit price.
        "trend_real": _trend_pts([it["realized"] for it in items],
                                 _LEFT, _STEP, _base_y, _PLOT, _scale) if trend else None,
        "trend_quota": _trend_pts([it["quota"] for it in items],
                                  _LEFT, _STEP, _base_y, _PLOT, _scale) if trend else None,
    }


def _build_line_chart(x_labels, series, y_max=None, y_suffix="%", x_urls=None):
    """Geometria SVG de um gráfico de N linhas (ver _line_chart.html).

    `x_labels`: rótulos do eixo X (ex.: trimestres). `series`: lista de
    {name, color, values} — values na MESMA ordem de x_labels, com None onde não
    há dado (o ponto some e a linha pula). Eixo Y de 0 ao topo arredondado."""
    _TOP, _PLOT, _BOT = 40, 340, 120
    _LEFT, _STEP = 74, 104
    base_y = _TOP + _PLOT
    n = max(len(x_labels), 1)
    right_x = _LEFT + n * _STEP
    vals = [v for s in series for v in s["values"] if v is not None]
    top = y_max or (max(vals) if vals else 100.0)
    top = max(10.0, (int(top / 10 - 1e-9) + 1) * 10)      # arredonda pra cima em 10

    def _fmt1(v):
        """1 casa decimal (vírgula pt-BR); inteiro exato fica sem decimal."""
        return (f"{int(round(v))}" if abs(v - round(v)) < 1e-9
                else f"{v:.1f}".replace(".", ",")) + y_suffix

    yL = [{"label": _fmt1(top * k / 4),
           "y": round(base_y - k / 4 * _PLOT, 1)} for k in range(5)]
    xs = [{"label": lbl, "cx": round(_LEFT + i * _STEP + _STEP / 2, 1),
           "zx": round(_LEFT + i * _STEP, 1),
           "url": (x_urls or {}).get(lbl)}
          for i, lbl in enumerate(x_labels)]
    out = []
    for s in series:
        pts, points = [], []
        for i, v in enumerate(s["values"]):
            if v is None:
                continue
            cx = _LEFT + i * _STEP + _STEP / 2
            cy = base_y - min(v / top, 1.0) * _PLOT
            pts.append(f"{round(cx, 1)},{round(cy, 1)}")
            points.append({"cx": round(cx, 1), "cy": round(cy, 1),
                           "label": f"{v:.1f}".replace(".", ",") + y_suffix})
        out.append({"name": s["name"], "color": s["color"],
                    "pts": " ".join(pts) if len(pts) > 1 else "", "points": points})
    return {"w": right_x + 70, "h": _TOP + _PLOT + _BOT, "left": _LEFT,
            "right_x": right_x, "base_y": base_y, "plot_h": _PLOT, "step": _STEP,
            "top": _TOP, "yL": yL, "xs": xs, "series": out}


def _trend_pts(vals, left, step, base_y, plot_h, scale):
    """Reta de tendência (mínimos quadrados) de uma série de barras, em pontos SVG.
    Ignora quarters sem dado (0/None) pra um buraco não puxar a reta pra baixo;
    devolve None se houver menos de 2 pontos com valor."""
    pts = [(i, float(v)) for i, v in enumerate(vals) if v]
    if len(pts) < 2 or not scale:
        return None
    n = len(pts)
    sx = sum(p[0] for p in pts)
    sy = sum(p[1] for p in pts)
    sxx = sum(p[0] * p[0] for p in pts)
    sxy = sum(p[0] * p[1] for p in pts)
    den = n * sxx - sx * sx
    if not den:
        return None
    b = (n * sxy - sx * sy) / den          # inclinação
    a = (sy - b * sx) / n                  # intercepto
    out = []
    for i in (pts[0][0], pts[-1][0]):      # do 1º ao último quarter COM dado
        y = max(0.0, a + b * i)
        cx = left + i * step + step / 2
        cy = base_y - min(y / scale, 1.0) * plot_h
        out.append(f"{round(cx, 1)},{round(cy, 1)}")
    return " ".join(out)


PT_MONTHS_SHORT = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]


def _fmt_date_short(d: date) -> str:
    return f"{d.day} {PT_MONTHS_SHORT[d.month-1]} {d.year}"


def _fmt_date_curta(d: date) -> str:
    """Sem o ano — pra eixo apertado, onde "28 jun 2026" não cabe em toda coluna
    e o ano já está dito no título da tela."""
    return f"{d.day} {PT_MONTHS_SHORT[d.month-1]}"


# Meta semanal de opps auto-geradas por AE: 3 por semana, cumulativo dentro do
# trimestre (3, 6, 9, ..., 36 ao final da semana 12). Reseta a cada trimestre.
WEEKLY_AE_GOAL = 3

# Meta DINÂMICA de cobertura Late (multiplicador Nx) — progressão semanal dentro
# do trimestre: 2x na semana 1, +0,25x por semana, com cap de 4x da semana 9 à 12.
COVERAGE_LATE_GOAL_START = 2.0
COVERAGE_LATE_GOAL_STEP  = 0.25
COVERAGE_LATE_GOAL_CAP   = 4.0


def _fiscal_week_of(d: date) -> tuple[int, int, date]:
    """Posição de uma data no calendário de semanas do trimestre.
    Devolve (mês do trimestre 1..3, semana do mês 1..5, segunda-feira da semana).

    Convenção: a semana é seg→dom e pertence ao mês da sua SEGUNDA. Então os dias
    antes da 1ª segunda de um mês contam na última semana do mês anterior — mesma
    regra do `_weekly_ae_goal`. Ex.: 30/09/2026 → M3, S4 (segundas de set: 7, 14,
    21, 28 → a de 28 é a 4ª); 01/09/2026 → M2, S5 (cai na semana de 31/08)."""
    monday = d - timedelta(days=d.weekday())
    first = date(monday.year, monday.month, 1)
    first_monday = first + timedelta(days=(0 - first.weekday()) % 7)
    week = (monday - first_monday).days // 7 + 1
    return (monday.month - 1) % 3 + 1, week, monday


def _quarter_week(today_d=None) -> int:
    """Semana (1..12) do trimestre corrente. Semana 1 = primeiros 7 dias do quarter."""
    today_d = today_d or _today()
    qs = _quarter_start_of(today_d)
    return max(1, min((today_d - qs).days // 7 + 1, 12))


def _late_coverage_goal(today_d=None):
    """Meta de cobertura Late da semana corrente: (semana, multiplicador).
    2x (S1) → +0,25x/semana → 4x (S9–S12)."""
    week = _quarter_week(today_d)
    goal = COVERAGE_LATE_GOAL_START + (week - 1) * COVERAGE_LATE_GOAL_STEP
    return week, round(min(goal, COVERAGE_LATE_GOAL_CAP), 2)


# Meta DINÂMICA de cobertura Early — rampa de 2,5x a 5x. Diferente do Late, o
# ciclo NÃO é o trimestre civil: é M3 do trimestre anterior + M1 e M2 do vigente.
# O AE precisa entrar no M3 com 2,5x de pipe já com close date no próximo
# trimestre, e sair do M2 desse próximo trimestre com 5x.
COVERAGE_EARLY_GOAL_START = 2.5
COVERAGE_EARLY_GOAL_END   = 5.0


def _early_cycle(today_d=None):
    """Ciclo da meta de Early: (início, fim, trimestre-alvo, estamos_no_m3).

    No M3 de um trimestre o ciclo REINICIA já apontando para o trimestre
    seguinte — é quando o alvo passa a ser a quota cheia do próximo. Nos M1/M2 o
    ciclo é o que começou no M3 do trimestre anterior e o alvo é o vigente.

    A fase é decidida pela SEGUNDA da semana corrente, não pelo dia: a meta é
    semanal, então não pode virar no meio da semana. Ex.: 01/09/2026 é terça da
    semana que começa em 31/08, logo ainda é M2 do ciclo antigo (5x), e o ciclo
    novo só entra na segunda seguinte (07/09)."""
    today_d = today_d or _today()
    today_d = today_d - timedelta(days=today_d.weekday())   # segunda da semana
    q = _quarter_start_of(today_d)
    month_of_q = (today_d.month - 1) % 3 + 1
    if month_of_q == 3:
        cycle_start = date(today_d.year, today_d.month, 1)
        target_q = _next_quarter(q)
    else:
        pq = _prev_quarter(q)
        cycle_start = date(pq.year, pq.month + 2, 1)   # M3 do trimestre anterior
        target_q = q
    cycle_end = _month_bounds(date(target_q.year, target_q.month + 1, 1))[1]  # fim do M2 do alvo
    return cycle_start, cycle_end, target_q, (month_of_q == 3)


def _early_coverage_goal(today_d=None) -> dict:
    """Meta de cobertura Early da semana corrente.

    A rampa 2,5x → 5x é dividida pelo número REAL de semanas do ciclo (13 ou 14,
    depende do trimestre), então o passo não é fixo: fecha exatamente em 5x na
    última semana em vez de bater no teto antes. Semana = seg→dom, contada pelas
    segundas dentro do ciclo (mesma convenção de `_fiscal_week_of`).

    `close_from`/`close_to` = janela de close date PRÓPRIA do Early (o trimestre
    alvo), independente do filtro da página. `use_full_quota` diz se o
    denominador é a quota cheia do alvo (M3) ou a remanescente (M1/M2)."""
    today_d = today_d or _today()
    cs, ce, target_q, is_m3 = _early_cycle(today_d)

    mondays, d = [], cs + timedelta(days=(0 - cs.weekday()) % 7)
    while d <= ce:
        mondays.append(d)
        d += timedelta(days=7)
    n = len(mondays) or 1

    cur_monday = today_d - timedelta(days=today_d.weekday())
    week = next((i for i, m in enumerate(mondays, 1) if m == cur_monday), None)
    if week is None:  # fora do ciclo (borda) — prende na ponta mais próxima
        week = 1 if (not mondays or cur_monday < mondays[0]) else n

    span = COVERAGE_EARLY_GOAL_END - COVERAGE_EARLY_GOAL_START
    step = (span / (n - 1)) if n > 1 else 0.0
    goal = COVERAGE_EARLY_GOAL_START + (week - 1) * step
    return {
        "week": week, "n_weeks": n, "step": round(step, 4),
        "goal": round(min(goal, COVERAGE_EARLY_GOAL_END), 2),
        "cycle_from": cs, "cycle_to": ce,
        "target_q": target_q, "is_m3": is_m3,
        "use_full_quota": is_m3,
        "close_from": target_q, "close_to": _quarter_end_of(target_q),
    }


def _last_n_window(n: int, grain: str, today_d=None) -> tuple:
    """Janela das últimas N períodos do grão (week/month/quarter) terminando hoje."""
    today_d = today_d or _today()
    if grain == "week":
        monday = today_d - timedelta(days=today_d.weekday())
        return monday - timedelta(weeks=n - 1), monday + timedelta(days=6)
    if grain == "quarter":
        cur_q = _quarter_start_of(today_d)
        from_q = cur_q
        for _ in range(n - 1):
            from_q = _prev_quarter(from_q)
        return from_q, _quarter_end_of(cur_q)
    # month
    cur_m = date(today_d.year, today_d.month, 1)
    m, y = cur_m.month, cur_m.year
    for _ in range(n - 1):
        m -= 1
        if m < 1:
            m, y = 12, y - 1
    from_d = date(y, m, 1)
    nxt = (cur_m.replace(day=28) + timedelta(days=4)).replace(day=1)
    return from_d, nxt - timedelta(days=1)


def _weekly_ae_goal(date_from: date, date_to: date, today_d=None) -> int:
    """Meta cumulativa: 3 opps × semanas decorridas, com cap de 12 semanas/trimestre.
    Reseta a cada trimestre. Nunca conta semana futura (cap em hoje).

    Regra de contagem: cada segunda-feira dentro do segmento adiciona 1 semana à
    meta. Quando o trimestre começa no meio da semana (qua/qui/sex/sáb/dom), os
    primeiros dias até o domingo formam uma "semana curta" que NÃO soma — só
    contamos a partir da próxima segunda. Ex.: Q2-2026 começa quarta 01-abr →
    primeira segunda é 06-abr, então até dom 10-mai temos 5 segundas (6/13/20/27 abr
    e 4 mai) = 15 opps cumulativas."""
    today_d = today_d or _today()
    end = min(date_to, today_d)
    if end < date_from:
        return 0
    total_weeks = 0
    cur = _quarter_start_of(date_from)
    while cur <= end:
        q_end = _quarter_end_of(cur)
        seg_start = max(cur, date_from)
        seg_end   = min(q_end, end)
        if seg_end >= seg_start:
            # Segunda-feira == weekday 0. Acha a 1a segunda >= seg_start.
            first_monday = seg_start + timedelta(days=(0 - seg_start.weekday()) % 7)
            if first_monday <= seg_end:
                weeks = ((seg_end - first_monday).days // 7) + 1
                total_weeks += min(weeks, 12)  # cap de 12 semanas/quarter (= 36 opps)
        cur = _next_quarter(cur)
    return total_weeks * WEEKLY_AE_GOAL


def _format_period(filters: dict, presets: list, active_preset: str) -> str:
    df_d = _parse_filter_date(filters.get("date_from"))
    dt_d = _parse_filter_date(filters.get("date_to"))
    if not df_d and not dt_d:
        return ""
    label = ""
    if active_preset:
        p = next((p for p in presets if p["key"] == active_preset), None)
        if p: label = p["label"]
    if df_d and dt_d:
        rng = f"{_fmt_date_short(df_d)} → {_fmt_date_short(dt_d)}"
    elif df_d:
        rng = f"a partir de {_fmt_date_short(df_d)}"
    else:
        rng = f"até {_fmt_date_short(dt_d)}"
    return f"{label} · {rng}" if label else rng


BRT = timezone(timedelta(hours=-3), "BRT")


def _today():
    """Hoje em Brasília — ver a nota do fuso junto aos imports."""
    return datetime.now(BRT).date()


def _last_two_sundays(today_d=None, now_dt=None) -> tuple:
    """Retorna (last_sunday, prev_sunday) das janelas WoW (12:00 BRT em cada).
    Se hoje é domingo:
      - antes de 12h BRT → snapshot de hoje ainda não "passou", usa dom anterior;
      - depois de 12h BRT → usa HOJE como snap atual e dom anterior como prev.
    Isso garante que no domingo à tarde já comparemos a semana atual vs anterior."""
    today_d = today_d or _today()
    days_since_sunday = (today_d.weekday() - 6) % 7
    if days_since_sunday == 0:
        # Hoje é domingo. Snapshot de hoje 12h só existe a partir das 12h locais.
        now = now_dt or datetime.now(BRT)
        if now.hour < 12:
            days_since_sunday = 7
    last_sunday = today_d - timedelta(days=days_since_sunday)
    return last_sunday, last_sunday - timedelta(days=7)


def _sunday_on_or_before(d: date) -> date:
    """Domingo igual ou anterior a `d`. Os snapshots do WoW são sempre domingo
    12h BRT, então qualquer data escolhida no filtro ancora no domingo dela."""
    return d - timedelta(days=(d.weekday() - 6) % 7)


def _wow_window(f: dict = None) -> tuple:
    """(domingo_atual, domingo_anterior) dos 2 snapshots comparados no WoW.

    Sem filtro de data → os 2 últimos domingos (`_last_two_sundays`), que é o
    padrão pré-fixado. Com filtro, `date_from` é o snapshot ANTERIOR e `date_to`
    o ATUAL — cada um ancorado no domingo igual ou anterior à data escolhida.
    Janela invertida ou de tamanho zero cai de volta pra 1 semana."""
    f = f or {}
    last_sun, prev_sun = _last_two_sundays()
    try:
        d_from = pd.to_datetime(f.get("date_from")).date() if f.get("date_from") else None
        d_to   = pd.to_datetime(f.get("date_to")).date()   if f.get("date_to")   else None
    except (ValueError, TypeError):
        d_from = d_to = None
    curr = _sunday_on_or_before(d_to)   if d_to   else last_sun
    prev = _sunday_on_or_before(d_from) if d_from else prev_sun
    if prev >= curr:
        prev = curr - timedelta(days=7)
    return curr, prev


def _wow_fixed_pair() -> tuple:
    """Par de domingos do PoP Early/Late: SEMPRE os 2 últimos, ignorando
    date_from/date_to.

    O filtro de período foi REMOVIDO dessas 2 telas (decisão do Pedro,
    2026-08-03) porque ele não entregava "domingo contra domingo": `_wow_window`
    ancora cada ponta no domingo dela de forma independente, então
    date_from=2026-06-01 + date_to=2026-06-30 virava a comparação
    "31/mai vs 28/jun" — 4 semanas de distância travestidas de PoP. Comparar
    Early com Late nesse estado não fechava, e era impossível saber se a
    divergência era de dado ou de janela.

    Volta a existir quando houver confiança nos números; aí o certo é um seletor
    de PAR de domingos consecutivos, não um date-range livre. Os outros
    consumidores de `_wow_window` (Funil WoW, Stale, Inatividade) seguem com
    filtro próprio — este helper é só das 2 telas PoP."""
    return _last_two_sundays()


def _quarter_start_of(d: date) -> date:
    """First day of the quarter containing d (calendar quarter)."""
    q_month = ((d.month - 1) // 3) * 3 + 1
    return date(d.year, q_month, 1)


def _prev_quarter(qstart: date) -> date:
    if qstart.month == 1:
        return date(qstart.year - 1, 10, 1)
    return date(qstart.year, qstart.month - 3, 1)


def _next_quarter(qstart: date) -> date:
    if qstart.month == 10:
        return date(qstart.year + 1, 1, 1)
    return date(qstart.year, qstart.month + 3, 1)


def _quarter_end_of(d: date) -> date:
    """Last day of the quarter containing d."""
    nxt = _next_quarter(_quarter_start_of(d))
    return nxt - timedelta(days=1)


def _quarter_label(d: date) -> str:
    """Rótulo curto do quarter: 2026-07-01 → 'Q3/26'."""
    return f"Q{(d.month - 1) // 3 + 1}/{d.year % 100:02d}"


def _week_bounds(d: date) -> tuple[date, date]:
    """ISO week (Monday → Sunday) containing d."""
    monday = d - timedelta(days=d.weekday())
    return monday, monday + timedelta(days=6)


def _month_bounds(d: date) -> tuple[date, date]:
    first = date(d.year, d.month, 1)
    if d.month == 12:
        nxt = date(d.year + 1, 1, 1)
    else:
        nxt = date(d.year, d.month + 1, 1)
    return first, nxt - timedelta(days=1)


def build_presets(available_quarters: list[date]) -> list[dict]:
    """Build preset date ranges grouped by grain (semana / mês / trimestre / FY)."""
    today = _today()
    current_q = _quarter_start_of(today)
    if available_quarters:
        latest_data_q = max(q for q in available_quarters if q <= current_q) if \
            any(q <= current_q for q in available_quarters) else max(available_quarters)
    else:
        latest_data_q = current_q

    last_q = _prev_quarter(latest_data_q)
    next_q = _next_quarter(_quarter_start_of(today))
    # Janelas multi-trimestre: começam no PRÓXIMO trimestre e vão somando. Usadas
    # na Cobertura, que olha só pra frente (o corrente tem preset próprio).
    next_q2 = _next_quarter(next_q)
    next_q3 = _next_quarter(next_q2)
    _wow_last_sun, _wow_prev_sun = _last_two_sundays()
    week_from, week_to = _week_bounds(today)
    prev_week_from, prev_week_to = _week_bounds(today - timedelta(days=7))
    month_from, month_to = _month_bounds(today)
    prev_month_anchor = month_from - timedelta(days=1)
    prev_month_from, prev_month_to = _month_bounds(prev_month_anchor)
    # Próximo mês: 1º dia do próximo mês até último dia
    if month_from.month == 12:
        next_month_from = date(month_from.year + 1, 1, 1)
    else:
        next_month_from = date(month_from.year, month_from.month + 1, 1)
    _, next_month_to = _month_bounds(next_month_from)

    presets = [
        # — Semana —
        {"key":"this_week", "label":"Esta semana", "group":"Semana",
         "from": week_from.isoformat(), "to": week_to.isoformat()},
        {"key":"last_week", "label":"Semana passada", "group":"Semana",
         "from": prev_week_from.isoformat(), "to": prev_week_to.isoformat()},
        # — Mês —
        {"key":"this_month", "label":"Este mês", "group":"Mês",
         "from": month_from.isoformat(), "to": month_to.isoformat()},
        {"key":"next_month", "label":"Próximo mês", "group":"Mês",
         "from": next_month_from.isoformat(), "to": next_month_to.isoformat()},
        {"key":"last_month", "label":"Mês passado", "group":"Mês",
         "from": prev_month_from.isoformat(), "to": prev_month_to.isoformat()},
        # — Trimestre —
        {"key":"current_q", "label":"Este trimestre · " + _quarter_label(latest_data_q),
         "group":"Trimestre",
         "from": latest_data_q.isoformat(), "to": _quarter_end_of(latest_data_q).isoformat()},
        {"key":"next_q", "label":"Próximo trimestre · " + _quarter_label(next_q),
         "group":"Trimestre",
         "from": next_q.isoformat(), "to": _quarter_end_of(next_q).isoformat()},
        # ISOLADOS, não acumulados: a pergunta é "quanto de pipe tenho pro Q4" e
        # "pro Q1", separadamente. Cada preset abre um trimestre só.
        {"key":"next_2q", "label":"Trimestre posterior · " + _quarter_label(next_q2),
         "group":"Trimestre",
         "from": next_q2.isoformat(), "to": _quarter_end_of(next_q2).isoformat()},
        {"key":"next_3q", "label":"Daqui a 3 trimestres · " + _quarter_label(next_q3),
         "group":"Trimestre",
         "from": next_q3.isoformat(), "to": _quarter_end_of(next_q3).isoformat()},
        {"key":"last_q", "label":"Trimestre passado", "group":"Trimestre",
         "from": last_q.isoformat(), "to": _quarter_end_of(last_q).isoformat()},
        # — Ano (calendário) —
        {"key":"this_year", "label":"Este ano", "group":"Ano",
         "from": date(today.year, 1, 1).isoformat(),
         "to":   date(today.year, 12, 31).isoformat()},
        {"key":"next_year", "label":"Próximo ano", "group":"Ano",
         "from": date(today.year + 1, 1, 1).isoformat(),
         "to":   date(today.year + 1, 12, 31).isoformat()},
        {"key":"last_year", "label":"Ano passado", "group":"Ano",
         "from": date(today.year - 1, 1, 1).isoformat(),
         "to":   date(today.year - 1, 12, 31).isoformat()},
        # — Ano fiscal (Tractian: fev → jan) —
        # — Domingos (WoW) — o snapshot é sempre domingo 12h BRT, então aqui os
        # presets escolhem QUAIS 2 domingos comparar. `to` = snapshot atual.
        {"key":"wow_1w", "label":"Último domingo vs anterior", "group":"Domingos",
         "from": _wow_prev_sun.isoformat(), "to": _wow_last_sun.isoformat()},
        {"key":"wow_2w", "label":"Últimas 2 semanas", "group":"Domingos",
         "from": (_wow_last_sun - timedelta(weeks=2)).isoformat(), "to": _wow_last_sun.isoformat()},
        {"key":"wow_4w", "label":"Últimas 4 semanas", "group":"Domingos",
         "from": (_wow_last_sun - timedelta(weeks=4)).isoformat(), "to": _wow_last_sun.isoformat()},
        {"key":"wow_8w", "label":"Últimas 8 semanas", "group":"Domingos",
         "from": (_wow_last_sun - timedelta(weeks=8)).isoformat(), "to": _wow_last_sun.isoformat()},
        {"key":"fy26", "label":"FY26", "group":"FY",
         "from":"2026-04-01", "to":"2027-01-31"},
        {"key":"all", "label":"Todo histórico", "group":"FY",
         "from":"2024-01-01", "to": _quarter_end_of(latest_data_q).isoformat()},
    ]
    return presets


# Cobertura de Pipeline (visão AE e visão diretores) — o filtro de período aqui é
# Close Date e a página só olha pra frente.
# Caminho da rota-espelho -> caminho da rota do BRASIL que ela reusa. Preenchido
# junto do registro das rotas, la embaixo. Sem ele, tirar o prefixo "/mx" nao
# basta: /mx/pipeline/cobertura viraria "/pipeline/cobertura", que nao e o
# "/pipeline" do Brasil — e a tela ficava fora do registro de Cobertura, sem a
# trava do trimestre corrente que faz o multiplicador significar alguma coisa.
_MX_PATH_TO_BR: dict = {}


def _rota_base(path: str) -> str:
    """Caminho sem o prefixo de país.

    As tuplas abaixo (COVERAGE_PATHS, WOW_PATHS, …) são registros POR TELA, não
    por país — dizem coisas como "aqui o período é close date" ou "aqui não há
    date-picker, a janela é domingo a domingo". Como o México roda as MESMAS
    views, ele precisa cair nos mesmos registros; comparar o path cru deixava
    /mx/... de fora de todos e a tela mexicana perdia filtro, trava de trimestre
    e rótulo de período — sem nada quebrar visivelmente."""
    if not path.startswith("/mx/"):
        return path
    return _MX_PATH_TO_BR.get(path) or path[3:]


COVERAGE_PATHS = ("/pipeline", "/leaders/pipeline")

# Pipeline Total — "quanto dinheiro tem no funil, cortado como eu quiser".
# O recorte de data é o CLOSE DATE (igual Cobertura), mas aqui ele é OPCIONAL:
# 49% dos deals abertos não têm close date preenchido no HubSpot, então abrir a
# aba travada num trimestre esconderia metade do pipe sem avisar. Sem período =
# todo o funil aberto; com período, a página informa quantos deals ficaram fora.
PIPELINE_TOTAL_PATHS = ("/pipeline/total", "/leaders/pipeline/total")

# WoW Early/Late — o período não é um intervalo, são 2 snapshots de domingo 12h
# BRT. O filtro escolhe QUAIS domingos comparar (de = anterior, até = atual).
WOW_PATHS = ("/pipeline/wow/early", "/pipeline/wow/late",
             "/leaders/pipeline/wow/early", "/leaders/pipeline/wow/late")

# Histórico PoP — mesma natureza (snapshots de domingo 12h BRT), mas em série de
# N semanas. A janela é uma CONTAGEM de domingos, escolhida no chip da própria
# página (?weeks=), não um intervalo: por isso entra junto do PoP nas listas de
# "sem date-picker", mas fora de PRESET_KEYS_BY_PATH (não usa preset nenhum).
WOW_HIST_PATHS = ("/pipeline/wow/history", "/leaders/pipeline/wow/history",
                  # PoP Histórico por quarter (só Leaders): mesma natureza — a
                  # janela é chip da página, e o trimestre de cada semana sai
                  # da própria semana, não do date-picker.
                  "/leaders/pipeline/wow/quarter",
                  # Cópia pro Sales AE BR (20/10/2026): mesma tela, grão de AE.
                  "/pipeline/wow/quarter")

# Pipeline DoD — a janela é o par de MEIAS-NOITES escolhido no chip da página
# (?dia=), e o pipe é o funil aberto inteiro, sem corte de close date. Por isso
# entra junto das telas sem date-picker.
# O Pipeline WoW é a mesma tela com pontas semanais (?semana=) e entra aqui pelo
# mesmo motivo.
PIPELINE_DOD_PATHS = ("/leaders/pipeline/dod", "/leaders/pipeline/wow")

# Funil II — o período aqui não é close date nem snapshot: é a data em que o deal
# ENTROU na etapa. Janela aberta (sem datas) = histórico inteiro, de propósito.
FUNNEL2_PATHS = ("/pipeline/funnel-ii", "/leaders/pipeline/funnel-ii")

# WoW do Forecast — o período recorta o QUARTER (close date), igual à aba
# Forecast e ao Histórico dela; o par de domingos comparados é escolhido no chip
# da própria página (?par=), não no date-picker. Por isso NÃO entra nas listas de
# "sem date-picker": aqui as datas significam algo (qual trimestre), só não
# significam "quais domingos".
FORECAST_WOW_PATHS = ("/forecast/wow", "/leaders/forecast/wow",
                      # DoD: mesma natureza, com o par de DIAS no chip (?dia=).
                      "/leaders/forecast/dod")

# Aba Forecast (a tela-mãe do grupo). Existe pra ela entrar no registro de
# filtros extras: Deal Type recorta esta tela pelo mesmo motivo do WoW e do
# Funil II — aquisição e expansão têm ticket, ciclo e taxa de conversão
# diferentes, e o ponderado somado esconde isso.
FORECAST_PATHS = ("/forecast", "/leaders/forecast")

# As sub-abas do grupo Forecast que NÃO honram o corte de Deal Type: o Histórico
# mede o time inteiro semana a semana e o Modelo Analytics já parametriza tipo
# dentro da própria probabilidade (segmento × estágio × tipo). Elas existem nesta
# lista só pra o CHIP não mentir — ver `build_active_chips`.
FORECAST_NO_DTYPE_PATHS = ("/forecast/history", "/leaders/forecast/history",
                           "/forecast/model", "/leaders/forecast/model")

# Aba Legal — o período recorta o CLOSE DATE, mas olhando pra TRÁS (a aba só
# fala de deal fechado). Por isso não reusa os presets da Cobertura, que são
# todos futuros: aqui os úteis são trimestre corrente, passado e ano. Janela
# aberta por padrão de propósito — são ~1.4k tickets do jurídico em 2 anos e
# meio, e travar num trimestre deixaria as células da matriz sem amostra.
LEGAL_PATHS = ("/pipeline/legal", "/leaders/pipeline/legal")

# Whitelist de presets por rota — preserva a ordem informada aqui.
# None = mostra todos.
PRESET_KEYS_BY_PATH = {
    # Só trimestre e ISOLADO (nunca acumulado): "quanto de pipe tenho pro Q4" e
    # "pro Q1" são perguntas separadas. Nada de mês, ano ou passado.
    **{p: ("current_q", "next_q", "next_2q") for p in COVERAGE_PATHS},
    # Só comparações de domingo; datas soltas ficam pro campo de/até.
    **{p: ("wow_1w", "wow_2w", "wow_4w", "wow_8w") for p in WOW_PATHS},
    # Mesmos presets da Cobertura (close date olha pra frente), mas sem pin:
    # a aba abre com o funil inteiro e o trimestre é uma escolha do usuário.
    **{p: ("current_q", "next_q", "next_2q") for p in PIPELINE_TOTAL_PATHS},
    # Só presets pra trás: a aba mede desfecho, e trimestre futuro não tem
    # deal fechado pra medir.
    **{p: ("current_q", "last_q", "this_year", "last_year", "all") for p in LEGAL_PATHS},
}


def filter_presets_for_path(presets: list[dict], path: str) -> list[dict]:
    allowed = PRESET_KEYS_BY_PATH.get(_rota_base(path))   # MX usa os presets da mesma TELA
    if allowed is None:
        return presets
    by_key = {p["key"]: p for p in presets}
    return [by_key[k] for k in allowed if k in by_key]


# Label do filtro de período varia por rota — na Cobertura é Close Date do HubSpot
# e no WoW são os 2 domingos comparados.
PERIOD_LABEL_BY_PATH = {**{p: "Close Date" for p in COVERAGE_PATHS},
                        **{p: "Close Date" for p in PIPELINE_TOTAL_PATHS},
                        **{p: "Domingos comparados" for p in WOW_PATHS},
                        **{p: "Entrada na etapa" for p in FUNNEL2_PATHS},
                        **{p: "Fechamento do deal" for p in LEGAL_PATHS}}


def get_filters():
    """Parse global filter params from querystring.

    As datas saem daqui SEMPRE normalizadas em ISO (ou ""), nunca cru: dezenas de
    call sites fazem `pd.to_datetime(f["date_from"])` sem proteção, então uma data
    quebrada na URL virava 500 em toda página. Saneando na origem, o filtro
    inválido é simplesmente ignorado."""
    return {
        "date_from": _iso_or_empty(request.args.get("date_from", "")),
        "date_to":   _iso_or_empty(request.args.get("date_to", "")),
        "region":    request.args.get("region", ""),
        "leader":    request.args.get("leader", ""),
        "ae":        request.args.get("ae", ""),
        "role":      request.args.get("role", ""),
        # Filtro mestre de moeda: "" / "mrr" → MRR em R$ (padrão); "arr" → ARR em US$.
        "money":     request.args.get("money", ""),
        # Cortes do Pipeline Total. Ficam aqui (e não soltos na rota) pra entrarem
        # no ctx.filters — é o que faz os chips de "Ativos", o Limpar e os
        # url_for(**ctx.filters) dos drills levarem o filtro adiante de graça.
        # Vazio em toda outra aba, onde os selects nem são renderizados.
        "dtype":     request.args.get("dtype", ""),
        "source":    request.args.get("source", ""),
        # Propriedade Product do HubSpot. Multi-select, então "prod=smarttrac"
        # significa "inclui Smart Trac" (ver `_pt_apply_dim_filter`).
        "prod":      request.args.get("prod", ""),
        "fcat":      request.args.get("fcat", ""),
        # Presença de close date no Pipeline Total: "" (todos) · "com" · "sem".
        # Não é uma dimensão de _PT_DIMS porque é teste de NULO, não de valor —
        # `_pt_apply_dim_filter` compara `col == raw`, que não sabe responder
        # "tem qualquer data preenchida".
        # Saneado aqui, como as datas e pelo mesmo motivo: valor inventado na URL
        # não pode deixar a página num 4º estado sem chip aceso — cai em "todos".
        "cdate":     (request.args.get("cdate", "")
                      if request.args.get("cdate") in ("com", "sem") else ""),
        # Presença de Customer Owner no Pipeline Total: "" (todos) · "com" · "sem".
        # Mesmo molde do `cdate` (teste de nulo, saneado na origem).
        "cown":      (request.args.get("cown", "")
                      if request.args.get("cown") in ("com", "sem") else ""),
        # Cortes da aba Legal: `env` = o que conta como "jurídico envolvido"
        # (ticket · interação nominal · qualquer um) e `coh` = a coorte da
        # comparação (Negociação+ · todos os fechados). Ficam aqui pelo mesmo
        # motivo de `dtype`/`source`: é o que faz os chips de Ativos, o Limpar e
        # os url_for(**ctx.filters) do drill carregarem o corte de graça.
        # Saneados na origem (default quando o valor não existe) pra URL editada
        # à mão não deixar a página num estado sem chip aceso.
        # Vazio = default (ticket · Negociação+), resolvido por `_legal_env` /
        # `_legal_coh` no uso. NÃO resolve aqui de propósito: `ctx.filters` entra
        # em todo url_for do site, e gravar o default faria cada link de cada aba
        # carregar ?env=ticket&coh=neg sem nunca ter passado pela aba Legal.
        "env":       (request.args.get("env", "")
                      if request.args.get("env") in _LEGAL_ENVS else ""),
        "coh":       (request.args.get("coh", "")
                      if request.args.get("coh") in _LEGAL_COHORTS else ""),
    }


def _money_mode() -> str:
    """'arr' quando o filtro mestre de moeda está em ARR US$; senão 'mrr'.

    O México é SEMPRE ARR US$: o time é medido em dólar e um deal em peso não tem
    leitura em real. Por isso lá o toggle nem aparece."""
    from flask import has_request_context
    if not has_request_context():
        return "mrr"
    if _is_mx():
        return "arr"
    return "arr" if request.args.get("money") == "arr" else "mrr"


def _parse_filter_date(v):
    """Data do filtro → date, ou None se vier vazia/inválida. Sem isso um
    `?date_from=xx` na URL derruba TODA página com 500 (a query string é
    editável e link salvo envelhece); melhor ignorar o filtro quebrado."""
    if not v:
        return None
    try:
        return pd.to_datetime(v).date()
    except (ValueError, TypeError, OverflowError):
        return None


def _iso_or_empty(v) -> str:
    """Normaliza data de filtro pra 'YYYY-MM-DD'; inválida/vazia → ''."""
    d = _parse_filter_date(v)
    return d.isoformat() if d else ""


def apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    """Slice df pelos filtros — boolean indexing já retorna view independente,
    sem copy() explícito. Usa quarter_end pré-computado para evitar .apply()."""
    from_d = _parse_filter_date(f["date_from"])
    to_d   = _parse_filter_date(f["date_to"])
    if from_d:
        if "quarter_end" in df.columns:
            df = df[df["quarter_end"] >= from_d]
        else:
            df = df[df["quarter"].apply(_quarter_end_of) >= from_d]
    if to_d:
        df = df[df["quarter"] <= to_d]

    if f["region"]: df = df[df["region"] == f["region"]]
    if f["leader"]: df = df[df["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:     df = df[df["account_executive"] == f["ae"]]
    if f["role"]:   df = df[df["role"] == f["role"]]
    return df


# ============================================================
# In-memory data cache — invalida quando o parquet subjacente muda
# ============================================================
# Cada request lia o parquet do disco + reaplicava overrides + filtros allowed.
# Agora isso é feito 1x por mtime do parquet (~70ms → ~0.05ms por request).
# Botão "↻ Refresh BQ" (?refresh=1) força rebuild propagando para o BQ.
_DATA_CACHE_LOCK = threading.Lock()
_DATA_CACHE: dict = {}


def _parquet_mtime(name: str) -> float:
    p = bq.CACHE_DIR / f"{name}.parquet"
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def _get_clean_ae_perf(force: bool = False) -> pd.DataFrame:
    """[Chave de cache começa com _pl(): sem isso a 1ª requisição de um país
    grava o frame e o outro país recebe o dado errado — o cache em memória não
    sabia que o mesmo nome de função agora lê dois funis.]

    ae_performance + leader overrides + ALLOWED_LEADERS + HIDDEN_FROM_AE_VIEWS,
    pré-computado com quarter (date) e quarter_end. Cacheado em memória —
    invalidação via mtime do parquet ou force=True."""
    df_raw = bq.ae_performance(force_refresh=force)
    # Chave versionada: o mtime do parquet só detecta dado novo, não CÓDIGO novo —
    # o sufixo garante que nenhum frame montado por uma versão anterior da curada
    # (sem a linha sintética do roster) sobreviva a um reload a quente.
    key = (_pl(), "ae_perf_clean_v2", _parquet_mtime("ae_performance"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = df_raw.copy()
    df["quarter"] = pd.to_datetime(df["quarter"]).dt.date
    df["quarter_end"] = df["quarter"].map(_quarter_end_of)
    if _is_mx():
        # MÉXICO: a fronteira do país é a REGIÃO, não o líder. Diferente do Brasil
        # de propósito, e é o que faz o PASSADO existir: 30 vendedores mexicanos
        # têm linha desde 2024, e quase todos os que SAÍRAM estão gravados como
        # "No leader assigned" — filtrar por líder apagaria o histórico inteiro
        # (sobravam 8 dos 30).
        #
        # Depois de recortar a região, todo mundo recebe o gestor do país: o MX
        # não tem camada intermediária, então o rollup por líder é um só. É a
        # "outra estrutura, mesmas premissas" que o usuário pediu.
        #
        # NÃO passa por apply_q3_roster_curated nem inject_roster_missing_curated:
        # os dois são a lista curada do BRASIL e injetariam brasileiro aqui.
        _mx = merge_ae_quarter_dups(canon_ae_names(df[df["region"] == "MX"].copy()))
        _mx["leader"] = MX_LEADER
        _mx = _mx.reset_index(drop=True)
        with _DATA_CACHE_LOCK:
            _DATA_CACHE[key] = _mx
        return _mx
    # Nome canônico ANTES do roster: é o rename que faz a linha órfã do AE casar
    # com o roster, e a fusão evita a quota sair dobrada.
    df = merge_ae_quarter_dups(canon_ae_names(df))
    df = apply_leader_overrides(df)
    df = apply_q3_roster_curated(df)        # time/quota Q3 autoritativos (só linhas Q3)
    # Depois do overwrite (a linha sintética já nasce com leader/region/quota do
    # roster, então passar por ele seria no-op) e ANTES dos três gates: a linha
    # sintética tem de enfrentar o MESMO gauntlet de visibilidade de uma real —
    # líder fora do allowlist, nome escondido ou saída com cutoff no Q3 a descartam
    # sozinhos, sem cláusula especial e sem abrir porta de fundo.
    df = inject_roster_missing_curated(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    df = drop_departed_from(df, "quarter").reset_index(drop=True)
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _get_unit_price_events(force: bool = False) -> pd.DataFrame:
    """Eventos de venda validada de Smart Trac (registro de venda) — base do
    Unit Price do Sales Overtime. event_date vira date; números coeridos."""
    raw = bq.unit_price_events(force_refresh=force)
    key = (_pl(), "unit_price_events_v1", _parquet_mtime("unit_price_events_v1"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = raw.copy()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    df = df[df["event_date"].notna()]
    df["amount_brl"] = pd.to_numeric(df["amount_brl"], errors="coerce").fillna(0.0)
    df["devices"] = pd.to_numeric(df["devices"], errors="coerce").fillna(0).astype(int)
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _get_quota_history(force: bool = False) -> pd.DataFrame:
    """Quota trimestral do BR (Sales Overtime), direto do BQ — meta OFICIAL do
    time, sem recorte de roster.

    De propósito NÃO aplicamos ALLOWED_LEADERS / HIDDEN / roster aqui: a meta do
    time inclui todo mundo que carregava meta naquele quarter — inclusive reps
    que já saíram e gestores que vendem. Reps entram e saem ao longo do tempo e a
    fonte já reflete isso (cada rep só tem meses em que esteve na casa), então
    somar por quarter dá a meta correta DAQUELE momento.

    `leader` é anexado só pra permitir o filtro opcional por líder; fica None pra
    quem não está na curated (não afeta o total)."""
    raw = bq.quota_history(force_refresh=force)
    key = (_pl(), "quota_history_v2", _parquet_mtime("quota_history_v2"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    df["quarter"] = pd.to_datetime(df["quarter"]).dt.date
    df["quota_in_brl"] = pd.to_numeric(df["quota_in_brl"], errors="coerce").fillna(0.0)
    df["new_logos_quota"] = pd.to_numeric(df["new_logos_quota"], errors="coerce").fillna(0).astype(int)

    # Líder POR QUARTER (histórico), pra o filtro de líder respeitar quem era o
    # gestor à época. Fallback: roster Q3 → líder atual do snapshot.
    lmap = _period_leader_map(force=force)
    cur_leader = {}
    for _r in _get_clean_ae_perf(force=force).itertuples(index=False):
        cur_leader[_r.account_executive] = _r.leader
    def _ld(row):
        ae = row["account_executive"]
        if not ae or pd.isna(ae):
            return None
        return (lmap.get((ae, row["quarter"]))
                or (_roster_of(row["quarter"] if row["quarter"] in ROSTERS_BR else None)
                    .get(ae) or ROSTER.get(ae) or {}).get("leader")
                or cur_leader.get(ae))
    df["leader"] = df.apply(_ld, axis=1) if len(df) else None
    df = df.reset_index(drop=True)
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _safe_int(v, default):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _current_leader_by_ae(force: bool = False) -> dict:
    """{account_executive: líder atual} — roster Q3 primeiro, senão o snapshot
    mais recente da curated. Usado onde não há data pra atribuir o líder da época
    (ex.: coortes de funil, que atravessam vários quarters)."""
    out = {ae: v["leader"] for ae, v in ROSTER.items()}
    for r in _get_clean_ae_perf(force=force).sort_values("quarter").itertuples(index=False):
        out.setdefault(r.account_executive, r.leader)
    return out


def _get_funnel_entries(force: bool = False) -> pd.DataFrame:
    """Pivot deal × estágio → timestamp da 1ª entrada, uma coluna por número de
    estágio (s1..s7). Base das coortes de conversão do funil."""
    raw = bq.funnel_stage_entries(force_refresh=force, pipeline=_pl())
    key = (_pl(), "funnel_entries_v6", _parquet_mtime("funnel_stage_entries_v5"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    df["first_at"] = pd.to_datetime(df["first_at"], errors="coerce", utc=True)
    # closedlost entra como pseudo-estágio 99 → vira a coluna `s99` (data da perda).
    id2num = {sid: n for n, sid, _ in _CONV_STAGES}
    id2num["closedlost"] = 99
    df["snum"] = df["stage"].map(id2num)
    df = df[df["snum"].notna()]
    # AE/tipo entram como ÍNDICE do pivot, e groupby descarta chave NaN — sem
    # esses sentinelas o pivot perdia silenciosamente 61 deals (59 sem AE no
    # int_adding_names_to_deals, 2 sem deal_type). Eram 0,3% do funil sumindo
    # das conversões e do universo total. O sentinela não casa com nenhum filtro
    # de AE/líder/tipo, então esses deals só aparecem na visão sem recorte —
    # que é exatamente onde eles devem contar.
    df["account_executive"] = df["account_executive"].fillna("— sem AE")
    df["deal_type"] = df["deal_type"].fillna("— sem tipo")
    piv = (df.pivot_table(index=["deal_id", "account_executive", "deal_type"],
                          columns="snum", values="first_at", aggfunc="min")
             .reset_index())
    piv.columns = [c if isinstance(c, str) else f"s{int(c)}" for c in piv.columns]
    for n in list(range(1, 8)) + [99]:
        if f"s{n}" not in piv.columns:
            piv[f"s{n}"] = pd.NaT
    # created_date/deal_name/amount por mapa (fora do índice do pivot — deal sem
    # data não some; nome e valor alimentam o drill de deals por estágio).
    base = df.drop_duplicates("deal_id").set_index("deal_id")
    piv["created_date"] = pd.to_datetime(piv["deal_id"].map(base["created_date"]), errors="coerce")
    piv["deal_name"] = piv["deal_id"].map(base["deal_name"])
    piv["amount"] = pd.to_numeric(piv["deal_id"].map(base["amount"]), errors="coerce").fillna(0.0)
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = piv
    return piv


def _stage_active_mask(piv, num, lo, hi):
    """Deals ATIVOS no estágio `num` na janela [lo, hi): entraram até o fim da
    janela e só saíram (outra 1ª entrada posterior) depois do início dela.
    Sem saída registrada = ainda está no estágio. num=7 (Won, terminal) →
    ganhos DENTRO da janela. Mesma régua do drill (/overview/funnel-stages)."""
    ent = piv[f"s{num}"]
    if num == 7:
        return ((ent >= lo) & (ent < hi)).fillna(False)
    stage_cols = [f"s{n}" for n in range(1, 8)] + ["s99"]
    others = piv[stage_cols].drop(columns=[f"s{num}"])
    exit_ts = others.where(others.gt(ent, axis=0)).min(axis=1)   # NaT = nunca saiu
    return ent.notna() & (ent < hi) & (exit_ts.isna() | (exit_ts >= lo))


def _conv_series_cohort(piv, quarters, a_num, b_num, roll=0):
    """Conversão por COORTE (modelo canônico da literatura — WbD/Roberge):
    dos deals que ENTRARAM em A na janela [Q-roll..Q], % que chegou em B em
    qualquer momento ≥ entrada. O rolling é só suavização da coorte. Janelas
    recentes são coortes imaturas (subestimadas até ~1 ciclo de venda)."""
    ca, cb = f"s{a_num}", f"s{b_num}"
    if ca not in piv.columns or cb not in piv.columns:
        return [None] * len(quarters), [0] * len(quarters)
    d = piv[piv[ca].notna()].copy()
    d["q"] = d[ca].dt.tz_convert(None).dt.date.map(_quarter_start_of)
    d["ok"] = d[cb].notna() & (d[cb] >= d[ca])
    vals, dens = [], []
    for q in quarters:
        lo = q
        for _ in range(max(0, roll)):
            lo = _quarter_start_of(lo - timedelta(days=1))
        m = (d["q"] >= lo) & (d["q"] <= q)
        n = int(m.sum())
        vals.append(100.0 * float(d.loc[m, "ok"].sum()) / n if n else None)
        dens.append(n)
    return vals, dens


def _conv_series(piv, quarters, a_num, b_num, roll=0):
    """% de conversão A→B por trimestre, ACUMULADO NAS DUAS PONTAS.

        ponto do trimestre Q = ativos em B na janela ÷ ativos em A na janela,
        janela = Q + os `roll` trimestres anteriores (móvel).

    O acumulado vale pra TODAS as etapas, não só a origem: um deal que entrou
    em Scheduled antes da janela e foi ganho dentro dela conta no Won — o won
    pode ocorrer a qualquer momento. Os valores batem 1:1 com o drill do
    trimestre (linha B ÷ linha A). Devolve (valores, denominadores)."""
    if f"s{a_num}" not in piv.columns or f"s{b_num}" not in piv.columns:
        return [None] * len(quarters), [0] * len(quarters)
    vals, dens = [], []
    for q in quarters:
        lo_q = q
        for _ in range(max(0, roll)):                 # início da janela móvel
            lo_q = _quarter_start_of(lo_q - timedelta(days=1))
        lo = pd.Timestamp(lo_q, tz="UTC")
        hi = pd.Timestamp(_quarter_end_of(q), tz="UTC") + pd.Timedelta(days=1)
        n = int(_stage_active_mask(piv, a_num, lo, hi).sum())
        if n:
            k = int(_stage_active_mask(piv, b_num, lo, hi).sum())
            vals.append(100.0 * k / n)
            dens.append(n)
        else:
            vals.append(None)
            dens.append(0)
    return vals, dens


def _get_ae_perf_with_managers(force: bool = False) -> pd.DataFrame:
    """ae_performance + overrides + quarter/quarter_end, mas SEM remover os
    gestores (diferente de _get_clean_ae_perf). Atribui o gestor ao próprio nó
    (leader = ele mesmo). Usado no /forecast pra somar vendas validadas (new_mrr)
    e on-hold dos AEs E das vendas diretas dos gestores. new_mrr do gestor é a
    venda direta dele (independe do time), então não duplica."""
    df_raw = bq.ae_performance(force_refresh=force)
    key = (_pl(), "ae_perf_mgrs_v2", _parquet_mtime("ae_performance"))  # ver nota da chave em _get_clean_ae_perf
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = df_raw.copy()
    df["quarter"] = pd.to_datetime(df["quarter"]).dt.date
    df["quarter_end"] = df["quarter"].map(_quarter_end_of)
    # Nome canônico ANTES do roster: é o rename que faz a linha órfã do AE casar
    # com o roster, e a fusão evita a quota sair dobrada.
    df = merge_ae_quarter_dups(canon_ae_names(df))
    df = apply_leader_overrides(df)
    df = apply_q3_roster_curated(df)        # time/quota Q3 autoritativos (só linhas Q3)
    # Mesma posição do outro loader, pelo mesmo motivo. Aqui o único gate é o
    # drop_departed_from (este loader não aplica ALLOWED_LEADERS/HIDDEN de
    # propósito, pra somar a venda direta dos gestores) — e a reatribuição de gestor
    # abaixo é no-op pra AE do roster, que nunca está em NON_AE_USERS.
    df = inject_roster_missing_curated(df)
    mgr = df["account_executive"].isin(NON_AE_USERS)
    df.loc[mgr, "leader"] = df.loc[mgr, "account_executive"]
    df = drop_departed_from(df, "quarter")
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _get_demand_deals(force: bool = False) -> pd.DataFrame:
    """Deals deal-level (pre-2024 → hoje) com source/bucket/leader/region já
    pré-computados. Substitui as queries por (date_from, date_to, grain) — todo
    slice/agregação acontece em pandas (sub-millisegundo) ao invés de 15-25s
    BQ por combinação."""
    raw = bq.demand_deals_raw(force_refresh=force, pipeline=_pl())
    # mtime da chave acompanha a versão da query (v5 = + coluna motion; v6 = +
    # estágio ao vivo e reached_scoping), senão um frame montado sobre o
    # parquet antigo sobreviveria a um reload a quente.
    # O nome do parquet leva o sufixo do funil (`_pk`): sem isso o frame do
    # México só era invalidado quando o parquet do BRASIL mudava — e o refresher
    # roda fora de request, ou seja, só aquece o BR. Dado novo do MX ficava preso
    # em memória até alguém reiniciar o processo.
    key = (_pl(), "demand_deals_v4",
           _parquet_mtime(bq._pk("demand_deals_raw_v6", _pl())))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    if _is_mx():
        # MÉXICO: só os 7 vendedores do organograma do Mauricio (MX_ROSTER).
        #
        # É a ÚNICA tela do MX que recorta assim, e é de propósito. O resto do
        # país roda com "o roster é o funil inteiro" porque mede ESTOQUE (o
        # pipeline aberto e o histórico de quem construiu ele existem mesmo
        # depois que a pessoa sai). Geração de demanda mede ESFORÇO da semana:
        # sem o corte, o gráfico ganhava 35 barras de gente que não trabalha
        # mais aqui — o funil MX tem 42 nomes desde 2024 contra os 7 de hoje —
        # e a meta de 3 opps/semana ficava ilegível no meio delas.
        #
        # O gestor fica fora junto (não está no MX_ROSTER, e NON_AE_USERS_MX já
        # o esconderia): ele fecha deal próprio mas não é vendedor.
        df = df[df["account_executive"].isin(MX_ROSTER)].copy()
    df["created_date"] = pd.to_datetime(df["created_date"])
    # BOOL do BQ com NULL vira object no pandas; o filtro da tela precisa de bool.
    df["reached_scoping"] = df["reached_scoping"].fillna(False).astype(bool)

    df["source"] = pd.Series("received", index=df.index)
    df.loc[df["sdr_id"].isna() | (df["sdr_id"] == df["account_executive_id"]), "source"] = "auto"

    # ------------------------------------------------------------------ canal
    # Classificador: propriedade `motion` do HubSpot, não mais `deal_source_agg`.
    #
    # Por quê (auditoria de 02/09/2026): `deal_source_agg` é derivada no dbt de
    # `source`, e chama de Outbound exatamente 5 valores — Outbound, Recovery,
    # Referral, LinkedIn, Linkedin Networking. O time parou de gravar
    # source='Outbound' em 17/06/2026 e migrou pra Cold Call / Cold Email /
    # Unattributed (ou deixou vazio), valores que caem no `else` = Inbound. Em
    # ago/2026 as 224 recebidas de aquisição viraram 100% Inbound: o gráfico
    # mostrava 0 outbound onde `motion` aponta 185. No ano, 888 contra 1.856.
    #
    # `motion` só está bem preenchida a partir de 2025 (em 2024, 71% dos deals
    # do funil estão sem valor), então sem motion cai de volta na regra antiga —
    # é o que preserva o histórico de 2024 em vez de jogá-lo todo em Inbound.
    # ------------------------------------------- mudança de motion no tempo
    # As duas faixas destacadas do gráfico são os deals em que a PRÓPRIA
    # propriedade motion mudou de lado depois de gravada — não a diferença
    # entre a regra velha e a nova. Vem das cópias diárias do BI (ver
    # bq.motion_history): a coluna motion entrou nelas em 16/04/2026, então
    # deal criado antes disso pode ter mudado sem aparecer aqui.
    hist = bq.motion_history(force_refresh=force)
    if len(hist):
        hist = hist[["deal_id", "motion_first", "n_changes", "motion_changed_at"]].copy()
        hist["deal_id"] = hist["deal_id"].astype(str)
        df["deal_id"] = df["deal_id"].astype(str)
        df = df.merge(hist, on="deal_id", how="left")
    else:
        df["motion_first"], df["n_changes"], df["motion_changed_at"] = pd.NA, 0, pd.NaT
    df["n_changes"] = pd.to_numeric(df["n_changes"], errors="coerce").fillna(0).astype(int)

    # As máscaras vêm todas DEPOIS do merge: ele devolve um frame novo, e uma
    # Series montada antes traria o índice velho pro mask().
    has_motion = df["motion"].notna() & (df["motion"].astype(str).str.strip() != "")
    is_outb = (df["motion"] == "Outbound") | (~has_motion & (df["deal_source_agg"] == "Outbound"))
    df["is_outbound"] = is_outb

    # Mudou de lado = saiu de um canal e terminou no outro. Quem oscilou e
    # voltou pro mesmo valor fica na faixa normal (n_changes registra isso).
    df["moved_to_in"]  = (df["motion_first"] == "Outbound") & (df["motion"] == "Inbound")
    df["moved_to_out"] = (df["motion_first"] == "Inbound")  & (df["motion"] == "Outbound")
    moved = df["moved_to_in"] | df["moved_to_out"]
    df["moved"] = moved

    is_auto = df["source"] == "auto"
    is_acq  = df["deal_type"] == "Aquisition"
    is_exp  = df["deal_type"] == "Up & cross sell"

    # Bucket canônico (7 + other) — mesmo set usado por by_ae e by_grain.
    # As recebidas de Aquisição abrem em 4: as duas normais e as duas em que a
    # propriedade motion do deal trocou de canal depois de gravada.
    bucket = pd.Series("other", index=df.index)
    bucket = bucket.mask(is_auto  & is_acq, "auto_acq")
    bucket = bucket.mask(is_auto  & is_exp, "auto_exp")
    bucket = bucket.mask(~is_auto & is_exp, "rcv_exp")
    rcv_acq = ~is_auto & is_acq
    bucket = bucket.mask(rcv_acq &  is_outb & ~moved, "rcv_outbound")
    bucket = bucket.mask(rcv_acq & ~is_outb & ~moved, "rcv_inbound")
    bucket = bucket.mask(rcv_acq & df["moved_to_out"], "rcv_moved_out")
    bucket = bucket.mask(rcv_acq & df["moved_to_in"],  "rcv_moved_in")
    df["bucket"] = bucket

    # Mapeia kind no shape antigo (acq/exp pra demand_generation; inbound/outbound/exp pra grain).
    # Mantemos source como "auto"/"received" (string).
    df["source"] = df["source"].astype(str)

    # Junta leader/region a partir do snapshot mais recente de ae_performance
    # (espelha o ae_meta CTE da SQL original).
    ae_full = canon_ae_names(bq.ae_performance(force_refresh=force))
    ae_full = apply_leader_overrides(ae_full)
    ae_full = ae_full[(ae_full["region"].notna()) & (ae_full["role"] == "AE")]
    ae_full["quarter"] = pd.to_datetime(ae_full["quarter"]).dt.date
    ae_meta = (ae_full.sort_values("quarter")
               .drop_duplicates("account_executive", keep="last")
               [["account_executive", "leader", "region"]])
    df = df.merge(ae_meta, on="account_executive", how="left")

    df = apply_roster_leader(df)
    if _is_mx():
        # Time do MX DEPOIS do merge, nunca antes: `ae_meta` também traz uma
        # coluna `leader`, e com ela já no frame o merge devolvia leader_x /
        # leader_y — a coluna `leader` que sobrava vinha do roster (BR) e
        # era nula pra quase todo mundo, então o gate `leader in
        # ALLOWED_LEADERS` da tela varria o México inteiro e o gráfico saía
        # VAZIO, sem erro nenhum. Um gestor só pro país (ver ORG_CHART).
        df["leader"] = MX_LEADER
        df["region"] = "MX"
    df = drop_departed_from(df, "created_date")
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _get_visits(force: bool = False) -> pd.DataFrame:
    """Visitas (HubSpot) por AE, nível-visita, com leader/region anexados pela
    MESMA atribuição de time do resto do app (snapshot ae_performance + roster Q3).
    Colunas: account_executive, start_date, category ('field'|'ia_center'), leader, region."""
    raw = bq.visits_raw(force_refresh=force)
    key = (_pl(), "visits", _parquet_mtime("visits_raw_v2"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    ae_full = canon_ae_names(bq.ae_performance(force_refresh=force))
    ae_full = apply_leader_overrides(ae_full)
    ae_full = ae_full[(ae_full["region"].notna()) & (ae_full["role"] == "AE")]
    ae_full["quarter"] = pd.to_datetime(ae_full["quarter"]).dt.date
    ae_meta = (ae_full.sort_values("quarter")
               .drop_duplicates("account_executive", keep="last")
               [["account_executive", "leader", "region"]])
    df = df.merge(ae_meta, on="account_executive", how="left")
    df = apply_roster_leader(df)
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _demand_by_ae_pd(deals: pd.DataFrame, date_from: date, date_to: date) -> pd.DataFrame:
    """Pandas equivalent de bq.demand_generation: per-AE bucket counts.
    Output idêntico em colunas/semântica ao retorno do BQ."""
    cols = ["account_executive","leader","region",
            "auto_acq","auto_exp","rcv_outbound","rcv_inbound",
            "rcv_moved_out","rcv_moved_in","rcv_exp",
            "rcv_outbound_all","rcv_inbound_all","total_generated"]
    if len(deals) == 0:
        return pd.DataFrame(columns=cols)
    mask = ((deals["created_date"] >= pd.Timestamp(date_from)) &
            (deals["created_date"] <= pd.Timestamp(date_to)) &
            (deals["bucket"] != "other"))
    d = deals.loc[mask, ["account_executive","leader","region","bucket"]]
    if len(d) == 0:
        return pd.DataFrame(columns=cols)
    pivot = (d.groupby(["account_executive","leader","region","bucket"], dropna=False)
              .size().unstack(fill_value=0).reset_index())
    for col in ["auto_acq","auto_exp","rcv_outbound","rcv_inbound",
                "rcv_moved_out","rcv_moved_in","rcv_exp"]:
        if col not in pivot.columns:
            pivot[col] = 0
    # Os 4 buckets de recebida-aquisição são disjuntos (a barra empilha os 4 sem
    # dupla contagem); `_all` é o consolidado por canal, pros números-resumo.
    pivot["rcv_outbound_all"] = pivot["rcv_outbound"] + pivot["rcv_moved_out"]
    pivot["rcv_inbound_all"]  = pivot["rcv_inbound"]  + pivot["rcv_moved_in"]
    pivot["total_generated"] = (pivot["auto_acq"] + pivot["auto_exp"]
                                + pivot["rcv_outbound"] + pivot["rcv_inbound"]
                                + pivot["rcv_moved_out"] + pivot["rcv_moved_in"]
                                + pivot["rcv_exp"])
    return (pivot[pivot["total_generated"] > 0]
            .sort_values("total_generated", ascending=False)
            .reset_index(drop=True))


def _txt_cel(v, dash: str = "—") -> str:
    """Valor de texto pra célula de drill, com NaN virando travessão.

    `r.get("x") or "—"` NÃO resolve: `float('nan')` é TRUTHY, então o NaN passava
    inteiro pro `jsonify`, que escreve `NaN` cru — e isso é JSON inválido. O
    `JSON.parse` do navegador aborta a resposta TODA, então a modal do drill
    morria com "Erro ao carregar" em qualquer lista que tivesse um único deal
    sem histórico de motion. Vale pro Brasil também: o bug entrou junto com as
    colunas de motion (02/09/2026) e vivia nas duas regiões."""
    if v is None:
        return dash
    try:
        if pd.isna(v):
            return dash
    except (TypeError, ValueError):
        pass
    t = str(v).strip()
    return t if t and t.lower() not in ("nan", "nat", "none") else dash


def _demand_deals_rows(deals: pd.DataFrame, date_from: date, date_to: date,
                       name: str = None, grain: str = "ae",
                       leader: str = None, region: str = None,
                       source: str = None, bucket: str = None,
                       scoping: bool = False) -> list:
    """Drill-down de demanda no MESMO universo das contagens (criados na janela,
    bucket != other). Cada linha traz nome, tipo, qualification owner (SDR; ou o
    próprio AE quando auto), estágio, valor e data de criação.

    Recorte igual ao dos outros drills (`_filter_entity`):
      · `name` + grain='ae'     → só os deals daquele vendedor;
      · `name` + grain='leader' → os deals de TODOS os AEs que respondem àquele
        líder (expand_leader, transitivo — pega gerente sob diretor);
      · sem `name`              → TOTAL, com filtros opcionais de líder/região.

    `source='auto'|'received'` restringe à origem — usado pelos gráficos que
    contam só um dos lados, pra lista explodida fechar com o número da barra.
    `scoping=True` fica só com os que chegaram ao estágio 3 (`reached_scoping`)."""
    if len(deals) == 0:
        return []
    mask = ((deals["created_date"] >= pd.Timestamp(date_from)) &
            (deals["created_date"] <= pd.Timestamp(date_to)) &
            (deals["bucket"] != "other"))
    if source in ("auto", "received"):
        mask &= (deals["source"] == source)
    if scoping:
        mask &= deals["reached_scoping"]
    # `bucket` fecha a lista com UM segmento da barra (ex.: só os que passaram a
    # Inbound). Aceita vários separados por vírgula.
    if bucket:
        # Pedido sem nenhum segmento válido (ex.: todos os chips da legenda
        # desligados) é lista VAZIA, nunca "todos".
        wanted = {b.strip() for b in bucket.split(",") if b.strip() in _BUCKET_TO_SOURCE_KIND}
        mask &= deals["bucket"].isin(wanted)
    d = deals.loc[mask]
    # O gate de visibilidade vem ANTES do split de grão, e não só no `else`.
    # Antes ele existia apenas no ramo de líder/total: pedir um `name` de AE
    # pulava a fronteira inteira, e um drill do Brasil com nome de vendedor
    # mexicano devolvia 222 deals do México rotulados em R$. Achado por auditoria
    # em 24/08/2026. O `name` vem de querystring, ou seja é entrada do usuário —
    # gate que depende do grão pedido não é gate.
    d = d[d["leader"].isin(ALLOWED_LEADERS)]
    d = d[~d["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    # Clique (AE/líder) + filtros de quem da tela por cima — o mesmo recorte de
    # todo drill (`_entity_filter`).
    d = _entity_filter(d, name, grain, leader, region)
    d = d.sort_values("created_date", ascending=False)
    out = []
    for _, r in d.iterrows():
        cd = r["created_date"]
        out.append({
            "deal_id":      str(r.get("deal_id") or ""),
            "deal_url":     _hs_deal_url(r.get("deal_id")),
            "ae":           r["account_executive"],
            "deal_name":    _txt_cel(r.get("deal_name")),
            "deal_type":    _txt_cel(r.get("deal_type")),
            "qual_owner":   _txt_cel(r.get("sdr"), r["account_executive"]),
            "stage":        stage_display(_demand_stage_ref(r)),
            "amount":       float(r.get("amount") or 0),
            "created_date": cd.date().isoformat() if pd.notna(cd) else "",
            "scoping_date": (str(r.get("scoping_reached_date"))[:10]
                             if pd.notna(r.get("scoping_reached_date")) else ""),
            # Rastro da classificação de canal, pra conferência deal a deal.
            "motion":       _txt_cel(r.get("motion")),
            "motion_first": _txt_cel(r.get("motion_first")),
            "motion_changed_at": (str(r.get("motion_changed_at"))[:10]
                                  if pd.notna(r.get("motion_changed_at")) else ""),
            "n_changes":    int(r.get("n_changes") or 0),
            "hs_source":    _txt_cel(r.get("deal_source_agg")),
            "canal":        _CANAL_LABEL.get(r.get("bucket"), "—"),
        })
    return out


def _demand_stage_ref(r):
    """Estágio do deal no drill de demanda: o AO VIVO (`dealstage`, da view que
    mescla as fontes) quando é um estágio conhecido; senão o rótulo da curated,
    que só enxerga a legada (parada desde 11/10/2026). Sem isso, deal contado
    como "chegou a Scoping" pelo estágio atual aparecia como Qualifying."""
    live = r.get("dealstage")
    if isinstance(live, str) and (live in _STAGE_BY_ID or live.lower() in _STAGE_TERMINAL_LABELS):
        return live
    return r.get("stage_label")


# Rótulo humano do bucket, mostrado no drill-down. Os dois "passou a" são os
# deals em que a troca de deal_source_agg → motion mudou o lado da conta.
_CANAL_LABEL = {
    "auto_acq":      "Próprio · Aquisição",
    "auto_exp":      "Próprio · Expansão",
    "rcv_exp":       "Expansão",
    "rcv_outbound":  "Outbound",
    "rcv_inbound":   "Inbound",
    "rcv_moved_out": "Motion virou Outbound",
    "rcv_moved_in":  "Motion virou Inbound",
}


# Mapa bucket canônico → (source, kind) no shape esperado pela rota grain.
_BUCKET_TO_SOURCE_KIND = {
    "auto_acq":      ("auto",     "acq"),
    "auto_exp":      ("auto",     "exp"),
    "rcv_exp":       ("received", "exp"),
    "rcv_outbound":  ("received", "outbound"),
    "rcv_inbound":   ("received", "inbound"),
    "rcv_moved_out": ("received", "moved_out"),
    "rcv_moved_in":  ("received", "moved_in"),
}


def _trunc_to_grain(dates: pd.Series, grain: str) -> pd.Series:
    """Equivalente pandas de DATE_TRUNC(d, WEEK(MONDAY)/MONTH/QUARTER) do BQ."""
    if grain == "week":
        # Segunda-feira da semana
        return (dates - pd.to_timedelta(dates.dt.weekday, unit="D")).dt.normalize()
    if grain == "quarter":
        return dates.dt.to_period("Q").dt.start_time
    return dates.dt.to_period("M").dt.start_time


def _demand_by_grain_pd(deals: pd.DataFrame, date_from: date, date_to: date,
                         grain: str) -> pd.DataFrame:
    """Pandas equivalent de bq.demand_generation_grain. Mesmo shape de saída
    [period, account_executive, leader, region, source, kind, n] — a rota
    pode pivotar sem alteração."""
    cols = ["period","account_executive","leader","region","source","kind","n"]
    if len(deals) == 0:
        return pd.DataFrame(columns=cols)
    mask = ((deals["created_date"] >= pd.Timestamp(date_from)) &
            (deals["created_date"] <= pd.Timestamp(date_to)) &
            (deals["bucket"] != "other"))
    d = deals.loc[mask, ["created_date","account_executive","leader","region","bucket"]]
    if len(d) == 0:
        return pd.DataFrame(columns=cols)
    period = _trunc_to_grain(d["created_date"], grain).dt.date
    src = d["bucket"].map(lambda b: _BUCKET_TO_SOURCE_KIND[b][0])
    kind = d["bucket"].map(lambda b: _BUCKET_TO_SOURCE_KIND[b][1])
    out = (d.assign(period=period, source=src, kind=kind)
            .groupby(["period","account_executive","leader","region","source","kind"],
                     dropna=False)
            .size().rename("n").reset_index())
    return out.sort_values("period").reset_index(drop=True)


def _demand_last_week(deals: pd.DataFrame, f: dict):
    """Opps geradas pelo PRÓPRIO AE (auto) na última semana fechada, vs meta de 3.

    Janela = seg→dom da semana que fechou, ancorada em `_last_two_sundays()` (o
    mesmo marco 12h BRT dos snapshots WoW). Por isso a janela vira sozinha todo
    domingo no mesmo horário dos outros snapshots, sem cron próprio.

    Ignora de propósito o filtro de data da página — este gráfico é SEMPRE a
    semana passada. Já os filtros de líder/região/AE valem, pra cada gestor ver
    só o seu time.

    Universo = ROSTER (time vigente) ∪ quem gerou na semana. Vem do roster, e
    não do `by_ae` da tela, justamente pra que o filtro de data da página não
    mexa em quantos AEs aparecem aqui. AE que gerou zero entra com barra 0 — é
    quem a gestão precisa cobrar. Retorna (rows, stats, max_y)."""
    last_sun, _ = _last_two_sundays()
    wk_from, wk_to = last_sun - timedelta(days=6), last_sun

    wk = _demand_by_ae_pd(deals, wk_from, wk_to)
    wk = apply_leader_overrides(wk)
    wk = wk[wk["leader"].isin(ALLOWED_LEADERS)]
    wk = wk[~wk["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f["region"]:
        wk = wk[wk["region"] == f["region"]]
    if f["leader"]:
        wk = wk[wk["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        wk = wk[wk["account_executive"] == f["ae"]]

    wk_recs = wk.to_dict("records")
    counts = {r["account_executive"]: (int(r["auto_acq"]), int(r["auto_exp"]))
              for r in wk_recs}

    # Roster do quarter, sujeito aos MESMOS filtros de líder/região/AE.
    leaders_ok = expand_leader(f["leader"]) if f["leader"] else None
    roster = {}
    for ae_name, cfg in ROSTER.items():
        if ae_name in HIDDEN_FROM_AE_VIEWS:
            continue
        if f["ae"] and ae_name != f["ae"]:
            continue
        if f["region"] and f["region"] != _roster_region():
            continue
        if leaders_ok is not None and cfg["leader"] not in leaders_ok:
            continue
        roster[ae_name] = cfg["leader"]
    # Quem gerou na semana mas não está no roster do quarter não pode sumir.
    for r in wk_recs:
        roster.setdefault(r["account_executive"], r.get("leader") or "")

    rows = []
    for ae_name, ae_leader in roster.items():
        acq, exp = counts.get(ae_name, (0, 0))
        rows.append({
            "account_executive": ae_name,
            "leader": ae_leader,
            "auto_acq": acq,
            "auto_exp": exp,
            "auto_total": acq + exp,
            "hit": (acq + exp) >= WEEKLY_AE_GOAL,
        })
    rows.sort(key=lambda r: (-r["auto_total"], r["account_executive"]))

    max_y = max([r["auto_total"] for r in rows] + [WEEKLY_AE_GOAL, 1])
    stats = {
        "goal":  WEEKLY_AE_GOAL,
        "n_ae":  len(rows),
        "n_hit": sum(1 for r in rows if r["hit"]),
        "total": sum(r["auto_total"] for r in rows),
        "date_from": wk_from.isoformat(),
        "date_to":   wk_to.isoformat(),
        "label": f"{_fmt_date_short(wk_from)} → {_fmt_date_short(wk_to)}",
    }
    return rows, stats, max_y


# ============================================================
# Pipeline (snapshot atual de deals abertos no pipeline Sales BR)
# ============================================================
# Stages do HubSpot — SEMPRE classificados/identificados pelo dealstage_id RAW
# (estável), NUNCA pelo texto do label. O label é renomeado livremente no HubSpot:
# todos ganharam prefixo "Stage N - " e "Consensus" virou "Stage 5 - Decision".
# O id (87188634, decisionmakerboughtin, ...) não muda através de renomeações.

# ---- Registro canônico do funil Sales BR — FONTE ÚNICA de nome/número/ordem ----
# Cada entrada:
#   id      dealstage_id RAW do HubSpot (imutável; chave de tudo no app)
#   num     número do estágio no HubSpot (1..6)
#   name    nome curto atual
#   label   label completo atual (como aparece hoje no HubSpot)
#   abbr    abreviação p/ UIs compactas
#   aliases nomes/rótulos antigos que ainda devem resolver (ex.: "Consensus")
# Lista JÁ em ordem de funil (num crescente) — é a ordem de exibição e de avanço.
# `label` = o que aparece na tela: SÓ o nome do estágio, sem "Stage N - ". O
# prefixo numerado do HubSpot fica em `aliases`, que é o que resolve o texto cru
# vindo do BQ de volta pro estágio canônico.
STAGE_REGISTRY = [
    {"id": "appointmentscheduled",  "num": 1, "name": "Scheduled",
     "label": "Scheduled",    "abbr": "Sched", "aliases": ("stage 1 - scheduled",)},
    {"id": "contractsent",          "num": 2, "name": "Qualifying",
     "label": "Qualifying",   "abbr": "Qual",  "aliases": ("qualification", "stage 2 - qualifying")},
    {"id": "93878633",              "num": 3, "name": "Scoping",
     "label": "Scoping",      "abbr": "Scop",  "aliases": ("project scoping", "stage 3 - scoping")},
    {"id": "decisionmakerboughtin", "num": 4, "name": "Negotiation",
     "label": "Negotiation",  "abbr": "Neg",   "aliases": ("stage 4 - negotiation",)},
    {"id": "87188634",              "num": 5, "name": "Decision",
     "label": "Decision",     "abbr": "Dec",   "aliases": ("consensus", "stage 5 - decision")},
    {"id": "65917881",              "num": 6, "name": "Procurement",
     "label": "Procurement",  "abbr": "Proc",  "aliases": ("stage 6 - procurement",)},
]
# Estágios elegíveis na conversão de funil: 1..6 do registry + Won como 7.
_CONV_STAGES = ([(s["num"], s["id"], s["label"]) for s in STAGE_REGISTRY]
                + [(7, "closedwon", "Won")])
_CONV_BY_NUM = {n: (sid, lbl) for n, sid, lbl in _CONV_STAGES}
# Cores das 4 linhas de conversão.
_CONV_COLORS = ["#2563EB", "#0F6E56", "#B45309", "#7C3AED"]
# Pares padrão (origem, destino): 3 passos consecutivos + win rate ponta a ponta.
_CONV_DEFAULTS = [(1, 2), (2, 3), (3, 4), (1, 7)]

_STAGE_BY_ID  = {s["id"]: s for s in STAGE_REGISTRY}
_STAGE_BY_NUM = {s["num"]: s for s in STAGE_REGISTRY}


def _norm_stage_ref(ref) -> str:
    """Normaliza texto: minúscula, sem 'stage/estágio/etapa/#/-/:', espaço único."""
    s = str(ref).strip().lower()
    for junk in ("stage", "estágio", "estagio", "etapa", "#", "-", ":"):
        s = s.replace(junk, " ")
    return " ".join(s.split())


def resolve_stage(ref):
    """Resolve QUALQUER referência a um estágio → entrada canônica do STAGE_REGISTRY.

    Indiferente a caixa/acento/prefixo, aceita:
      • número       5, "5", "Stage 5", "estágio 5"
      • nome         "Decision", "decision", "Negotiation"
      • label full   "Stage 5 - Decision"
      • dealstage_id "87188634", "decisionmakerboughtin"
      • nome antigo  "Consensus"  (alias → Decision)
    Retorna a dict canônica, ou None se não resolver."""
    if ref is None:
        return None
    if str(ref) in _STAGE_BY_ID:                 # dealstage_id raw
        return _STAGE_BY_ID[str(ref)]
    norm = _norm_stage_ref(ref)
    if norm.isdigit():                           # número (com/sem "stage")
        return _STAGE_BY_NUM.get(int(norm))
    for s in STAGE_REGISTRY:                      # match exato de nome/label/alias
        cands = {s["name"].lower(), s["label"].lower(), _norm_stage_ref(s["label"]),
                 *(a.lower() for a in s["aliases"])}
        if norm in cands:
            return s
    for s in STAGE_REGISTRY:                      # fallback: nome/alias contido no texto
        if s["name"].lower() in norm or any(a.lower() in norm for a in s["aliases"]):
            return s
    return None


def stage_id(ref):
    """Atalho: referência → dealstage_id canônico (ou None)."""
    s = resolve_stage(ref)
    return s["id"] if s else None


# Prefixo numerado que o HubSpot põe no label ("Stage 3 - Scoping", "Etapa 2: X").
_STAGE_PREFIX_RE = re.compile(r"^\s*(?:stage|est[áa]gio|etapa)\s*\d+\s*[-–:]?\s*",
                              re.IGNORECASE)


# Terminais não estão no STAGE_REGISTRY (não são etapas do funil), mas aparecem
# nos drills como id raw do HubSpot.
_STAGE_TERMINAL_LABELS = {"closedwon": "Won", "closedlost": "Lost",
                          # No Show: stage do HubSpot fora do registry do funil —
                          # sem isso o drill mostrava o id cru "53862044".
                          "53862044": "No Show"}


def stage_display(ref) -> str:
    """Nome do estágio pra TELA — sem "Stage N - ". Resolve pelo registry quando
    reconhece; se não reconhecer (estágio novo/fora do funil Sales BR), só remove
    o prefixo numerado em vez de esconder o valor."""
    if ref is None or (isinstance(ref, float) and pd.isna(ref)):
        return "—"
    txt = str(ref).strip()
    if not txt:
        return "—"
    if txt.lower() in _STAGE_TERMINAL_LABELS:
        return _STAGE_TERMINAL_LABELS[txt.lower()]
    s = resolve_stage(txt)
    if s:
        return s["label"]
    return _STAGE_PREFIX_RE.sub("", txt) or txt


# IDs raw do `dealstage` no HubSpot pro funil Sales BR. Usado quando reconstruimos
# snapshots históricos via `hubspot_deals_history` (que carrega só o stage_id).
#   Project Scoping           → 93878633              (Early)
#   Decision (ex-"Consensus") → 87188634              (Late)
#   Negotiation               → decisionmakerboughtin (Late)
#   Procurement               → 65917881              (Late)
#   PO/WON                    → closedwon
#   Lost                      → closedlost
_HS_EARLY_STAGES = {"93878633"}
_HS_LATE_STAGES  = {"87188634", "decisionmakerboughtin", "65917881"}
_HS_WON_STAGE    = "closedwon"
_HS_LOST_STAGE   = "closedlost"


def _hs_stage_group(stage_id) -> str:
    """Classifica um dealstage_id raw em 'early','late','won','lost','pre','none'."""
    if not isinstance(stage_id, str) or not stage_id:
        return "none"
    if stage_id in _HS_EARLY_STAGES: return "early"
    if stage_id in _HS_LATE_STAGES:  return "late"
    if stage_id == _HS_WON_STAGE:    return "won"
    if stage_id == _HS_LOST_STAGE:   return "lost"
    return "pre"


# Stages com SLA de inatividade — cada um com seu threshold (em dias) acima do
# qual o deal entra na lista de "parado". Usado em /pipeline/stalled.
_STALLED_THRESHOLDS = {
    "contractsent":          14,  # Stage 2 - Qualifying
    "93878633":              21,  # Stage 3 - Project Scoping
    "decisionmakerboughtin": 14,  # Stage 4 - Negotiation
    "87188634":              14,  # Stage 5 - Decision (ex-Consensus)
    "65917881":              21,  # Stage 6 - Procurement
}
# Inatividade (dias sem RETORNO do cliente): limite ÚNICO em todos os estágios.
# Decisão do Pedro em 09/10/2026: "todo deal que tenha mais de 7 dias sem
# interação significativa com o cliente entre no dado". Antes a Inatividade
# usava os mesmos SLAs do Stale (14/21 por estágio). O Stale (dias NO estágio)
# segue com os SLAs acima — são perguntas diferentes.
ACTIVITY_SLA_DAYS = 7
# Ordem de exibição do stalled (Negotiation antes de Decision, igual ao funil).
_STALLED_STAGE_ORDER  = ["contractsent", "93878633",
                         "decisionmakerboughtin", "87188634", "65917881"]
_STALLED_STAGE_LABELS = {sid: _STAGE_BY_ID[sid]["label"] for sid in _STALLED_STAGE_ORDER}
_STALLED_STAGE_KEYS   = {  # chave curta ESTÁVEL usada em data-attrs/JS do template.
    "contractsent":          "qual",   # NÃO mudar sem mexer no template correspondente.
    "93878633":              "proj",
    "decisionmakerboughtin": "nego",
    "87188634":              "cons",   # id 87188634 = Decision (chave 'cons' mantida p/ o JS)
    "65917881":              "proc",
}
_STALLED_STAGE_COLORS = {  # 5 cores DISTINTAS (sem verde/vermelho), escala Radix 9 —
    "contractsent":          "#3E63DD",  # Qualifying  · indigo     mesma luminância,
    "93878633":              "#00A2C7",  # Scoping     · ciano      paleta harmonizada
    "decisionmakerboughtin": "#6E56CF",  # Negotiation · violeta    (padrão Linear/Radix)
    "87188634":              "#D6409F",  # Decision    · rosa
    "65917881":              "#F76B15",  # Procurement · laranja
}
# Versão ESCURA da mesma paleta (Radix 11), só pra TEXTO. As cores acima são de
# PREENCHIMENTO: têm luminância igual entre si de propósito (é o que faz as 5
# barras conviverem), e por isso o ciano e o laranja reprovam em contraste
# quando viram número/rótulo sobre fundo claro — medido: 2,7:1 e 3,8:1, contra
# 4,5:1 exigido. Preenchimento e tinta são papéis diferentes; misturar os dois
# num único hex força escolher entre gráfico bonito e texto legível.
_STALLED_STAGE_INKS = {
    "contractsent":          "#3451B2",  # indigo 11
    "93878633":              "#0C7792",  # ciano 11
    "decisionmakerboughtin": "#5B45B0",  # violeta 11
    "87188634":              "#B02A82",  # rosa 11
    "65917881":              "#C2410C",  # laranja 11
}

# A Inatividade cobre também o Stage 1 - Scheduled (09/10/2026, pedido do Pedro:
# "todo deal com mais de 7 dias sem interação"). O Stale segue só com os 5
# estágios de SLA (_STALLED_STAGE_ORDER). As tabelas de chave/rótulo/cor ganham
# o Stage 1 sem mudar a ordem do Stale — quem itera é a ORDEM de cada tela.
_STALLED_STAGE_KEYS["appointmentscheduled"]   = "sched"
_STALLED_STAGE_LABELS["appointmentscheduled"] = _STAGE_BY_ID["appointmentscheduled"]["label"]
_STALLED_STAGE_COLORS["appointmentscheduled"] = "#8B8D98"   # slate 9 — neutro, estágio de entrada
_STALLED_STAGE_INKS["appointmentscheduled"]   = "#60646C"   # slate 11
_ACTIVITY_STAGE_ORDER = ["appointmentscheduled"] + _STALLED_STAGE_ORDER
_ACTIVITY_THRESHOLDS = {sid: ACTIVITY_SLA_DAYS for sid in _ACTIVITY_STAGE_ORDER}

# Ordem do funil de vendas Sales BR — usado em /pipeline/funnel pra mostrar
# WoW por estágio (qtd e amount). Derivado do STAGE_REGISTRY (ordem canônica).
_FUNNEL_STAGES     = [(s["id"], s["label"]) for s in STAGE_REGISTRY]
_FUNNEL_STAGE_ABBR = {s["id"]: s["abbr"] for s in STAGE_REGISTRY}
# Ordem linear pro funil — index maior = mais avançado. No Show entra entre
# Scheduled e Qualifying; closedwon é a saída por avanço final; closedlost fica
# fora (categoria separada). Deriva do num do registry (Negotiation=4 < Decision=5).
_FUNNEL_ORDER_MAP = {
    "appointmentscheduled": 0,
    "53862044":             1,  # No Show
    **{s["id"]: s["num"] for s in STAGE_REGISTRY if s["id"] != "appointmentscheduled"},
    "closedwon":            7,
}

# Forecast category → probabilidade, exatamente como está descrito nos parâmetros
# da propriedade `hs_manual_forecast_category` no HubSpot:
#   Funnel - 25% · Probable - 50% · Best Case - 75% · Commit - 100% · Not forecasted 0%
FORECAST_CATEGORY_PCT = {
    "COMMIT": 1.00, "BEST_CASE": 0.75, "PROBABLE": 0.50,
    "PIPELINE": 0.25, "CLOSED": 1.00, "OMIT": 0.0,
}
# Fator de correção aplicado a TODA projeção do MODELO DOS DIRETORES — o
# ponderado por `hs_manual_forecast_category`. Cada peso vira peso × 0,90 (10% a
# menos, relativo), mesma ideia e mesmo tamanho do MODEL_SAFETY_FACTOR do Modelo
# Analytics. Vale igual no trimestre corrente e em TODA a série histórica: as
# duas leem desta mesma tabela, e o histórico é RECALCULADO a cada request a
# partir de amount + forecast category CRUS (materialized_data_source e
# pepeuhacker.forecast_snapshots guardam o fato, nunca o ponderado) — então não
# existe número gravado pra corrigir, e nem risco de passado e presente
# discordarem. Muda aqui p/ ajustar.
DIR_FORECAST_FACTOR = 0.90
# Peso EFETIVO por categoria: é ESTE que multiplica o amount em qualquer
# projeção. A tabela crua acima continua existindo pros RÓTULOS (100/75/50/25%),
# que são o parâmetro do HubSpot e não mudam com o fator. Won não passa por
# aqui: é receita realizada (`amount` cheio), não previsão.
FORECAST_CATEGORY_PCT_EFF = {k: v * DIR_FORECAST_FACTOR
                             for k, v in FORECAST_CATEGORY_PCT.items()}
# Global de template: toda tela que mostra ponderado dos diretores diz o fator na
# cara, do mesmo jeito que `forecast_model.html` diz o desconto de segurança do
# Modelo Analytics. Um número que sai 10% menor sem aviso vira dúvida de bug.
app.jinja_env.globals["dir_factor"] = DIR_FORECAST_FACTOR
# Categorias previstas, da maior confiança pra menor (ordem de segmentos/tabela).
FORECAST_CATEGORY_ORDER = ["COMMIT", "BEST_CASE", "PROBABLE", "PIPELINE"]
# Rótulo = o PESO, não o nome do HubSpot (pedido em 24/08/2026). "Commit" e
# "Best Case" só significam alguma coisa pra quem decorou a tabela do modelo; o
# número já é a informação — e é o mesmo número que multiplica o amount no
# ponderado. `OMIT` (Not forecasted) vira "0%" porque é peso zero de verdade;
# "sem categoria" continua por extenso em _PT_DIMS, porque ausência de valor NÃO
# é peso 0 e a distinção sustenta a linha de cobertura do ponderado.
# CLOSED fica por extenso: é deal ganho, não um bucket de probabilidade — "100%"
# ali colidiria com Commit e leria como previsão.
FORECAST_CATEGORY_LABEL = {
    "COMMIT": "100%", "BEST_CASE": "75%", "PROBABLE": "50%",
    "PIPELINE": "25%", "OMIT": "0%", "CLOSED": "Closed won",
}
FORECAST_CATEGORY_COLOR = {   # preenchimento (barra/ponto) — rampa clara→escura
    "COMMIT": "#047857", "BEST_CASE": "#10B981",
    "PROBABLE": "#2563EB", "PIPELINE": "#93C5FD",
}
# Tinta (texto) da mesma rampa. As duas pontas claras (#10B981 e sobretudo o
# #93C5FD do Funnel) existem pra dar gradação nas barras, mas como texto de 12px
# medem 3,3:1 e 2,5:1 — abaixo do mínimo de 4,5:1. Ver nota em .leg-toggle.is-on.
FORECAST_CATEGORY_INK = {
    "COMMIT": "#065F46", "BEST_CASE": "#047857",
    "PROBABLE": "#1D4ED8", "PIPELINE": "#1E40AF",
}

def _fc_key(cat: str) -> str:
    """Chave curta sem underscore p/ usar em data-attrs/JS (BEST_CASE→bestcase)."""
    return cat.lower().replace("_", "")


# ---- Modelo de forecast por conversão histórica (sub-aba "Modelo Analytics") ----
# Probabilidade de fechamento por SEGMENTO × ESTÁGIO × TIPO (aquisição/expansão),
# medida em Q4+Q1. Aplicada aos deals ABERTOS nos 3 estágios abaixo pra projetar
# quanto deve converter. Fonte: tabela fornecida pela liderança.
MODEL_STAGE = {  # HubSpot dealstage → estágio do modelo
    "decisionmakerboughtin": "negotiation",
    "87188634":              "consensus",
    "65917881":              "procurement",
}
# Ordem do modelo = ordem do funil (Negotiation → Decision → Procurement). A chave
# interna "consensus" é mantida (indexa FORECAST_MODEL_PROB); só o label muda.
MODEL_STAGE_ORDER = ["negotiation", "consensus", "procurement"]
MODEL_STAGE_LABEL = {"negotiation": _STAGE_BY_ID["decisionmakerboughtin"]["label"],
                     "consensus":   _STAGE_BY_ID["87188634"]["label"],
                     "procurement": _STAGE_BY_ID["65917881"]["label"]}
# Segmento de cada líder (times inteiros seguem o segmento do gestor).
LEADER_SEGMENT = {
    "Bernardo Agosti": "commercial", "João Tavares": "commercial",
    "Fernando Barati": "midmarket",  "Arthur Tkotz":  "midmarket",
    "Leonardo Scopel": "enterprise", "Gustavo Giovani": "enterprise",
}
SEGMENT_LABEL = {"enterprise": "Enterprise", "midmarket": "MidMarket",
                 "commercial": "Commercial"}
FORECAST_MODEL_PROB = {
    "enterprise": {
        "negotiation": {"acq": 0.36, "exp": 0.61},
        "consensus":   {"acq": 0.67, "exp": 0.79},
        "procurement": {"acq": 0.83, "exp": 0.90},
    },
    "midmarket": {
        "negotiation": {"acq": 0.34, "exp": 0.61},
        "consensus":   {"acq": 0.76, "exp": 0.86},
        "procurement": {"acq": 0.88, "exp": 0.95},
    },
    "commercial": {
        "negotiation": {"acq": 0.30, "exp": 0.58},
        "consensus":   {"acq": 0.73, "exp": 0.86},
        "procurement": {"acq": 0.92, "exp": 0.95},
    },
}
# Desconto de segurança aplicado a TODAS as probabilidades (conservadorismo):
# cada % vira % × 0,90 (10% a menos, relativo). Muda aqui p/ ajustar.
MODEL_SAFETY_FACTOR = 0.90


# México: o MESMO modelo, com o segmento do VENDEDOR (tier do MX_ROSTER) no lugar
# do segmento do líder — lá um gestor só cobre os três segmentos, e sem isto
# todo deal mexicano pesava 0 e o Modelo Analytics do México zerava.
_MX_TIER_SEGMENT = {"Enterprise": "enterprise", "Mid Market": "midmarket",
                    "Account Executive": "commercial"}


def _mx_ae_segment(ae):
    return _MX_TIER_SEGMENT.get(MX_ROSTER.get(ae, {}).get("tier"))


def _model_prob(leader, dealstage, deal_type, ae=None) -> float:
    """Probabilidade do modelo p/ um deal aberto, já com o desconto de segurança de
    10% (× MODEL_SAFETY_FACTOR). 0 se fora do escopo (estágio não modelado — só
    Negotiation/Decision/Procurement —, tipo desconhecido ou líder sem segmento)."""
    seg = _mx_ae_segment(ae) if _is_mx() else LEADER_SEGMENT.get(leader)
    st = MODEL_STAGE.get(dealstage)
    kind = ("acq" if deal_type == "Aquisition"
            else "exp" if deal_type == "Up & cross sell" else None)
    if not seg or not st or not kind:
        return 0.0
    return FORECAST_MODEL_PROB[seg][st][kind] * MODEL_SAFETY_FACTOR


def _funnel_dir(prev, curr) -> str:
    """Classifica a movimentação WoW do deal em relação ao funil:
      'stay'    — não mudou de stage
      'advance' — foi pra um stage de index maior (ou Won)
      'lost'    — virou closedlost
      'regress' — voltou pra stage de index menor
      'other'   — algum dos lados está fora do mapping (ex: deal novo)"""
    if prev == curr:
        return "stay"
    if curr == "closedlost":
        return "lost"
    p = _FUNNEL_ORDER_MAP.get(prev, -1)
    c = _FUNNEL_ORDER_MAP.get(curr, -1)
    if p == -1 or c == -1:
        return "other"
    if c > p: return "advance"
    if c < p: return "regress"
    return "stay"


def _pipeline_bucket(stage_id: str) -> str:
    """Classifica um deal em early/late/other pelo dealstage_id RAW do HubSpot.

    Usa ID (estável) em vez do stage_label (texto), porque o label é renomeado
    livremente no HubSpot — ex.: 'Consensus' virou 'Stage 5 - Decision', o que
    quebrava o match por substring e derrubava esses deals de 'late' pra 'other'.
    O ID (87188634, etc.) permanece intacto através de renomeações."""
    grp = _hs_stage_group(stage_id)
    if grp == "early":
        return "early"
    if grp == "late":
        return "late"
    return "other"


def _get_pipeline_deals(force: bool = False) -> pd.DataFrame:
    """Todos os deals do funil Sales BR com bucket (early/late/other), is_open,
    hs_closedate (close date do HubSpot raw) e leader/region. Memoizado por mtime."""
    raw = bq.pipeline_deals_raw(force_refresh=force, pipeline=_pl())
    key = (_pl(), "pipeline_deals", _parquet_mtime("pipeline_deals_raw_v6"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["bucket"] = df["dealstage"].map(_pipeline_bucket)
    df["hs_closedate"] = pd.to_datetime(df["hs_closedate"], errors="coerce")
    df["is_open"] = df["is_open"].astype(bool)

    # Junta leader/region usando o snapshot mais recente de ae_performance.
    ae_full = canon_ae_names(bq.ae_performance(force_refresh=force))
    ae_full = apply_leader_overrides(ae_full)
    ae_full = ae_full[(ae_full["region"].notna()) & (ae_full["role"] == "AE")]
    ae_full["quarter"] = pd.to_datetime(ae_full["quarter"]).dt.date
    ae_meta = (ae_full.sort_values("quarter")
               .drop_duplicates("account_executive", keep="last")
               [["account_executive", "leader", "region"]])
    df = df.merge(ae_meta, on="account_executive", how="left")

    df = apply_roster_leader(df)
    # Pipeline = visão corrente/forward: quem já saiu (cutoff <= hoje) some.
    _gone = {ae for ae, cut in DEPARTED_FROM.items() if cut <= _today()}
    if _gone:
        df = df[~df["account_executive"].isin(_gone)]
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _get_forecast_deals(force: bool = False) -> pd.DataFrame:
    """Deals dos estágios 1-6 com close date, amount, forecast category e o
    ponderado (amount × pct da categoria). leader/region do snapshot mais recente
    de ae_performance. Memoizado por mtime do parquet."""
    raw = bq.forecast_deals_raw(force_refresh=force, pipeline=_pl())
    key = (_pl(), "forecast_deals_v2", _parquet_mtime("forecast_deals_raw_v6"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")
    df["po_emission_date"] = pd.to_datetime(df["po_emission_date"], errors="coerce")
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")  # p/ ciclo de vendas
    # Data de validação do won = data de emissão do PO; cai pro close date se vazia.
    # É ela (priorizada sobre o close date do HubSpot) que define o quarter do won.
    df["valid_date"] = df["po_emission_date"].fillna(df["close_date"])
    # Datas da RECEITA REALIZADA (extensão de fechamento aplicada, ver
    # REALIZED_QUARTER_EXTENSIONS). Colunas NOVAS de propósito: po_emission_date
    # e valid_date seguem no calendário real para contagem, ticket, ciclo e new
    # logos. Toda janela/quarter/líder do realizado lê `rev_po`/`rev_date`.
    df["rev_po"] = _rev_date(df["po_emission_date"])
    df["rev_date"] = _rev_date(df["valid_date"])
    df["forecast_category"] = df["forecast_category"].fillna("(None)")
    df["is_won"] = df["dealstage"] == "closedwon"  # PO Won = realizado
    # Ponderado só faz sentido pro pipeline aberto; closedwon não está no PCT → 0.
    # Peso EFETIVO (já com DIR_FORECAST_FACTOR) — o realizado sai de `amount`,
    # nunca daqui, então o fator não toca no Won.
    df["pct"] = df["forecast_category"].map(FORECAST_CATEGORY_PCT_EFF).fillna(0.0)
    df["weighted"] = df["amount"] * df["pct"]

    # leader/region do snapshot mais recente de ae_performance (igual pipeline).
    ae_full = canon_ae_names(bq.ae_performance(force_refresh=force))
    ae_full = apply_leader_overrides(ae_full)
    ae_full = ae_full[(ae_full["region"].notna()) & (ae_full["role"] == "AE")]
    ae_full["quarter"] = pd.to_datetime(ae_full["quarter"]).dt.date
    ae_meta = (ae_full.sort_values("quarter")
               .drop_duplicates("account_executive", keep="last")
               [["account_executive", "leader", "region"]])
    df = df.merge(ae_meta, on="account_executive", how="left")

    # Vendas diretas dos gestores: o "AE" do deal é um líder (não casa no ae_meta
    # de role='AE'). Atribui ao próprio gestor (leader = ele mesmo) e região BR,
    # pra entrar nos números dele e no total. `is_manager_sale` mantém essas
    # vendas fora das barras do gráfico (mas na tabela e nos totais).
    df = apply_roster_leader(df)            # time atual (Q3) nos deals dos AEs do roster
    mgr_mask = df["account_executive"].isin(NON_AE_USERS)
    df.loc[mgr_mask, "leader"] = df.loc[mgr_mask, "account_executive"]
    df.loc[mgr_mask, "region"] = "BR"
    df["is_manager_sale"] = mgr_mask

    # Corte de saída: won conta pela data da receita (validação, com extensão),
    # aberto pela close date. Data efetiva >= cutoff → some (mantém histórico
    # anterior). NaT fica.
    if DEPARTED_FROM:
        eff = df["rev_date"].where(df["is_won"], df["close_date"])
        eff = pd.to_datetime(eff, errors="coerce").dt.date
        drop = pd.Series(False, index=df.index)
        for ae, cutoff in DEPARTED_FROM.items():
            m = (df["account_executive"] == ae) & eff.notna() & (eff >= cutoff)
            if ae in DEPARTED_FROM_KEEP_WON:
                m &= ~df["is_won"]              # venda fechada fica (ver a constante)
            drop |= m
        df = df[~drop]
    # Venda fechada de quem está em DEPARTED_FROM_KEEP_WON é venda de gestor:
    # líder = ele mesmo (sem isso o leader vinha do snapshot cru e o deal caía
    # fora do ALLOWED_LEADERS / do pod dele).
    if DEPARTED_FROM_KEEP_WON:
        _kw = df["is_won"] & df["account_executive"].isin(DEPARTED_FROM_KEEP_WON)
        df.loc[_kw, "leader"] = df.loc[_kw, "account_executive"]
        df.loc[_kw, "region"] = "BR"
        df.loc[_kw, "is_manager_sale"] = True

    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _pipeline_by_ae_pd(deals: pd.DataFrame,
                        date_from: "date | None" = None,
                        date_to: "date | None" = None) -> pd.DataFrame:
    """Soma R$ por AE em 2 buckets (early, late) — apenas deals abertos.
    Filtro de período aplica em hs_closedate (close date do HubSpot): deals
    com closedate dentro do range entram. Sem closedate populado:
      - sem filtro setado → entra (default = todo pipeline aberto atual)
      - com filtro setado → fica fora do recorte"""
    cols = ["account_executive", "leader", "region",
            "early_amount", "late_amount", "early_deals", "late_deals",
            "no_closedate_amount", "no_closedate_deals"]
    if len(deals) == 0:
        return pd.DataFrame(columns=cols)
    d = deals[deals["is_open"] & deals["bucket"].isin(("early", "late"))]
    has_filter = date_from is not None or date_to is not None
    # Antes do filtro, conta deals sem closedate (informativo p/ KPI)
    no_cd = d[d["hs_closedate"].isna()]
    no_cd_by_ae = (no_cd.groupby("account_executive", dropna=False)
                       .agg(no_closedate_amount=("amount","sum"),
                            no_closedate_deals=("amount","count"))
                       .reset_index())
    if has_filter:
        cd = d["hs_closedate"]
        mask = cd.notna()
        if date_from is not None:
            mask &= cd >= pd.Timestamp(date_from)
        if date_to is not None:
            mask &= cd <= pd.Timestamp(date_to)
        d = d[mask]
    if len(d) == 0:
        out = no_cd_by_ae.copy()
        for c in ("early_amount","late_amount","early_deals","late_deals"):
            out[c] = 0
        out["leader"] = None; out["region"] = None
        return out[cols] if len(out) else pd.DataFrame(columns=cols)
    g = d.groupby(["account_executive", "leader", "region", "bucket"], dropna=False)
    amt = g["amount"].sum().unstack(fill_value=0.0)
    cnt = g.size().unstack(fill_value=0).rename(columns={"early": "early_deals", "late": "late_deals"})
    amt = amt.rename(columns={"early": "early_amount", "late": "late_amount"})
    out = amt.join(cnt).reset_index()
    for col in ("early_amount","late_amount","early_deals","late_deals"):
        if col not in out.columns:
            out[col] = 0
    out = out.merge(no_cd_by_ae, on="account_executive", how="left")
    out["no_closedate_amount"] = out["no_closedate_amount"].fillna(0.0)
    out["no_closedate_deals"]  = out["no_closedate_deals"].fillna(0).astype(int)
    return out[cols]


def get_filter_options(df: pd.DataFrame, f: dict = None):
    """Cascading filter options: cascade region → leader → AE.
    Líderes seguem ordem hierárquica do organograma (VP → Diretores → Gerentes).
    Selecionar um líder no AE cascade expande para subordinados (ORG_CHART)."""
    f = f or {}
    quarters = sorted(df["quarter"].dropna().unique().tolist())
    regions  = sorted(df["region"].dropna().unique().tolist())
    roles    = sorted(df["role"].dropna().unique().tolist())

    leaders_df = df[df["region"] == f["region"]] if f.get("region") else df
    data_leaders = set(leaders_df["leader"].dropna().unique().tolist())
    # Pedro entra sempre (VP virtual); diretores/gerentes só se tiverem AEs no recorte
    leaders = [l for l in LEADER_ORDER if l == "Pedro Mendes" or l in data_leaders]

    aes_df = leaders_df
    if f.get("leader"):
        aes_df = aes_df[aes_df["leader"].isin(expand_leader(f["leader"]))]
    aes = sorted(aes_df["account_executive"].dropna().unique().tolist())
    # Esconde AEs desligados do dropdown (mas mantém o histórico nos charts).
    aes = [a for a in aes if a not in HIDDEN_FROM_AE_VIEWS]
    # Geração de Demanda do México: o dropdown segue o MESMO universo da tela —
    # os 7 vendedores do organograma do Mauricio (ver o ramo MX de
    # _get_demand_deals). Sem isto o select listava os 38 nomes que o funil
    # mexicano acumulou desde 2024, e escolher qualquer um dos outros 31
    # devolvia um gráfico vazio sem dizer por quê.
    # A lista sai do MX_ROSTER, e não do que sobrou do frame: a curada
    # `account_executives_performance` ainda não tem linha pra quem entrou
    # agora (a Sabina Diaz é o caso de hoje), e intersectar deixaria de fora
    # justamente uma vendedora que APARECE no gráfico — 63 opps no Q3.
    if _is_mx() and _rota_base(request.path) == "/demand":
        aes = sorted(MX_ROSTER)
        if f.get("ae") and f["ae"] not in aes:
            aes = sorted(aes + [f["ae"]])

    if f.get("leader") and f["leader"] not in leaders:
        leaders.append(f["leader"])
    if f.get("ae") and f["ae"] not in aes:
        # Se o usuário explicitamente selecionou um AE (mesmo desligado), mantém visível.
        aes = sorted(aes + [f["ae"]])

    # Labels com cargo pra dropdown ficar legível.
    leader_labels = {l: f"{l} — {LEADER_ROLE.get(l, '')}" if LEADER_ROLE.get(l) else l for l in leaders}

    return {
        "quarters": quarters,
        "regions":  regions,
        "leaders":  leaders,
        "leader_labels": leader_labels,
        "aes":      aes,
        "roles":    roles,
    }


# Abas cujo filtro de PERÍODO opera em trimestres (não em datas).
_QUARTER_PERIOD_PATHS = {"/overview/overtime", "/leaders/overview/overtime"}


def _period_widget_ctx() -> dict:
    """Contexto do filtro de período conforme a aba. Em modo 'quarter' devolve os
    presets de N trimestres, a lista de trimestres disponíveis (pro intervalo
    aberto) e o rótulo do que está selecionado."""
    if request.path not in _QUARTER_PERIOD_PATHS:
        return {"period_mode": "date"}
    cur_q = _quarter_start_of(_today())
    opts = [{"key": _quarter_key(q), "label": _quarter_label(q)}
            for q in _quarter_range(_OVERTIME_MIN_Q, cur_q)]
    q_from, q_to = request.args.get("q_from", ""), request.args.get("q_to", "")
    n_sel = (request.args.get("quarters") or "").strip()
    if q_from or q_to:
        _f = _parse_quarter(q_from); _t = _parse_quarter(q_to)
        label = f"{_quarter_label(_f) if _f else '…'} → {_quarter_label(_t) if _t else 'hoje'}"
    else:
        n_sel = n_sel or "4"
        label = dict(_OVERTIME_QUARTER_OPTS).get(n_sel, "Últimos 4 trimestres")
    base = {k: v for k, v in request.args.items()
            if k not in ("quarters", "q_from", "q_to", "refresh") and v}
    return {
        "period_mode": "quarter",
        "q_opts": opts,
        "q_from": q_from, "q_to": q_to,
        "q_sel": n_sel,
        "q_label": label,
        "q_urls": {v: request.path + "?" + urllib.parse.urlencode({**base, "quarters": v})
                   for v, _ in _OVERTIME_QUARTER_OPTS},
        "q_presets": _OVERTIME_QUARTER_OPTS,
    }


def build_active_chips(filters: dict, current_path: str) -> list[dict]:
    """Chips for each non-empty filter, with a URL to remove that filter only."""
    chips = []
    labels = {
        "date_from": "De", "date_to": "Até", "region": "Região",
        "leader":    "Líder", "ae": "AE",    "role": "Role",
        "dtype": "Deal Type", "source": "Source", "fcat": "Forecast",
        "prod": "Product",
    }
    # Nas telas de janela fixa (PoP Early/Late) as datas são IGNORADAS, então um
    # chip "De: …" vindo de link antigo/salvo mentiria sobre o que está na tela.
    _skip = ({"date_from", "date_to"}
             if _rota_base(current_path) in WOW_PATHS + WOW_HIST_PATHS
             + PIPELINE_DOD_PATHS else set())
    # Mesmo motivo, outro filtro: os links das sub-abas viajam com `**ctx.filters`,
    # então escolher "Aquisição" no Forecast levava o chip junto pro Histórico e
    # pro Modelo Analytics — telas que ignoram o corte e mostram o time inteiro.
    if _rota_base(current_path) in FORECAST_NO_DTYPE_PATHS:
        _skip = _skip | {"dtype"}
    for k, v in filters.items():
        if not v or k not in labels or k in _skip: continue  # 'money' não vira chip (é o toggle mestre)
        remaining = {kk: vv for kk, vv in filters.items() if vv and kk != k}
        qs = urllib.parse.urlencode(remaining)
        url = current_path + ("?" + qs if qs else "")
        # Os cortes do Pipeline Total viajam na URL como chave curta (dtype=acq);
        # o chip mostra o rótulo de negócio ("Aquisição"), não a chave.
        shown = v
        if k in _PT_DIMS:
            shown = _pt_dim_label(k, v) or v
        chips.append({"label": f"{labels[k]}: {shown}", "remove_url": url})
    return chips


def _extra_filters_for_path(path: str) -> list:
    """Selects extra da barra de filtros, POR ABA — filtro é por aba, não global
    (Deal Type não significa nada no Overview, então lá os campos nem existem).

    Pipeline Total pede as 4 dimensões; o Funil II só Deal Type, porque expansão
    converte numa ordem de grandeza diferente de aquisição e misturar as duas na
    mesma taxa esconde isso. Fora dessas abas, lista vazia."""
    if _rota_base(path) in PIPELINE_TOTAL_PATHS:
        return [{"key": k, "label": v["label"], "opts": _pt_dim_meta(k)}
                for k, v in _PT_DIMS.items()]
    # Funil II e WoW pedem só Deal Type. No WoW o motivo é o mesmo do Funil II:
    # aquisição e expansão se movimentam em ritmos diferentes, e a série somada
    # esconde isso — expansão entra e fecha rápido, aquisição fica semanas no
    # funil, então uma semana ruim de uma pode aparecer como semana boa do total.
    # Pipeline do MX usa o registro MX_DIMS (valores próprios do México).
    if path == "/mx/pipeline":
        return [{"key": k, "label": v["label"], "opts": _mx_dim_meta(k)}
                for k, v in MX_DIMS.items()]
    if (_rota_base(path) in FUNNEL2_PATHS + WOW_PATHS + WOW_HIST_PATHS
            + FORECAST_WOW_PATHS + FORECAST_PATHS + PIPELINE_DOD_PATHS):
        return [{"key": "dtype", "label": _PT_DIMS["dtype"]["label"],
                 "opts": _pt_dim_meta("dtype")}]
    return []


def base_context():
    """Common context for every page: full df, filters, options, presets."""
    force = request.args.get("refresh") == "1"
    df_all = _get_clean_ae_perf(force=force)

    f = get_filters()
    # `opts` não olha data (só region/leader), então pode vir antes do pin abaixo.
    opts = get_filter_options(df_all, f)
    presets = filter_presets_for_path(build_presets(opts["quarters"]), request.path)
    period_label = PERIOD_LABEL_BY_PATH.get(_rota_base(request.path), "Período")
    # No Funil II o que o período recorta é escolhido no chip da página, então o
    # rótulo do filtro segue o chip em vez de ser fixo por rota.
    if _rota_base(request.path) in FUNNEL2_PATHS and request.args.get("base") == "created":
        period_label = "Criação do deal"
    active_preset = request.args.get("preset", "")

    # Cobertura abre travada no trimestre corrente em vez de "todo o pipe".
    # Herda as datas do próprio preset current_q pra UI, dados e drill-downs não
    # divergirem. Escolher outro preset manda date_from/date_to e desliga o pin.
    # Completa ponta a ponta: sem isso, remover só um dos chips De:/Até: deixaria
    # a janela aberta pra trás numa página que só olha pra frente.
    # WoW abre pré-fixado no último domingo vs o anterior. Só marca o preset — as
    # datas seguem vazias de propósito, pra `_wow_window` continuar derivando os
    # domingos de hoje (e não congelar num par de datas na URL).
    if _rota_base(request.path) in WOW_PATHS and not f["date_from"] and not f["date_to"]:
        active_preset = active_preset or "wow_1w"

    if _rota_base(request.path) in COVERAGE_PATHS and not (f["date_from"] and f["date_to"]):
        _cur_q = next((p for p in presets if p["key"] == "current_q"), None)
        if _cur_q:
            if not f["date_from"] and not f["date_to"]:
                f["date_from"], f["date_to"] = _cur_q["from"], _cur_q["to"]
                active_preset = active_preset or "current_q"
            else:
                # Só uma ponta: completa pelo trimestre DELA (não pelo corrente,
                # senão uma ponta futura geraria janela invertida/vazia).
                try:
                    anchor = pd.to_datetime(f["date_from"] or f["date_to"]).date()
                    f["date_from"] = f["date_from"] or _quarter_start_of(anchor).isoformat()
                    f["date_to"]   = f["date_to"]   or _quarter_end_of(anchor).isoformat()
                except (ValueError, TypeError):
                    f["date_from"], f["date_to"] = _cur_q["from"], _cur_q["to"]

    df = apply_filters(df_all, f)

    # Build querystring for refresh link (preserve filters minus refresh flag)
    qs_params = {k: v for k, v in request.args.items() if k != "refresh" and v}
    querystring = urllib.parse.urlencode(qs_params)

    # View detection: 3 níveis de dashboard (AE / Leaders / SDR).
    if request.path.startswith("/leaders/"):
        view = "leaders"
    elif request.path.startswith("/sdr/"):
        view = "sdr"
    elif request.path.startswith("/mx/"):
        view = "mx"
    else:
        view = "ae"

    # URLs do top-level switch (preserva filtros e a TELA atual entre AE↔Leaders).
    # Leaders tem paridade: overview/demand/pipeline/forecast. SDR só `effort`.
    LEADER_TABS = {"demand", "overview", "pipeline", "forecast"}
    SDR_TABS = {"effort"}
    base_args = {k: v for k, v in request.args.items() if k != "refresh" and v}
    base_qs = ("?" + urllib.parse.urlencode(base_args)) if base_args else ""
    if view == "leaders":
        # 1º segmento após /leaders/ (ex.: /leaders/pipeline/funnel → 'pipeline')
        cur_tab = request.path.replace("/leaders/", "", 1).split("/")[0]
        view_url_ae      = "/" + cur_tab + base_qs
        view_url_leaders = request.path + base_qs
        view_url_sdr     = "/sdr/effort" + base_qs
    elif view == "sdr":
        view_url_ae      = "/demand" + base_qs
        view_url_leaders = "/leaders/demand" + base_qs
        view_url_sdr     = request.path + base_qs
    elif view == "mx":
        # Paridade de telas desde 12/10/2026: voltar pro BR cai na MESMA tela
        # quando ela existe no AE (o DoD/WoW do México vêm do Leaders do BR, aí
        # o AE cai na raiz da aba). Senão, no Overview.
        _br = _MX_PATH_TO_BR.get(request.path, "")
        _tab = request.path[len("/mx/"):].split("/")[0]
        if _br and not _br.startswith("/leaders/"):
            view_url_ae = _br + base_qs
        else:
            view_url_ae = "/" + (_tab if _tab in LEADER_TABS else "overview") + base_qs
        view_url_leaders = "/leaders/" + (_tab if _tab in LEADER_TABS else "overview") + base_qs
        view_url_sdr     = "/sdr/effort" + base_qs
    else:
        # 1º segmento do path (ex.: /overview/ae → 'overview', /pipeline/funnel → 'pipeline')
        cur_tab = (request.path.lstrip("/").split("/")[0]) or "demand"
        view_url_ae      = request.path + base_qs
        target_leader    = cur_tab if cur_tab in LEADER_TABS else "demand"
        view_url_leaders = "/leaders/" + target_leader + base_qs
        view_url_sdr     = "/sdr/effort" + base_qs

    return {
        "df": df,
        "df_all": df_all,
        "filters": f,
        "options": opts,
        "presets": presets,
        "period_label": period_label,
        "active_preset": active_preset,
        "active_chips": build_active_chips(f, request.path),
        "querystring": querystring,
        "current_path": request.path,
        # PoP Early/Late não têm filtro de período: a janela é fixa em domingo vs
        # domingo anterior, pra as 2 telas serem sempre comparáveis entre si.
        # base.html esconde o date-picker quando isto é True. Ver _wow_fixed_pair.
        "period_locked": _rota_base(request.path) in (WOW_PATHS + WOW_HIST_PATHS
                                                      + PIPELINE_DOD_PATHS),
        "extra_filters": _extra_filters_for_path(request.path),
        "period_text": _format_period(f, presets, active_preset),
        "view": view,
        "view_url_ae": view_url_ae,
        "view_url_leaders": view_url_leaders,
        "view_url_sdr": view_url_sdr,
        "view_url_mx": _mx_equivalente(request.path) + base_qs,
        # Nome do funil pra texto de tela ("Sales BR" / "Sales MX").
        "pipeline_label": _pl(),
        # Símbolo da moeda ativa pra rótulo fixo de template ("R$" / "US$").
        "money_sym": "US$" if _money_mode() == "arr" else "R$",
        "leader_tabs": LEADER_TABS,
        "sdr_tabs": SDR_TABS,
        "usd_rate": USD_BRL_RATE,
        "months_per_year": MONTHS_PER_YEAR,
        # Selo de frescor: de QUANDO é o dado que está na tela. Ver
        # `source_freshness` — idade da fonte, nunca idade do cache.
        "freshness": source_freshness(),
        # Filtro de PERÍODO mutável por aba: 'quarter' (Sales Overtime) troca o
        # date-range por presets de N trimestres + intervalo aberto quarter→quarter.
        **_period_widget_ctx(),
        # Chama _money_mode() em vez de reler a querystring: a regra do México
        # ("sempre ARR US$") mora lá, e a cópia daqui não a tinha. O efeito era
        # o pior tipo de erro — o MESMO card em duas moedas: o subtítulo do drill
        # vinha do servidor em US$ ARR e as linhas, formatadas pelo JS com este
        # valor, saíam em R$ MRR. Doze vezes de diferença dentro do mesmo modal.
        "money_mode": _money_mode(),
        "money_url_mrr": request.path + "?" + urllib.parse.urlencode(
            {**{k: v for k, v in request.args.items() if k not in ("money", "refresh") and v}, "money": "mrr"}),
        "money_url_arr": request.path + "?" + urllib.parse.urlencode(
            {**{k: v for k, v in request.args.items() if k not in ("money", "refresh") and v}, "money": "arr"}),
    }


# -------- routes --------

@app.route("/")
def home():
    # Default landing = Overview (visão consolidada de vendas).
    return redirect(url_for("overview", **request.args))


@app.route("/overview")
def overview():
    """Sub-aba 'Sales Overview' — dados macro do time (receita, new logos, aq×exp)."""
    return _overview_page("sales")


@app.route("/overview/ae")
def overview_ae():
    """Sub-aba 'AE Overview' — ranking de receita e new logos por vendedor + drill."""
    return _overview_page("ae")


@app.route("/leaders/overview")
def leaders_overview():
    """Sales Leaders · Sales Overview — dados macro do time (idêntico ao AE)."""
    return _overview_page("sales", grain="leader")


@app.route("/leaders/overview/team")
def leaders_overview_team():
    """Sales Leaders · Leaders Overview — ranking por líder (soma do time) + drill."""
    return _overview_page("ae", grain="leader")


def _funnel_drill_scope():
    """Recorte compartilhado dos drills de conversão (tabela de estágios e a
    lista de deals de um estágio): aplica trimestre clicado + rolling + tipo de
    deal + AE/líder + coorte de criação. None se o trimestre for inválido."""
    q = _parse_quarter(request.args.get("q"))
    if not q:
        return None
    q_end = _quarter_end_of(q)
    piv = _get_funnel_entries()
    ctype = (request.args.get("conv_type") or "").strip()
    if ctype == "acq":
        piv = piv[piv["deal_type"] == "Aquisition"]
    elif ctype == "exp":
        piv = piv[piv["deal_type"] == "Up & cross sell"]
    ae, leader = request.args.get("ae"), request.args.get("leader")
    if ae:
        piv = piv[piv["account_executive"] == ae]
    elif leader:
        _aes = {a for a, ld in _current_leader_by_ae().items() if ld in expand_leader(leader)}
        piv = piv[piv["account_executive"].isin(_aes)]
    # Mesma coorte de criação do gráfico (quando ativa).
    cq_from, cq_to = _parse_quarter(request.args.get("cq_from")), _parse_quarter(request.args.get("cq_to"))
    if (cq_from or cq_to) and len(piv):
        cd = pd.to_datetime(piv["created_date"], errors="coerce")
        if cq_from:
            piv = piv[cd >= pd.Timestamp(cq_from)]
        if cq_to:
            piv = piv[cd <= pd.Timestamp(_quarter_end_of(cq_to))]

    # Janela ROLLING: o trimestre clicado + os `roll` anteriores (mesma coorte
    # acumulada do gráfico). roll=0 → só o trimestre.
    roll = min(max(_safe_int(request.args.get("roll"), 0), 0), 16)
    lo_q = q
    for _ in range(roll):
        lo_q = _quarter_start_of(lo_q - timedelta(days=1))
    lo, hi = pd.Timestamp(lo_q, tz="UTC"), pd.Timestamp(q_end, tz="UTC") + pd.Timedelta(days=1)
    win_label = f"{_quarter_label(lo_q)} → {_quarter_label(q)}" if roll else _quarter_label(q)
    return piv, lo, hi, win_label, ctype


def _funnel_stage_mask(piv, stage, lo, hi):
    """Máscara de deals do drill p/ um `stage`: 1..7 = ativos na janela (mesma
    régua do gráfico), 99 = lost na janela, 'all' = união (deals analisados)."""
    if stage == "all":
        m = pd.Series(False, index=piv.index)
        for num, _sid, _lbl in _CONV_STAGES:
            m |= _stage_active_mask(piv, num, lo, hi)
        if "s99" in piv.columns:
            m |= ((piv["s99"] >= lo) & (piv["s99"] < hi)).fillna(False)
        return m
    num = _safe_int(stage, 0)
    if num == 99:
        if "s99" not in piv.columns:
            return pd.Series(False, index=piv.index)
        return ((piv["s99"] >= lo) & (piv["s99"] < hi)).fillna(False)
    return _stage_active_mask(piv, num, lo, hi)


@app.route("/overview/funnel-stages")
def overview_funnel_stages():
    """Drill do gráfico de conversão: quantos deals PASSARAM por cada estágio
    no trimestre clicado (1ª entrada no estágio dentro do trimestre). Cada
    número é clicável → lista os deals daquele estágio (2º nível do drill)."""
    scope = _funnel_drill_scope()
    if scope is None:
        return jsonify(_empty_drill(""))
    piv, lo, hi, win_label, ctype = scope

    # URL do 2º nível: mesmos filtros da tabela + o estágio clicado.
    _carry = {k: v for k, v in request.args.items() if v and k != "money"}
    def _stage_url(stage):
        return url_for("overview_funnel_stage_deals", **{**_carry, "stage": str(stage)})

    # ACUMULADO no funil (snapshot/coorte): um deal conta no estágio N se estava
    # ATIVO nele em algum momento do trimestre — entrou até o fim do trimestre e
    # só saiu (foi pra outro estágio / won / lost) depois do início dele. Não é
    # só quem entrou ali: o funil arrasta entre quarters. Saída ≈ o menor
    # timestamp de outra 1ª entrada posterior; sem saída = ainda está no estágio.
    rows = []
    any_mask = pd.Series(False, index=piv.index)   # p/ o total de deals analisados
    for num, _sid, label in _CONV_STAGES:
        active = _stage_active_mask(piv, num, lo, hi)   # mesma régua do gráfico
        any_mask |= active
        lbl = "Won (na janela)" if num == 7 else label
        rows.append({"estagio": lbl, "deals": int(active.sum()), "drill": _stage_url(num)})
    lost = 0
    if "s99" in piv.columns:
        c = piv["s99"]
        m = (c >= lo) & (c < hi)
        any_mask |= m.fillna(False)
        lost = int(m.sum())
    rows.append({"estagio": "Lost (na janela)", "deals": lost, "drill": _stage_url(99)})
    # Total = deals DISTINTOS analisados na janela (um deal que passa por vários
    # estágios conta 1x aqui — as linhas acima somam mais que isso por natureza).
    rows.append({"estagio": "Total de deals analisados", "deals": int(any_mask.sum()),
                 "drill": _stage_url("all")})
    sub = {"acq": " · só aquisição", "exp": " · só expansão"}.get(ctype, "")
    return jsonify({
        "title": f"Deals por estágio · {win_label}",
        "subtitle": f"ativos em cada estágio na janela — acumulado, inclui quem entrou antes{sub} · clique num número pra ver os deals",
        "columns": [{"key": "estagio", "label": "Estágio"},
                    {"key": "deals", "label": "Deals", "type": "int", "drill": "drill"}],
        "rows": rows,
    })


@app.route("/overview/funnel-stages/deals")
def overview_funnel_stage_deals():
    """2º nível do drill de conversão: os deals que compõem o número de um
    estágio (mesma máscara da tabela) — nome com link do HubSpot, tipo e MRR."""
    scope = _funnel_drill_scope()
    if scope is None:
        return jsonify(_empty_drill(""))
    piv, lo, hi, win_label, ctype = scope
    stage = (request.args.get("stage") or "all").strip()
    sel = piv[_funnel_stage_mask(piv, stage, lo, hi)]

    _stage_lbl = {str(num): ("Won (na janela)" if num == 7 else lbl)
                  for num, _sid, lbl in _CONV_STAGES}
    _stage_lbl["99"] = "Lost (na janela)"
    _stage_lbl["all"] = "Total de deals analisados"
    _tipo = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}

    rows = []
    for _, r in sel.iterrows():
        rows.append({
            "deal_name": r.get("deal_name") or "—",
            "deal_url": _hs_deal_url(r.get("deal_id")),
            "ae": r.get("account_executive") or "—",
            "tipo": _tipo.get(r.get("deal_type"), r.get("deal_type") or "—"),
            "amount": float(r.get("amount") or 0),
        })
    rows.sort(key=lambda x: x["amount"], reverse=True)
    sub = {"acq": " · só aquisição", "exp": " · só expansão"}.get(ctype, "")
    return jsonify({
        "title": f"{_stage_lbl.get(stage, 'Deals')} · {win_label}",
        "subtitle": _drill_money_subtitle(len(rows), float(sum(x['amount'] for x in rows))) + sub,
        "columns": [
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "ae",        "label": "AE"},
            {"key": "tipo",      "label": "Tipo"},
            {"key": "amount",    "label": "MRR", "type": "money"},
        ],
        "rows": rows,
    })


@app.route("/overview/overtime/unit-deals")
def overview_unit_price_deals():
    """Drill do Unit Price (Sales Overtime): eventos de venda VALIDADA de Smart
    Trac no período — registro de venda linha a linha (MRR do sensor ÷ sensores),
    não os deals do HubSpot. 'First PO' = venda nova; Upsell/Cross sell = expansão."""
    upe = _get_unit_price_events()
    ae, leader = request.args.get("ae"), request.args.get("leader")
    if ae:
        upe = upe[upe["account_executive"] == ae]
    elif leader:
        _aes = {a for a, ld in _current_leader_by_ae().items() if ld in expand_leader(leader)}
        upe = upe[upe["account_executive"].isin(_aes)]
    def _pd_date(s):
        try:
            return pd.to_datetime(s).date() if s else None
        except (ValueError, TypeError):
            return None
    d_from = _pd_date(request.args.get("date_from"))
    d_to = _pd_date(request.args.get("date_to"))
    if d_from:
        upe = upe[upe["event_date"] >= d_from]
    if d_to:
        upe = upe[upe["event_date"] <= d_to]

    _tipo = {"First PO": "Venda nova", "Upsell": "Expansão", "Cross sell": "Expansão"}
    rows = []
    for _, r in upe.iterrows():
        dev = int(r["devices"] or 0)
        amt = float(r["amount_brl"] or 0)
        rows.append({
            "deal_name": r.get("deal_name") or "—",
            "deal_url": _hs_deal_url(r.get("deal_id")),
            "ae": r.get("account_executive") or "—",
            "tipo": _tipo.get(r.get("deal_sale_type"), r.get("deal_sale_type")),
            "sensores": dev,
            "unit": (amt / dev) if dev else 0.0,
            "amount": amt,
            "data": r["event_date"].isoformat() if r.get("event_date") else "",
        })
    rows.sort(key=lambda x: x["sensores"], reverse=True)
    tot_s = sum(x["sensores"] for x in rows)
    tot_a = sum(x["amount"] for x in rows)
    _n = [x for x in rows if x["tipo"] == "Venda nova"]
    _e = [x for x in rows if x["tipo"] == "Expansão"]
    _u = lambda xs: (fmt_money(sum(x["amount"] for x in xs) / sum(x["sensores"] for x in xs))
                     if sum(x["sensores"] for x in xs) else "—")
    return jsonify({
        "title": "Unit price · Smart Trac (venda validada)",
        "subtitle": (f"{len(rows)} vendas · {tot_s} sensores · "
                     f"nova {_u(_n)} ({len(_n)}) · expansão {_u(_e)} ({len(_e)}) · "
                     f"blended {fmt_money((tot_a / tot_s) if tot_s else 0)}"),
        "columns": [
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "ae",        "label": "AE"},
            {"key": "tipo",      "label": "Tipo"},
            {"key": "sensores",  "label": "Sensores",       "type": "int"},
            {"key": "unit",      "label": "Preço / sensor", "type": "money"},
            {"key": "amount",    "label": "MRR Smart Trac", "type": "money"},
            {"key": "data",      "label": "PO em",          "type": "date"},
        ],
        "rows": rows,
    })


@app.route("/overview/overtime")
def overview_overtime():
    return _sales_overtime_page("ae")


@app.route("/leaders/overview/overtime")
def leaders_overview_overtime():
    return _sales_overtime_page("leader")


# Presets rápidos do filtro de período no modo TRIMESTRE (Sales Overtime).
_OVERTIME_QUARTER_OPTS = [("4", "Últimos 4 trimestres"), ("8", "Últimos 8 trimestres"),
                          ("12", "Últimos 12 trimestres"), ("16", "Últimos 16 trimestres"),
                          ("all", "Tudo")]
_OVERTIME_MIN_Q = date(2022, 1, 1)


def _parse_quarter(s):
    """'2026-Q3' | '2026-07-01' → date do início do quarter. None se inválido."""
    if not s:
        return None
    s = str(s).strip().upper()
    m = re.match(r"^(\d{4})-?Q([1-4])$", s)
    if m:
        return date(int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1, 1)
    try:
        return _quarter_start_of(pd.to_datetime(s).date())
    except (ValueError, TypeError):
        return None


def _quarter_key(d: date) -> str:
    """date → '2026-Q3' (valor usado no filtro de período trimestral)."""
    return f"{d.year}-Q{(d.month - 1) // 3 + 1}"


def _quarter_range(start: date, end: date) -> list:
    """Lista de quarter-starts de `start` até `end` (inclusive)."""
    out, q = [], _quarter_start_of(start)
    last = _quarter_start_of(end)
    while q <= last:
        out.append(q)
        q = _next_quarter(q)
    return out


def _sales_overtime_page(grain="ae"):
    """Sales Overtime — receita trimestral: Quota × Realizado (barras) + linha de
    % de atingimento, ao longo do tempo. Cada barra é um QUARTER (ordem
    cronológica), não um vendedor.

    Janela: o filtro de PERÍODO do topo muda de modo nesta aba — vira trimestral
    (presets de N trimestres via `quarters`, ou intervalo aberto `q_from`/`q_to`
    de quarter a quarter). Líder/AE/região continuam valendo.

    VALOR GERAL = venda de QUALQUER pessoa da Tractian no funil Sales BR: sem
    recorte de roster/líderes permitidos e SEM excluir quem já saiu (reps entram
    e saem ao longo do tempo; a venda feita à época continua contando). Só quando
    o usuário filtra líder/AE é que o recorte entra.

    Quota = meta oficial do BQ (sales_rep_monthly_quota), somando todos os reps
    que carregavam meta naquele quarter. New logos: realizado = deals de
    Aquisição ganhos; meta = new_logos_quota do mesmo BQ."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # ---- Quota trimestral (BQ, meta oficial do time) ----
    qh = _get_quota_history(force=force)
    if f["leader"]:
        qh = qh[qh["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        qh = qh[qh["account_executive"] == f["ae"]]

    # ---- Realizado por quarter: TODO mundo (inclui gestores e quem já saiu) ----
    deals = _get_forecast_deals(force=force)
    dd = deals.copy()
    if f["region"]:
        dd = dd[dd["region"] == f["region"]]
    if f["ae"]:
        dd = dd[dd["account_executive"] == f["ae"]]
    won = dd[dd["is_won"] & dd["po_emission_date"].notna()].copy()
    # Dois recortes do MESMO conjunto de won: `won_rev` decide o quarter (e o
    # líder) pela data da RECEITA (extensão de fechamento aplicada) e só alimenta
    # o Realizado; `won` segue o calendário do PO para deals, new logos e ticket.
    won_rev = won.copy()
    if len(won) and f["leader"]:
        # Só recorta por líder quando pedido — usando o líder do quarter do PO.
        lmap_ot = _period_leader_map(force=force)
        won["leader"] = _attr_deal_leader(won, "po_emission_date", lmap_ot)
        won = won[won["leader"].isin(expand_leader(f["leader"]))]
        won_rev["leader"] = _attr_deal_leader(won_rev, "rev_po", lmap_ot)
        won_rev = won_rev[won_rev["leader"].isin(expand_leader(f["leader"]))]
    if len(won):
        won["q"] = won["po_emission_date"].dt.date.map(_quarter_start_of)
        won["is_acq"] = won["deal_type"] == "Aquisition"
        won["is_exp"] = won["deal_type"] == "Up & cross sell"
    if len(won_rev):
        won_rev["q"] = won_rev["rev_po"].dt.date.map(_quarter_start_of)

    # ---- Janela de quarters ----
    cur_q = _quarter_start_of(_today())
    universe = sorted({q for q in (set(qh["quarter"]) | (set(won["q"]) if len(won) else set())
                                   | (set(won_rev["q"]) if len(won_rev) else set()))
                       if q and _OVERTIME_MIN_Q <= q <= cur_q})
    q_from = _parse_quarter(request.args.get("q_from"))
    q_to = _parse_quarter(request.args.get("q_to"))
    n_arg = (request.args.get("quarters") or "").strip()
    if q_from or q_to:                      # intervalo aberto quarter→quarter
        lo_q = q_from or (universe[0] if universe else cur_q)
        hi_q = min(q_to or cur_q, cur_q)
        qs = [q for q in universe if lo_q <= q <= hi_q]
        n_arg = ""
    else:
        n_arg = n_arg or "4"
        n_q = None if n_arg == "all" else (int(n_arg) if n_arg.isdigit() and int(n_arg) > 0 else 4)
        qs = universe[-n_q:] if n_q else universe

    quota_by_q = qh.groupby("quarter")["quota_in_brl"].sum().to_dict() if len(qh) else {}
    nlq_by_q = qh.groupby("quarter")["new_logos_quota"].sum().to_dict() if len(qh) else {}
    real_by_q = won_rev.groupby("q")["amount"].sum().to_dict() if len(won_rev) else {}
    deals_by_q = won.groupby("q").size().to_dict() if len(won) else {}
    nl_by_q = won[won["is_acq"]].groupby("q").size().to_dict() if len(won) else {}

    # Ticket médio por quarter, separado por tipo de deal.
    def _ticket(mask):
        if not len(won):
            return {}
        g = won[mask].groupby("q")["amount"]
        return (g.sum() / g.size().replace(0, pd.NA)).dropna().to_dict()
    tk_acq = _ticket(won["is_acq"]) if len(won) else {}
    tk_exp = _ticket(won["is_exp"]) if len(won) else {}

    # Unit price = MRR do sensor ÷ sensores, na VENDA VALIDADA (registro de
    # venda do ops, via bq.unit_price_events) — não no deal do HubSpot, cujos
    # expected_devices foram abandonados e subcontavam. Por ser por PRODUTO,
    # deals mistos entram sem distorção (só a linha de Smart Trac conta).
    # SEPARADO por tipo: venda NOVA (First PO) x EXPANSÃO (Upsell/Cross sell).
    upe = _get_unit_price_events(force=force)
    if f["ae"]:
        upe = upe[upe["account_executive"] == f["ae"]]
    elif f["leader"]:
        _up_aes = {a for a, ld in _current_leader_by_ae().items()
                   if ld in expand_leader(f["leader"])}
        upe = upe[upe["account_executive"].isin(_up_aes)]
    if len(upe):
        upe = upe.copy()
        upe["q"] = upe["event_date"].map(_quarter_start_of)
        upe["is_new"] = upe["deal_sale_type"] == "First PO"

        def _unit(mask):
            g = upe[mask].groupby("q").agg(amt=("amount_brl", "sum"), n=("devices", "sum"))
            return (g["amt"] / g["n"].replace(0, pd.NA)).dropna().to_dict()
        unit_new_by_q = _unit(upe["is_new"])
        unit_exp_by_q = _unit(~upe["is_new"])
        st_sensors_by_q = upe.groupby("q")["devices"].sum().to_dict()
    else:
        unit_new_by_q, unit_exp_by_q, st_sensors_by_q = {}, {}, {}

    def _drill_qs(q, kind=None, q_hi=None):
        # scope=all → o drill usa a MESMA régua das barras (todo mundo, sem
        # recorte de roster/hold). q_hi permite janela de vários trimestres
        # (cards acumulados). Sem ele, a janela é o próprio quarter.
        d = {"date_from": q.isoformat(),
             "date_to": _quarter_end_of(q_hi or q).isoformat(),
             "grain": grain, "scope": "all"}
        if kind:
            d["kind"] = kind
        if f["leader"]:
            d["leader"] = f["leader"]
        if f["region"]:
            d["region"] = f["region"]
        if f["ae"]:
            # Filtro de AE como filtro (`ae`), e o nome só no grão de AE: na tela
            # de Leaders `name=<AE>` com grain=leader procurava um LÍDER com o
            # nome do vendedor e a explosão voltava vazia.
            d["ae"] = f["ae"]
            if grain == "ae":
                d["name"] = f["ae"]
        return url_for("overview_deals", **d)

    items, items_nl, items_tk, items_up = [], [], [], []
    for q in qs:
        lbl = _quarter_label(q)
        realized, quota = float(real_by_q.get(q, 0.0)), float(quota_by_q.get(q, 0.0))
        items.append({"name": lbl, "short": lbl, "leader": "",
                      "realized": realized, "quota": quota,
                      "attainment": (realized / quota) if quota else None,
                      "deals": int(deals_by_q.get(q, 0)),
                      "modal_url": _drill_qs(q)})
        nl_real, nl_goal = int(nl_by_q.get(q, 0)), int(nlq_by_q.get(q, 0))
        items_nl.append({"name": lbl, "short": lbl, "leader": "",
                         "realized": nl_real, "quota": nl_goal,
                         "attainment": (nl_real / nl_goal) if nl_goal else None,
                         "modal_url": _drill_qs(q, kind="acq")})
        # Ticket médio: barra 1 = aquisição, barra 2 = expansão (sem linha).
        items_tk.append({"name": lbl, "short": lbl, "leader": "",
                         "realized": float(tk_acq.get(q, 0.0)),
                         "quota": float(tk_exp.get(q, 0.0)),
                         "attainment": None, "modal_url": _drill_qs(q, kind="ticket")})
        # Unit price: barra 1 = venda nova, barra 2 = expansão. Drill próprio —
        # a fonte é o registro de venda (unit_price_events), não os deals won.
        _up_qs = {"date_from": q.isoformat(), "date_to": _quarter_end_of(q).isoformat()}
        if f["leader"]:
            _up_qs["leader"] = f["leader"]
        if f["ae"]:
            _up_qs["ae"] = f["ae"]
        items_up.append({"name": lbl, "short": lbl, "leader": "",
                         "realized": float(unit_new_by_q.get(q, 0.0)),
                         "quota": float(unit_exp_by_q.get(q, 0.0)),
                         "attainment": None,
                         "sensors": int(st_sensors_by_q.get(q, 0)),
                         "modal_url": url_for("overview_unit_price_deals", **_up_qs)})

    rk = _build_rank_chart(items, {}, axis_mode="currency",
                           drill_path=_mx_path("/overview/overtime")) if items else None
    rk_nl = (_build_rank_chart(items_nl, {}, axis_mode="count",
                               drill_path=_mx_path("/overview/overtime"))
             if any(i["realized"] or i["quota"] for i in items_nl) else None)
    rk_tk = (_build_rank_chart(items_tk, {}, axis_mode="currency", show_line=False,
                               drill_path=_mx_path("/overview/overtime"))
             if any(i["realized"] or i["quota"] for i in items_tk) else None)
    rk_up = (_build_rank_chart(items_up, {}, axis_mode="currency", show_line=False,
                               drill_path=_mx_path("/overview/overtime"))
             if any(i["realized"] or i["quota"] for i in items_up) else None)

    tot_real = sum(i["realized"] for i in items)
    tot_quota = sum(i["quota"] for i in items)
    tot_nl = sum(i["realized"] for i in items_nl)
    tot_nlq = sum(i["quota"] for i in items_nl)
    # Drill dos cards ACUMULADOS: janela = do 1º ao último quarter selecionado.
    drill_real_tot = _drill_qs(qs[0], q_hi=qs[-1]) if qs else None
    drill_nl_tot = _drill_qs(qs[0], kind="acq", q_hi=qs[-1]) if qs else None
    # ---- Conversão de funil ao longo do tempo (4 linhas configuráveis) ----
    piv = _get_funnel_entries(force=force)
    if f["ae"]:
        piv = piv[piv["account_executive"] == f["ae"]]
    elif f["leader"]:
        _scope_aes = {ae for ae, ld in _current_leader_by_ae().items()
                      if ld in expand_leader(f["leader"])}
        piv = piv[piv["account_executive"].isin(_scope_aes)]
    # Filtro de tipo do deal — só deste gráfico, pra comparar a conversão de
    # aquisição vs expansão (perfis de funil bem diferentes).
    conv_type = (request.args.get("conv_type") or "").strip()
    if conv_type == "acq":
        piv = piv[piv["deal_type"] == "Aquisition"]
    elif conv_type == "exp":
        piv = piv[piv["deal_type"] == "Up & cross sell"]

    # ---- Coorte ROLLING (só do gráfico de conversão): cada ponto acumula o
    # trimestre vigente + os últimos X (X selecionável). Substitui o antigo
    # filtro fixo de data de criação — o funil arrasta entre quarters.
    conv_roll = min(max(_safe_int(request.args.get("conv_roll"), 3), 0), 16)

    conv_pairs, conv_series, conv_series_cohort = [], [], []
    for i in range(4):
        a = _safe_int(request.args.get(f"c{i+1}f"), _CONV_DEFAULTS[i][0])
        b = _safe_int(request.args.get(f"c{i+1}t"), _CONV_DEFAULTS[i][1])
        a = a if a in _CONV_BY_NUM else _CONV_DEFAULTS[i][0]
        b = b if b in _CONV_BY_NUM else _CONV_DEFAULTS[i][1]
        conv_pairs.append({"i": i + 1, "a": a, "b": b})
        if b <= a:                      # destino tem que ser adiante da origem
            continue
        vals, _ = _conv_series(piv, qs, a, b, roll=conv_roll)
        conv_series.append({"name": f"S{a} → S{b}", "color": _CONV_COLORS[i],
                            "values": vals})
        vals_c, _ = _conv_series_cohort(piv, qs, a, b, roll=conv_roll)
        conv_series_cohort.append({"name": f"S{a} → S{b}", "color": _CONV_COLORS[i],
                                   "values": vals_c})
    # Cada trimestre do gráfico explode em "quantos deals por estágio".
    _cv_base = {k: v for k, v in (("conv_type", conv_type), ("leader", f["leader"]),
                                  ("ae", f["ae"])) if v}
    if conv_roll:
        _cv_base["roll"] = str(conv_roll)   # drill herda a janela rolling
    _x_urls = {_quarter_label(q): url_for("overview_funnel_stages") + "?" +
               urllib.parse.urlencode({**_cv_base, "q": _quarter_key(q)}) for q in qs}
    lc = (_build_line_chart([_quarter_label(q) for q in qs], conv_series, x_urls=_x_urls)
          if conv_series else None)
    lc_cohort = (_build_line_chart([_quarter_label(q) for q in qs], conv_series_cohort, x_urls=_x_urls)
                 if conv_series_cohort else None)

    return render_template(
        "overview_overtime.html", ctx=ctx, rk=rk, rk_nl=rk_nl,
        rk_tk=rk_tk, rk_up=rk_up, grain=grain,
        lc=lc, lc_cohort=lc_cohort, conv_pairs=conv_pairs, conv_stages=_CONV_STAGES,
        conv_colors=_CONV_COLORS, conv_type=conv_type,
        conv_roll=conv_roll,
        totals={"realized": tot_real, "quota": tot_quota,
                "att": (tot_real / tot_quota) if tot_quota else None,
                "deals": sum(i["deals"] for i in items), "n_q": len(items),
                "nl_real": tot_nl, "nl_quota": tot_nlq,
                "nl_att": (tot_nl / tot_nlq) if tot_nlq else None,
                "drill_real": drill_real_tot, "drill_nl": drill_nl_tot},
    )


def _overview_page(ov_tab, grain="ae"):
    """Visão consolidada de vendas: realizado (PO Won), ticket médio, ciclo de
    vendas, pipeline aberto e projeção — no período/líder/AE selecionados.
    Mesma base do /forecast (forecast_deals), então soma bate com aquela tela.
    `ov_tab` decide a sub-aba renderizada: 'sales' (macro) ou 'ae' (rankings+drill)."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    today = _today()
    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else _quarter_start_of(today)
    to_d   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else _quarter_end_of(today)
    lo, hi = pd.Timestamp(from_d), pd.Timestamp(to_d)

    # Base = forecast_deals (won + pipeline aberto). Região/AE filtram cedo; o
    # LÍDER é atribuído por quarter (abaixo) e filtrado DEPOIS — senão o líder do
    # roster Q3 vazaria pro histórico (Q2 deve ficar no líder de Q2).
    deals = _get_forecast_deals(force=force)
    dd = deals.copy()
    dd = dd[~dd["account_executive"].isin(DEPARTED_AES)]
    if f["region"]:
        dd = dd[dd["region"] == f["region"]]
    if f["ae"]:
        dd = dd[dd["account_executive"] == f["ae"]]

    # Identifica os deals em HOLD (em aprovação financeira): o total_mrr_on_hold_brl
    # da curada é um snapshot no quarter corrente, mas os deals que o compõem
    # fecharam (PO) em quarters passados. Caso o valor do on-hold de cada AE com
    # seus closedwon+PO recentes (1 ou 2 deals, tol. R$2) → esses deal_ids são HOLD,
    # atribuídos ao quarter do PO do deal. Assim um deal do Q2 em aprovação aparece
    # como hold NO Q2, e o trimestre vigente mostra só o hold que fechou nele.
    hold_ids = _identify_hold_deal_ids(deals, force=force)

    # VALIDADO (realizado) = ganho COM PO emitido, EXCETO os que estão em hold
    # (um deal é validado OU em aprovação, nunca os dois) → quarter da data do PO.
    # HOLD = deals em aprovação → quarter do PO. Pipeline aberto → close date.
    # O realizado (`won`, e o hold, que vira realizado quando aprova) recorta pela
    # data da RECEITA (`rev_po`, com a extensão de fechamento); `won_cal` é o mesmo
    # conjunto no calendário do PO e alimenta SÓ deals, ticket, ciclo, new logos e
    # nº de AEs — as métricas que não são receita seguem o calendário normal.
    # REGRA (Pedro, 04/11/2026): venda fechada com PO em validação financeira
    # (hold) CONTA no realizado, com o selo "em validação financeira". Por isso
    # `_won_base` não exclui mais o hold; `hld` segue existindo só pra informar
    # quanto do realizado ainda está em validação (card e selo do drill).
    _won_base = dd["is_won"] & dd["po_emission_date"].notna()
    won  = dd[_won_base & (dd["rev_po"] >= lo) & (dd["rev_po"] <= hi)].copy()
    won_cal = dd[_won_base & (dd["po_emission_date"] >= lo)
                 & (dd["po_emission_date"] <= hi)].copy()
    pipe = dd[(~dd["is_won"]) & dd["close_date"].notna()
              & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)].copy()
    hld  = dd[dd["deal_id"].isin(hold_ids) & dd["po_emission_date"].notna()
              & (dd["rev_po"] >= lo) & (dd["rev_po"] <= hi)].copy()

    # Atribui o líder de CADA deal pelo quarter da sua data (won/hold→PO, pipe→close):
    # o realizado cai no MESMO líder que a quota daquele quarter. Depois aplica
    # ALLOWED_LEADERS + filtro de líder.
    lmap = _period_leader_map(force=force)
    won["leader"]  = _attr_deal_leader(won,  "rev_po",           lmap)
    won_cal["leader"] = _attr_deal_leader(won_cal, "po_emission_date", lmap)
    pipe["leader"] = _attr_deal_leader(pipe, "close_date",       lmap)
    hld["leader"]  = _attr_deal_leader(hld,  "rev_po",           lmap)
    won  = won[won["leader"].isin(ALLOWED_LEADERS)]
    won_cal = won_cal[won_cal["leader"].isin(ALLOWED_LEADERS)]
    pipe = pipe[pipe["leader"].isin(ALLOWED_LEADERS)]
    hld  = hld[hld["leader"].isin(ALLOWED_LEADERS)]
    if f["leader"]:
        _scope = expand_leader(f["leader"])
        won  = won[won["leader"].isin(_scope)]
        won_cal = won_cal[won_cal["leader"].isin(_scope)]
        pipe = pipe[pipe["leader"].isin(_scope)]
        hld  = hld[hld["leader"].isin(_scope)]
    hold_by_ae = (hld.groupby("account_executive")["amount"].sum().to_dict()
                  if len(hld) else {})

    # Ciclo de vendas (dias) = validação − criação, só nos won com created_date.
    won_cal["cycle_days"] = (won_cal["valid_date"] - won_cal["created_date"]).dt.days
    cyc = won_cal.loc[won_cal["cycle_days"].notna() & (won_cal["cycle_days"] >= 0), "cycle_days"]

    def _ticket_of(frame):
        return (float(frame["amount"].sum()) / len(frame)) if len(frame) else 0.0

    def _stats(rev, cal):
        # Card "Total em …": R$ e o "N deals" embaixo dele são a MESMA população
        # (a receita, que é o que o drill do card lista). O card "Ticket médio" é
        # outra métrica e segue o calendário (drill com basis=cal).
        return {"mrr": float(rev["amount"].sum()), "deals": int(len(rev)),
                "ticket": _ticket_of(cal)}

    acq = _stats(won[won["deal_type"] == "Aquisition"],
                 won_cal[won_cal["deal_type"] == "Aquisition"])
    exp = _stats(won[won["deal_type"] == "Up & cross sell"],
                 won_cal[won_cal["deal_type"] == "Up & cross sell"])

    kpis = {
        "realized": float(won["amount"].sum()),
        "deals_won": int(len(won_cal)),
        "ticket": _ticket_of(won_cal),
        "cycle_avg": float(cyc.mean()) if len(cyc) else 0.0,
        "cycle_median": float(cyc.median()) if len(cyc) else 0.0,
        "cycle_n": int(len(cyc)),
        "pipe_gross": float(pipe["amount"].sum()),
        "pipe_weighted": float(pipe["weighted"].sum()),
        "pipe_deals": int(len(pipe)),
        "projection": float(won["amount"].sum()) + float(pipe["weighted"].sum()),
        "n_ae": int(won_cal.loc[~won_cal["account_executive"].isin(NON_AE_USERS),
                                "account_executive"].nunique()),
    }

    # Quebra por líder: realizado, deals, ticket, ciclo médio, pipeline ponderado, projeção.
    by_leader = []
    leaders = sorted(set(won["leader"].dropna()) | set(won_cal["leader"].dropna())
                     | set(pipe["leader"].dropna()))
    for ld in leaders:
        w = won[won["leader"] == ld]
        wc = won_cal[won_cal["leader"] == ld]
        p = pipe[pipe["leader"] == ld]
        c = wc.loc[wc["cycle_days"].notna() & (wc["cycle_days"] >= 0), "cycle_days"]
        realized = float(w["amount"].sum())
        weighted = float(p["weighted"].sum())
        by_leader.append({
            "leader": ld,
            "realized": realized,
            "deals": int(len(wc)),
            "ticket": _ticket_of(wc),
            "cycle": float(c.mean()) if len(c) else None,
            "pipe_weighted": weighted,
            "projection": realized + weighted,
        })
    by_leader.sort(key=lambda r: r["projection"], reverse=True)

    # ----- Metas (quota) + meta de new logos por AE.
    # Quando o período cabe num trimestre com roster (Q3/2026, Q4/2026...), a
    # fonte da verdade é o roster DELE (time + quota + meta de new logos
    # informados pela liderança). Fora disso, cai na quota da curated
    # (quota_in_brl, MRR) somada nos quarters do período — sem meta de new logos
    # (só existe no roster). Realizado (deal amount) é comparável à quota
    # (mesma unidade MRR). -----
    rq = _roster_window(from_d, to_d)
    period_is_q3 = rq is not None          # nome antigo: "o período tem roster"
    roster_q = _roster_of(rq)

    # New logos REALIZADOS = deals de Aquisição ganhos no período, por AE. É
    # contagem, não receita: segue o calendário do PO (sem a extensão).
    newlogos_actual = (won_cal[won_cal["deal_type"] == "Aquisition"]
                       .groupby("account_executive").size()
                       if len(won_cal) else pd.Series(dtype=int))

    newlogos_goal = {}          # meta de new logos por AE (só trimestre com roster)
    roster_meta = {}            # leader/region p/ AEs do roster sem deal no período
    if period_is_q3:
        scope = expand_leader(f["leader"]) if f["leader"] else None
        quota_by_ae = {}
        for name, v in roster_q.items():
            if f["region"] and (_roster_region() != f["region"]):
                continue
            if scope is not None and v["leader"] not in scope:
                continue
            if f["ae"] and name != f["ae"]:
                continue
            quota_by_ae[name] = float(v["quota"])
            newlogos_goal[name] = int(v["new_logos"])
            roster_meta[name] = {"leader": v["leader"], "region": _roster_region()}
    else:
        aep = _get_clean_ae_perf(force=force)
        aep = aep[(aep["quarter"] <= to_d) & (aep["quarter_end"] >= from_d)]
        if f["region"]:
            aep = aep[aep["region"] == f["region"]]
        if f["leader"]:
            aep = aep[aep["leader"].isin(expand_leader(f["leader"]))]
        if f["ae"]:
            aep = aep[aep["account_executive"] == f["ae"]]
        quota_by_ae = (aep.groupby("account_executive")["quota_in_brl"].sum().to_dict()
                       if len(aep) else {})
        if len(aep):
            snap = (aep.sort_values("quarter").drop_duplicates("account_executive", keep="last")
                       .set_index("account_executive"))
            roster_meta = {ae: {"leader": snap.loc[ae, "leader"], "region": snap.loc[ae, "region"]}
                           for ae in snap.index}

    # hold_by_ae já foi calculado lá em cima (deals em aprovação, atribuídos pelo
    # quarter do PO e recortados no período). leader/region p/ AEs que só têm hold:
    if len(hld):
        for _ae, _grp in hld.groupby("account_executive"):
            roster_meta.setdefault(_ae, {"leader": _grp["leader"].iloc[0],
                                         "region": (_grp["region"].iloc[0] if "region" in _grp else None)})

    # ----- Quebra por AE — a análise do time. Cada AE com realizado (validado + hold),
    # ticket, ciclo, pipeline ponderado, projeção, meta (quota), attainment, new logos. -----
    by_ae = []
    ae_names = sorted(set(won["account_executive"].dropna())
                      | set(won_cal["account_executive"].dropna())
                      | set(pipe["account_executive"].dropna())
                      | {a for a, q in quota_by_ae.items() if q > 0}
                      | {a for a, hv in hold_by_ae.items() if hv}
                      | set(newlogos_goal.keys()))
    for ae in ae_names:
        w = won[won["account_executive"] == ae]
        wc = won_cal[won_cal["account_executive"] == ae]
        p = pipe[pipe["account_executive"] == ae]
        c = wc.loc[wc["cycle_days"].notna() & (wc["cycle_days"] >= 0), "cycle_days"]
        realized = float(w["amount"].sum())
        weighted = float(p["weighted"].sum())
        # leader/region: primeiro won, senão pipeline, senão o snapshot/roster.
        src = w if len(w) else (wc if len(wc) else p)
        if len(src) and pd.notna(src["leader"].iloc[0]):
            leader = src["leader"].iloc[0]
            region = src["region"].iloc[0] if pd.notna(src["region"].iloc[0]) else None
        elif ae in roster_meta:
            leader = roster_meta[ae]["leader"]
            region = roster_meta[ae]["region"]
        else:
            leader = region = None
        quota = float(quota_by_ae.get(ae, 0.0) or 0.0)
        nl_goal = newlogos_goal.get(ae)
        nl_real = int(newlogos_actual.get(ae, 0))
        hold = float(hold_by_ae.get(ae, 0.0) or 0.0)   # em aprovação financeira
        by_ae.append({
            "account_executive": ae,
            "leader": leader,
            "region": region,
            "is_manager": ae in _managers_in(rq),
            "realized": realized,                       # Won c/ PO (validado + em validação)
            "hold": hold,                               # dos quais em validação financeira
            "won_total": realized,                      # hold já está no realizado
            "deals": int(len(wc)),                      # contagem: calendário
            "ticket": _ticket_of(wc),                   # ticket: calendário
            "cycle": float(c.mean()) if len(c) else None,
            "pipe_weighted": weighted,
            "pipe_deals": int(len(p)),
            "projection": realized + weighted,
            "quota": quota,
            "attainment": (realized / quota) if quota else None,
            "nl_goal": nl_goal,
            "nl_real": nl_real,
            "nl_att": (nl_real / nl_goal) if nl_goal else None,
        })
    by_ae.sort(key=lambda r: r["projection"], reverse=True)

    # Ranking dos vendedores: TODOS os vendedores (sem gestores), barras de
    # Realizado (validado) e Quota (meta), + linha de % de atingimento (realizado ÷
    # meta). Eixo X ordenado por % de atingimento DECRESCENTE. Combo bar+line (eixo
    # duplo R$/%). Geometria SVG pré-computada aqui; nomes clicáveis → drill no AE.
    _drill_base = {k: v for k, v in (("date_from", f["date_from"]), ("date_to", f["date_to"]),
                                     ("leader", f["leader"]), ("region", f["region"])) if v}
    _sellers = [r for r in by_ae if not r["is_manager"]]
    _sort_att = lambda rows, key: sorted(
        rows, key=lambda r: (r[key] is not None, r[key] if r[key] is not None else -1.0),
        reverse=True)

    if grain == "leader":
        # LÍDERES: cada barra = soma do time (expand_leader). Realizado × Quota (R$)
        # + % atingimento; drill por líder → deals de todos os AEs do time.
        _rk_path, _rk_key = "/leaders/overview/team", "leader"
        _lr = _leader_rollup(by_ae, f["leader"], ["realized", "quota"])
        for r in _lr:
            # Meta do líder = Board Target (planilha), não a soma das quotas do time.
            r["quota"] = _leader_targets(rq).get(r["leader"], r["quota"])
            r["attainment"] = (r["realized"] / r["quota"]) if r["quota"] else None
        _rev_items = [{"name": r["name"], "short": r["short"], "leader": r["leader"],
                       "realized": r["realized"], "quota": r["quota"],
                       "attainment": r["attainment"]} for r in _sort_att(_lr, "attainment")]
        rank = _build_rank_chart(_rev_items, _drill_base, axis_mode="currency",
                                 drill_path=_mx_path(_rk_path), drill_key=_rk_key)
        rank_nl = None
        if period_is_q3:
            # Só quem CARREGA meta de new logo entra no realizado do líder — a
            # mesma população do card e do ranking por AE (e da explosão). Antes
            # a venda direta do gestor (sem meta) somava aqui: no Q3/26 a barra
            # do Gustavo dizia 13 e a lista explodida, 9.
            _carriers = [{**r, "nl_real": (r["nl_real"] if r["nl_goal"] is not None else 0)}
                         for r in by_ae]
            _lnl = [r for r in _leader_rollup(_carriers, f["leader"], ["nl_real", "nl_goal"])
                    if r["nl_goal"] > 0]
            for r in _lnl:
                r["attainment"] = (r["nl_real"] / r["nl_goal"]) if r["nl_goal"] else None
            if _lnl:
                _nl_items = [{"name": r["name"], "short": r["short"], "leader": r["leader"],
                              "realized": int(r["nl_real"]), "quota": int(r["nl_goal"]),
                              "attainment": r["attainment"]} for r in _sort_att(_lnl, "attainment")]
                rank_nl = _build_rank_chart(_nl_items, _drill_base, axis_mode="count",
                                            drill_path=_mx_path(_rk_path), drill_key=_rk_key)
    else:
        # Chart 1 — RECEITA: Realizado × Quota (R$) + % atingimento. Ordenado por
        # % de atingimento DECRESCENTE. Geometria pré-computada em _build_rank_chart.
        _rev = _sort_att(_sellers, "attainment")
        _rev_items = [{"name": r["account_executive"], "short": short_name(r["account_executive"]),
                       "leader": r["leader"], "realized": r["realized"], "quota": r["quota"],
                       "attainment": r["attainment"]} for r in _rev]
        rank = _build_rank_chart(_rev_items, _drill_base, axis_mode="currency",
                                 drill_path=_mx_path("/overview/ae"))

        # Chart 2 — NEW LOGOS: mesma estrutura, mas Realizado × Meta (contagem) +
        # % atingimento de new logos. Só quando há meta de new logos (Q3). População
        # restrita a AEs COM meta (nl_goal is not None) — MESMA base do KPI nl_real (o
        # card acima), pra a soma dos realizados do chart nunca contradizer o headline.
        # meta 0 (ex: Luca) fica; attainment None vai pro fim da ordenação.
        rank_nl = None
        _nl_carriers = [r for r in _sellers if r["nl_goal"] is not None]
        if period_is_q3 and _nl_carriers:
            _nl = _sort_att(_nl_carriers, "nl_att")
            _nl_items = [{"name": r["account_executive"], "short": short_name(r["account_executive"]),
                          "leader": r["leader"], "realized": int(r["nl_real"] or 0),
                          "quota": int(r["nl_goal"] or 0), "attainment": r["nl_att"]} for r in _nl]
            rank_nl = _build_rank_chart(_nl_items, _drill_base, axis_mode="count",
                                        drill_path=_mx_path("/overview/ae"))

    # Totais de meta do time. IMPORTANTE: attainment compara MESMA população —
    # numerador (realizado) e denominador (quota) só dos AEs com meta (carriers de
    # quota). Vendas de gestor e de AEs fora do roster entram em "Vendas realizadas"
    # (total honesto), mas NÃO no attainment (senão infla, pois não têm quota).
    quota_carriers = [r for r in by_ae if r["quota"] > 0]
    total_quota = float(sum(r["quota"] for r in by_ae if not r["is_manager"]))  # soma das quotas individuais no escopo
    carrier_realized = float(sum(r["realized"] for r in quota_carriers))
    # Meta de RECEITA (card): no TIME INTEIRO usa a meta top-down FIXA
    # do trimestre (TEAM_REVENUE_META[rq], direto no card). "Time inteiro" = sem filtro de AE/região
    # e sem líder OU com o líder = VP (Pedro Mendes), que expande p/ todo o time.
    # Ao filtrar por um líder/AE/região específico, cai na soma das quotas individuais
    # daquele escopo (quotas individuais ficam como estão). Fora do Q3, usa a curated.
    is_whole_team = (not f["ae"] and not f["region"]
                     and f["leader"] in ("", "Pedro Mendes"))
    # TEAM_REVENUE_META é a meta top-down do BRASIL; no México a meta do time é
    # a soma das quotas do roster (o painel do Mauricio).
    if not _is_mx() and rq in TEAM_REVENUE_META and is_whole_team:
        meta_receita = float(TEAM_REVENUE_META[rq])
    else:
        meta_receita = total_quota
    kpis["quota"] = meta_receita
    kpis["quota_ic"] = total_quota                          # soma das quotas individuais (referência)
    kpis["realized_all"] = kpis["realized"]                 # total do time (todos)
    kpis["realized_carriers"] = carrier_realized            # só quem tem meta
    kpis["hold"] = float(sum(r["hold"] for r in by_ae))     # em aprovação financeira (MRR)
    kpis["attainment"] = (carrier_realized / meta_receita) if meta_receita else None
    kpis["gap"] = max(meta_receita - carrier_realized, 0.0)
    # New logos (meta vs realizado) — só quando há meta (período Q3). Numerador
    # restrito aos AEs com meta de new logos (mesma população da meta 87).
    kpis["has_newlogos"] = period_is_q3 and bool(newlogos_goal)
    kpis["nl_goal"] = int(sum(v for v in newlogos_goal.values()))
    kpis["nl_real"] = int(sum(r["nl_real"] for r in by_ae if r["nl_goal"] is not None))
    kpis["nl_att"] = (kpis["nl_real"] / kpis["nl_goal"]) if kpis["nl_goal"] else None
    # % da meta de receita no nível do time (receita total realizada ÷ meta distribuída).
    kpis["rev_att"] = (kpis["realized"] / meta_receita) if meta_receita else None
    # Reporte global: receita (meta + realizado) convertida de MRR BRL p/ ARR USD.
    kpis["quota_arr_usd"] = mrr_brl_to_arr_usd(meta_receita)
    kpis["realized_arr_usd"] = mrr_brl_to_arr_usd(kpis["realized"])

    # ----- Insights do time — leitura rápida pro líder -----
    real_aes = [r for r in by_ae if not r["is_manager"]]              # exclui gestores
    sold = [r for r in real_aes if r["realized"] > 0]
    pipe_only = [r for r in real_aes if r["realized"] <= 0 and r["pipe_weighted"] > 0]
    with_quota = [r for r in real_aes if r["quota"] > 0]
    hit_goal = [r for r in with_quota if r["attainment"] is not None and r["attainment"] >= 1.0]
    total_real = sum(r["realized"] for r in real_aes) or 0.0
    top3 = sorted(real_aes, key=lambda r: r["realized"], reverse=True)[:3]
    top3_real = sum(r["realized"] for r in top3)
    top_ae = top3[0] if top3 and top3[0]["realized"] > 0 else None
    insights = {
        "n_active": len(real_aes),                    # AEs c/ venda OU pipeline
        "n_sold": len(sold),                          # AEs que já venderam
        "n_pipe_only": len(pipe_only),                # só pipeline, sem venda ainda
        "n_quota": len(with_quota),                   # AEs com meta no período
        "n_hit_goal": len(hit_goal),                  # AEs que bateram a meta
        "top3_share": (top3_real / total_real) if total_real else 0.0,
        "top_ae": top_ae["account_executive"] if top_ae else None,
        "top_ae_real": top_ae["realized"] if top_ae else 0.0,
        # cobertura = pipeline ponderado ÷ realizado (quantas vezes o já-vendido
        # ainda está por vir, ponderado). Sinaliza dependência do pipeline.
        "coverage": (kpis["pipe_weighted"] / kpis["realized"]) if kpis["realized"] else None,
    }

    # ----- Drill de deals: quando um ÚNICO AE está selecionado, lista os deals
    # que ele FECHOU no período — nome, MRR e tipo (aquisição/expansão). Inclui os
    # validados (PO Won) e os em aprovação financeira (hold), cada um com seu status.
    # A soma dos MRR bate com Realizado + em aprovação do card acima. -----
    # No grain 'ae', o drill dispara ao selecionar um AE; no grain 'leader', ao
    # selecionar um líder (won/hld já vêm escopados por expand_leader). Na visão de
    # líder a tabela ganha a coluna Vendedor (deals de TODOS os AEs do time).
    deal_rows = None
    deal_scope = (f["ae"] if grain == "ae" else f["leader"]) or None
    if deal_scope:
        _KIND = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}
        def _mk_deals(frame, status):
            out = []
            for _, r in frame.iterrows():
                nm = r["deal_name"]
                nm = str(nm).strip() if (pd.notna(nm) and str(nm).strip()) else "(sem nome)"
                dt = r["deal_type"]
                kind = _KIND.get(dt, dt if pd.notna(dt) else "—")
                out.append({
                    "deal": nm,
                    "ae": r["account_executive"],
                    "mrr": float(r["amount"] or 0.0),
                    "kind": kind,
                    "status": status,
                    "rev_tag": _rev_tag(r["po_emission_date"]),
                    "date": (r["po_emission_date"].strftime("%d/%m/%Y")
                             if pd.notna(r["po_emission_date"]) else "—"),
                })
            return out
        _hold_set = set(hld["deal_id"])
        deal_rows = (_mk_deals(won[~won["deal_id"].isin(_hold_set)], "validado")
                     + _mk_deals(won[won["deal_id"].isin(_hold_set)], "aprovação"))
        deal_rows.sort(key=lambda x: x["mrr"], reverse=True)

    range_label = f"{from_d.strftime('%d/%m/%Y')} → {to_d.strftime('%d/%m/%Y')}"
    return render_template("overview.html", ctx=ctx, kpis=kpis, by_leader=by_leader,
                           by_ae=by_ae, rank=rank, rank_nl=rank_nl,
                           insights=insights, acq=acq, exp=exp, range_label=range_label,
                           usd_rate=USD_BRL_RATE, deal_rows=deal_rows, deal_ae=deal_scope,
                           ov_tab=ov_tab, grain=grain)


@app.route("/demand")
def demand():
    """Demand generation — bar chart X=AE, Y=opps geradas.
    4 segmentos por barra: auto-acq, auto-exp, rcv-acq, rcv-exp."""
    ctx = base_context()
    f = ctx["filters"]

    # Resolve date range
    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else date(2026, 4, 1)
    to_d   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else date(2027, 1, 31)

    # Slice em pandas a partir do cache deal-level — sem BQ por filtro.
    force = request.args.get("refresh") == "1"
    grain = request.args.get("grain", "month")
    if grain not in ("week", "month", "quarter"): grain = "month"

    deals = _get_demand_deals(force=force)
    by_ae = _demand_by_ae_pd(deals, from_d, to_d)
    grain_raw = _demand_by_grain_pd(deals, from_d, to_d, grain)

    by_ae = apply_leader_overrides(by_ae)
    by_ae = by_ae[by_ae["leader"].isin(ALLOWED_LEADERS)]
    by_ae = by_ae[~by_ae["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]

    # Apply region/leader/AE filters
    if f["region"]:
        by_ae = by_ae[by_ae["region"] == f["region"]]
    if f["leader"]:
        by_ae = by_ae[by_ae["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        by_ae = by_ae[by_ae["account_executive"] == f["ae"]]

    by_ae = by_ae.sort_values("total_generated", ascending=False).reset_index(drop=True)
    by_ae["auto_total"] = by_ae["auto_acq"] + by_ae["auto_exp"]
    # Recebidas total = Inbound + Outbound (já com os reclassificados) + Expansão.
    by_ae["rcv_total"]  = by_ae["rcv_inbound_all"] + by_ae["rcv_outbound_all"] + by_ae["rcv_exp"]
    by_ae["pct_auto"]   = by_ae.apply(
        lambda r: (r["auto_total"] / r["total_generated"] * 100) if r["total_generated"] else 0,
        axis=1,
    )

    # Geradas pelo próprio AE que JÁ chegaram ao estágio 3 (Scoping). Mesmo
    # universo de AEs do gráfico de cima (merge no by_ae já filtrado), então
    # quem gerou e não avançou nada aparece com barra zerada.
    scop = _demand_by_ae_pd(deals[deals["reached_scoping"]], from_d, to_d)
    scop = (scop.groupby("account_executive")[["auto_acq", "auto_exp"]].sum()
                .rename(columns={"auto_acq": "scop_acq", "auto_exp": "scop_exp"})
                .reset_index())
    by_ae = by_ae.merge(scop, on="account_executive", how="left")
    by_ae[["scop_acq", "scop_exp"]] = by_ae[["scop_acq", "scop_exp"]].fillna(0).astype(int)
    by_ae["scop_total"] = by_ae["scop_acq"] + by_ae["scop_exp"]

    # Meta semanal por AE: 3 opps × semanas decorridas no período
    weekly_goal = _weekly_ae_goal(from_d, to_d)
    # Reescala o eixo Y pra meta ser visível mesmo se ninguém bater.
    # `.max()` em série vazia → NaN, e NaN é truthy, então `or 0` não basta.
    _auto_max = by_ae["auto_total"].max() if len(by_ae) else 0
    _rcv_max  = by_ae["rcv_total"].max()  if len(by_ae) else 0
    max_auto = max(int(_auto_max) if pd.notna(_auto_max) else 0, weekly_goal, 1)
    max_rcv  = max(int(_rcv_max)  if pd.notna(_rcv_max)  else 0, 1)
    max_scop = max(int(by_ae["scop_total"].max()) if len(by_ae) else 0, 1)

    totals = {
        "n_ae": len(by_ae),
        "total_generated": int(by_ae["total_generated"].sum()),
        "auto_total":  int(by_ae["auto_acq"].sum() + by_ae["auto_exp"].sum()),
        "rcv_total":   int(by_ae["rcv_inbound_all"].sum() + by_ae["rcv_outbound_all"].sum() + by_ae["rcv_exp"].sum()),
        "acq_total":   int(by_ae["auto_acq"].sum() + by_ae["rcv_inbound_all"].sum() + by_ae["rcv_outbound_all"].sum()),
        "exp_total":   int(by_ae["auto_exp"].sum() + by_ae["rcv_exp"].sum()),
        "auto_acq":    int(by_ae["auto_acq"].sum()),
        "auto_exp":    int(by_ae["auto_exp"].sum()),
        "rcv_inbound": int(by_ae["rcv_inbound_all"].sum()),
        "rcv_outbound":int(by_ae["rcv_outbound_all"].sum()),
        "rcv_exp":     int(by_ae["rcv_exp"].sum()),
        # Reclassificados pela troca de deal_source_agg → motion (02/09/2026).
        "rcv_moved_out": int(by_ae["rcv_moved_out"].sum()),
        "rcv_moved_in":  int(by_ae["rcv_moved_in"].sum()),
        "scop_total":    int(by_ae["scop_total"].sum()),
    }
    totals["scop_pct"] = (totals["scop_total"] / totals["auto_total"] * 100) if totals["auto_total"] else 0

    week_rows, week_stats, max_week = _demand_last_week(deals, f)

    # Trend series — aplica filtros (líder hierárquico, AE) antes de pivot
    trend_pivot = []
    if len(grain_raw):
        gr = grain_raw.copy()
        gr = apply_leader_overrides(gr)
        gr = gr[gr["leader"].isin(ALLOWED_LEADERS)]
        gr = gr[~gr["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
        if f["region"]:
            gr = gr[gr["region"] == f["region"]]
        if f["leader"]:
            gr = gr[gr["leader"].isin(expand_leader(f["leader"]))]
        if f["ae"]:
            gr = gr[gr["account_executive"] == f["ae"]]
        gr["bucket"] = gr["source"] + "_" + gr["kind"]
        gp = gr.pivot_table(index="period", columns="bucket", values="n",
                            aggfunc="sum").fillna(0).reset_index()
        # 7 buckets: auto_acq, auto_exp, received_{inbound,outbound,moved_out,moved_in,exp}
        for col in ["auto_acq","auto_exp","received_inbound","received_outbound",
                    "received_moved_out","received_moved_in","received_exp"]:
            if col not in gp.columns: gp[col] = 0
        gp["total"] = (gp["auto_acq"] + gp["auto_exp"]
                       + gp["received_inbound"] + gp["received_outbound"]
                       + gp["received_moved_out"] + gp["received_moved_in"]
                       + gp["received_exp"])
        gp["period"] = pd.to_datetime(gp["period"])

        # Label do eixo X conforme o grão
        def _grain_label(d, g=grain):
            d = d.date() if hasattr(d, "date") else d
            if g == "week":    return f"{d.day:02d}/{d.month:02d}"          # ex 13/04
            if g == "quarter": return f"Q{(d.month-1)//3 + 1}/{d.year % 100:02d}"  # ex Q2/26
            return f"{PT_MONTHS_SHORT[d.month-1]}/{d.year % 100:02d}"     # ex abr/26
        gp["label"] = gp["period"].apply(_grain_label)

        # Intervalo [período_from, período_to] de cada barra — pro drill-down do
        # total explodir só os deals daquele período (semana/mês/trimestre).
        def _period_end(d, g=grain):
            d = d.date() if hasattr(d, "date") else d
            if g == "week":
                return d + timedelta(days=6)
            if g == "quarter":
                q_first_month = ((d.month - 1) // 3) * 3 + 1
                nxt = date(d.year + (q_first_month + 3 > 12), (q_first_month + 3 - 1) % 12 + 1, 1)
                return nxt - timedelta(days=1)
            nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
            return nxt - timedelta(days=1)
        gp["period_from"] = gp["period"].apply(lambda d: d.date().isoformat())
        gp["period_to"] = gp["period"].apply(lambda d: _period_end(d).isoformat())
        trend_pivot = gp.sort_values("period").to_dict("records")

    # URLs do toggle de grão (preserva todos os filtros principais)
    grain_args = {k: v for k, v in request.args.items() if k not in ("refresh", "grain") and v}
    grain_urls = {
        g: request.path + "?" + urllib.parse.urlencode({**grain_args, "grain": g})
        for g in ("week", "month", "quarter")
    }

    return render_template(
        "demand.html", ctx=ctx,
        by_ae=by_ae.to_dict("records"),
        trend=trend_pivot,
        grain=grain, grain_urls=grain_urls,
        totals=totals,
        max_auto=max_auto, max_rcv=max_rcv, max_scop=max_scop,
        weekly_goal=weekly_goal,
        week_rows=week_rows, week_stats=week_stats, max_week=max_week,
        range_from=from_d.isoformat(), range_to=to_d.isoformat(),
    )


# Portal do HubSpot (deste tenant) — usado pra montar o link do registro do deal.
HS_PORTAL_ID = "7933635"


def _hs_deal_url(deal_id) -> str:
    """URL do registro do deal no HubSpot (abre o deal). '' se id inválido."""
    did = str(deal_id if deal_id is not None else "").strip()
    if not did or did.lower() in ("nan", "none", "0"):
        return ""
    return f"https://app.hubspot.com/contacts/{HS_PORTAL_ID}/record/0-3/{did}"


# Colunas padrão do drill-down de deals (reutilizadas pelos relatórios via
# _drilldown.html). A coluna do nome vira hyperlink pro deal no HubSpot quando a
# linha traz `deal_url` (link="deal_url"). Cada endpoint pode acrescentar/trocar
# colunas conforme o recorte (ex.: dias parado, data de fechamento).
_DRILL_DEAL_COLUMNS = [
    {"key": "deal_name",    "label": "Deal", "wrap": True, "link": "deal_url"},
    {"key": "deal_type",    "label": "Tipo"},
    {"key": "stage",        "label": "Estágio"},
    {"key": "amount",       "label": "Valor",     "type": "money"},
    {"key": "created_date", "label": "Criado em", "type": "date"},
]


def _drill_money_subtitle(n, amount):
    # Sensível ao filtro mestre: a modal recebe ?money= no fetch, então o total
    # do subtítulo acompanha a moeda ativa (R$ MRR ou US$ ARR).
    label = fmt_money(amount)  # "R$ 137.320" ou "US$ 326.284"
    return f"{n} deal{'s' if n != 1 else ''} · valor total {label}"


def _empty_drill(title=""):
    return {"title": title, "subtitle": "", "columns": [], "rows": []}


def _filter_entity(df, name, grain):
    """Filtra um DataFrame de deals pelo que foi clicado: um AE (grain='ae') ou
    um líder (grain='leader' → todos os AEs do time via expand_leader)."""
    if grain == "leader":
        return df[df["leader"].isin(expand_leader(name))]
    return df[df["account_executive"] == name]


def _deal_type_map():
    """{deal_id: deal_type} a partir do cache de pipeline_deals (barato). Usado pra
    enriquecer drill-downs cuja fonte não traz o tipo do deal (stalled/funil)."""
    d = _get_pipeline_deals()
    return dict(zip(d["deal_id"].astype(str), d["deal_type"]))


# ---- Explosão respeita TODOS os filtros da tela (21/10/2026) ----
# Antes, cada link de drill listava à mão o que levava (quase sempre só líder,
# região e datas) e cada rota aplicava o que lembrava: filtrar um AE e explodir
# qualquer número trazia deals do time inteiro, e o clique numa linha de líder
# ignorava o filtro de AE/região da tela. Agora são duas peças, num lugar só:
#   1) `_drill_carry` (chamado pelo `url_for` acima): todo link pra uma rota de
#      drill herda os filtros de QUEM da tela (ae, líder, região) e os cortes da
#      própria aba (Deal Type, Source, Product…), quando o link não diz outro;
#   2) `_drill_who`: a rota aplica esses filtros por cima do que foi clicado
#      (interseção). `_entity_filter` já chama, então quem usa ele herda.
_DRILL_WHO_KEYS = ("ae", "leader", "region")
# Rotas de explosão que não terminam em "deals".
_DRILL_EXTRA_RULES = {"/overview/funnel-stages", "/pipeline/funnel/moves",
                      "/pipeline/wow/quarter/cobertura",
                      "/leaders/pipeline/wow/quarter/cobertura"}
_DRILL_ENDPOINTS: set = set()


def _drill_endpoint_set() -> set:
    if not _DRILL_ENDPOINTS:
        for r in app.url_map.iter_rules():
            if r.rule.endswith("deals") or _rota_base(r.rule) in _DRILL_EXTRA_RULES:
                _DRILL_ENDPOINTS.add(r.endpoint)
    return _DRILL_ENDPOINTS


def _page_slice_keys(path: str) -> list:
    """Cortes próprios da aba (além de quem/período) que a tela aplica e que o
    drill tem que repetir. Saem do mesmo registro da barra de filtros, mais os
    que a barra carrega escondidos (close date/customer owner no Pipeline Total;
    envolvimento/coorte na Legal)."""
    keys = [x["key"] for x in _extra_filters_for_path(path)]
    base = _rota_base(path)
    if base in PIPELINE_TOTAL_PATHS:
        keys += ["cdate", "cown"]
    if base in LEGAL_PATHS:
        keys += ["env", "coh"]
    return keys


def _drill_carry(endpoint, values: dict) -> dict:
    """Completa a URL de um drill com os filtros ativos da requisição atual que o
    link não trouxe. Não mexe no que o link já define (nem vazio de propósito com
    valor: só preenche chave ausente ou vazia). Período fica de fora: cada tela
    resolve o seu (pin de trimestre, domingos, janela aberta) e o link já leva
    as datas certas."""
    from flask import has_request_context
    if not has_request_context() or endpoint not in _drill_endpoint_set():
        return values
    keys = list(_DRILL_WHO_KEYS)
    if request.endpoint not in _drill_endpoint_set():
        keys += _page_slice_keys(request.path)
    out = dict(values)
    for k in keys:
        v = request.args.get(k)
        if v and out.get(k) in (None, ""):
            out[k] = v
    return out


def _drill_who(df, ae_col="account_executive", leader_col="leader", region_col="region"):
    """Aplica os filtros de QUEM da requisição (ae, líder, região) num frame de
    deals do drill. É interseção com o que foi clicado: na tela filtrada por um
    AE, explodir o líder dele mostra só os deals daquele AE."""
    from flask import has_request_context
    if not has_request_context() or df is None or not len(df):
        return df
    ae = (request.args.get("ae") or "").strip()
    leader = (request.args.get("leader") or "").strip()
    region = (request.args.get("region") or "").strip()
    if ae and ae_col in df.columns:
        df = df[df[ae_col] == ae]
    if leader and leader_col in df.columns:
        df = df[df[leader_col].isin(expand_leader(leader))]
    if region and region_col in df.columns:
        df = df[df[region_col] == region]
    return df


def _entity_filter(df, name, grain, leader=None, region=None):
    """Filtro do drill-down: com `name` → um AE/líder; sem `name` → TOTAL (todos
    os AEs). Por cima, sempre, os filtros de quem da tela (`_drill_who`) — antes
    eles só valiam no total, e o filtro de AE não valia em lugar nenhum. Assume
    que ALLOWED_LEADERS/HIDDEN já foram aplicados pelo caller."""
    if name:
        df = _filter_entity(df, name, grain)
    if region and "region" in df.columns:
        df = df[df["region"] == region]
    if leader:
        df = df[df["leader"].isin(expand_leader(leader))]
    return _drill_who(df)


def _with_ae_col(columns, grain):
    """Na visão de líder, insere a coluna 'AE' (logo após o nome do deal) pra
    mostrar de qual vendedor é cada deal. Na visão de AE, devolve como está."""
    if grain != "leader":
        return columns
    return [columns[0], {"key": "ae", "label": "AE"}] + list(columns[1:])


@app.route("/demand/deals")
def demand_deals():
    """Drill-down JSON de demanda. `name` + `grain` na convenção dos outros
    drills: grain='ae' → só os deals do vendedor clicado; grain='leader' → os
    deals de todos os AEs que respondem àquele líder. Sem `name` → TOTAL da
    página. `source=auto|received` fecha a lista com a barra clicada.
    Formato genérico consumido por _drilldown.html."""
    # `ae=` continua aceito como apelido de `name` (URLs antigas/salvas).
    name  = (request.args.get("name") or request.args.get("ae") or "").strip()
    grain = request.args.get("grain") if request.args.get("grain") in ("ae", "leader") else "ae"
    is_total = name == "" or name == "__total__"
    if not is_total and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    try:
        from_d = pd.to_datetime(request.args.get("date_from")).date() if request.args.get("date_from") else date(2026, 4, 1)
        to_d   = pd.to_datetime(request.args.get("date_to")).date()   if request.args.get("date_to")   else date(2027, 1, 31)
    except (ValueError, TypeError):
        from_d, to_d = date(2026, 4, 1), date(2027, 1, 31)

    # `source=auto` (ex.: gráfico da semana passada) → só as auto-geradas, pra
    # lista bater com o número da barra em vez de somar as recebidas também.
    src = (request.args.get("source") or "").strip() or None
    # `bucket=` vem do clique num segmento da barra (ex.: a faixa "passou a
    # Inbound"), pra lista mostrar exatamente aqueles deals.
    bkt = (request.args.get("bucket") or "").strip() or None
    # `scoping=1` vem do gráfico "chegaram a Scoping": só os deals que já
    # entraram no estágio 3 ou além.
    scop = request.args.get("scoping") == "1"
    deals = _get_demand_deals(force=request.args.get("refresh") == "1")
    rows = _demand_deals_rows(
        deals, from_d, to_d,
        name=None if is_total else name, grain=grain,
        leader=request.args.get("leader"), region=request.args.get("region"),
        source=src, bucket=bkt, scoping=scop,
    )
    title = "Geração de Demanda — total" if is_total else name
    if scop:
        title += " · chegaram a Scoping"
    if bkt:
        labels = [_CANAL_LABEL.get(b.strip(), b.strip()) for b in bkt.split(",") if b.strip()]
        if labels:
            title += " · " + " + ".join(labels)
    amount = float(sum(r["amount"] for r in rows))
    cols = [
        {"key": "deal_name",  "label": "Deal", "wrap": True, "link": "deal_url"},
        {"key": "deal_type",  "label": "Tipo"},
        {"key": "qual_owner", "label": "Qualification owner"},
        {"key": "stage",      "label": "Estágio"},
        {"key": "amount",     "label": "Valor",     "type": "money"},
        {"key": "created_date", "label": "Criado em", "type": "date"},
    ]
    # Nos recortes de canal, mostra o rastro da classificação — o que a regra
    # antiga (source) dizia e o que a `motion` diz — pra conferência na hora.
    if bkt or src == "received":
        cols = cols[:1] + [
            {"key": "canal",              "label": "Canal"},
            {"key": "motion_first",       "label": "motion (1º)"},
            {"key": "motion",             "label": "motion (hoje)"},
            {"key": "motion_changed_at",  "label": "Mudou em", "type": "date"},
            {"key": "n_changes",          "label": "Trocas", "type": "int"},
        ] + cols[1:]
    if scop:
        cols.append({"key": "scoping_date", "label": "Chegou a Scoping", "type": "date"})
    # Total e visão de líder juntam vários AEs → mostra de quem é cada deal.
    if is_total or grain == "leader":
        cols = _with_ae_col(cols, "leader")
    return jsonify({
        "title": title,
        "subtitle": _drill_money_subtitle(len(rows), amount) + " gerado no período",
        "columns": cols,
        "rows": rows,
    })


# Colunas do drill-down de inatividade (parado por stage / por atividade).
_DRILL_STALLED_COLUMNS = [
    {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
    {"key": "deal_type", "label": "Tipo"},
    {"key": "stage",     "label": "Estágio"},
    {"key": "amount",    "label": "Valor",       "type": "money"},
    {"key": "days",      "label": "Dias parado", "type": "int"},
]


def _stale_wow_by_stage(frame, stage_col):
    """{AE: {chave_do_estágio: n}} — breakdown por estágio-âncora dos deals que
    saíram/entraram da inatividade (chips de estágio do chart WoW recalculam
    no cliente a partir disso)."""
    g = frame.groupby(["account_executive", stage_col])["deal_id"].count()
    out = {}
    for (ae, sid), n in g.items():
        k = _STALLED_STAGE_KEYS.get(sid)
        if k:
            out.setdefault(ae, {})[k] = int(n)
    return out


def _stalled_stage_ids_arg():
    """Lê o param `stages` (chaves curtas qual,proj,... dos chips da legenda) e
    devolve o set de stage_ids selecionados — None se não veio/veio tudo."""
    raw = (request.args.get("stages") or "").strip()
    if not raw:
        return None
    key2id = {v: k for k, v in _STALLED_STAGE_KEYS.items()}
    ids = {key2id[k] for k in raw.split(",") if k in key2id}
    # Pedido sem nenhum estágio válido (todos os chips desligados) = nada, e
    # não "todos": um id que não casa com deal nenhum.
    return ids or {"__nenhum__"}


def _stalled_stage_title(base: str) -> str:
    """Título do drill com o(s) estágio(s) pedido(s) — clicar no pedaço Decision
    da barra abre "Fulano · Decision", e não um título igual ao da barra."""
    ids = _stalled_stage_ids_arg()
    if not ids:
        return base
    labels = [_STALLED_STAGE_LABELS[s] for s in _STALLED_STAGE_LABELS if s in ids]
    return f"{base} · {' + '.join(labels)}" if labels else base


def _last_return_label(r) -> str:
    """"dd/mm · canal" do último retorno de contato (drill da Inatividade).
    "—" quando o deal nunca teve retorno na janela (a contagem caiu na data de
    entrada no stage). Frames sem as colunas de canal (Stale) devolvem ""."""
    if "last_activity_at" not in r:
        return ""
    la = pd.to_datetime(r.get("last_activity_at"), errors="coerce", utc=True)
    if pd.isna(la):
        return "—"
    canal = []
    for col, nome in (("last_whatsapp_at", "WhatsApp"), ("last_gong_at", "reunião Gong"),
                      ("last_email_at", "e-mail"), ("last_call_at", "ligação")):
        v = pd.to_datetime(r.get(col), errors="coerce", utc=True)
        if pd.notna(v) and v.date() == la.date():
            canal.append(nome)
    # Retorno de outro ano leva o ano: "29/10" de 2025 lido em out/2026 parecia
    # data futura no drill.
    quando = f"{la:%d/%m}" if la.year == _today().year else f"{la:%d/%m/%y}"
    return f"{quando} · {' + '.join(canal) or '—'}"


def _stalled_drill(df, name, grain, days_col, stage_ids_col="stage_curr",
                   subtitle_suffix="parado", leader=None, region=None, title=None,
                   stage_ids=None, thresholds=None, keep_managers=False):
    """Payload do drill-down de inatividade. Com `name` → um AE/líder; sem `name`
    → TOTAL (todos os AEs). Marca stalled pelo threshold do stage e devolve só os
    parados, ordenados por dias desc. Mostra a coluna AE no total e na visão líder.
    `stage_ids` (dos chips da legenda) restringe aos estágios selecionados."""
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    # Inatividade (09/10/2026): deal de GESTOR entra no total e na visão de
    # Líderes; só quem saiu da empresa fica fora.
    df = (drop_departed_people(df) if keep_managers
          else df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)])
    df = _entity_filter(df, name, grain, leader, region)
    if stage_ids:
        df = df[df[stage_ids_col].isin(stage_ids)]
    rows = []
    if len(df):
        tmap = _deal_type_map()
        df = df.copy()
        _th = thresholds or _STALLED_THRESHOLDS
        df = df[df.apply(lambda r: bool(pd.notna(r[days_col]) and _th.get(r[stage_ids_col], 999) < r[days_col]),
                         axis=1)]
        df = df.sort_values(days_col, ascending=False)
        rows = [{
            "deal_name": r["deal_name"],
            "deal_url":  _hs_deal_url(r["deal_id"]),
            "ae":        r["account_executive"],
            "deal_type": tmap.get(str(r["deal_id"]), "—") or "—",
            "stage":     _STALLED_STAGE_LABELS.get(r[stage_ids_col], r[stage_ids_col]),
            "amount":    float(r["amount"] or 0),
            "days":      int(r[days_col]),
            "last_ret":  _last_return_label(r),
        } for _, r in df.iterrows()]
    amount = sum(x["amount"] for x in rows)
    show_ae = "leader" if (grain == "leader" or not name) else "ae"
    return {"title": title or name,
            "subtitle": _drill_money_subtitle(len(rows), amount) + f" {subtitle_suffix}",
            "columns": _with_ae_col(_DRILL_STALLED_COLUMNS, show_ae), "rows": rows}


@app.route("/pipeline/stalled/deals")
def pipeline_stalled_deals():
    """Drill-down: deals parados (por tempo no stage) de um AE/líder, ou TOTAL
    (sem name → todos os AEs)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    payload = _stalled_drill(bq.pipeline_stalled(force_refresh=request.args.get("refresh") == "1", pipeline=_pl()),
                             name, grain, days_col="days_stalled",
                             subtitle_suffix="parado (sem mudança de stage)",
                             leader=request.args.get("leader"), region=request.args.get("region"),
                             title=_stalled_stage_title(name or "Stale — total"),
                             stage_ids=_stalled_stage_ids_arg())
    return jsonify(payload)


def _stale_wow_deals_payload(dw, prev_sun, last_sun, title_all, days_label, thresholds=None,
                             keep_managers=False):
    """Payload compartilhado do drill de Movimentação WoW (Stale e Stale ·
    Atividade): deals que SAÍRAM da inatividade (estavam parados no domingo
    anterior e destravaram) e que ENTRARAM (cruzaram o threshold na semana).
    `dir` = saiu|entrou (sem = ambos); `stages` (chaves dos chips) restringe ao
    estágio-âncora: onde estava parado (saiu) ou onde parou (entrou)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return _empty_drill(name)
    direction = (request.args.get("dir") or "").strip()
    stage_ids = _stalled_stage_ids_arg()

    dw = _apply_roster_current(dw)
    dw = dw[dw["leader"].isin(ALLOWED_LEADERS)]
    dw = (drop_departed_people(dw) if keep_managers
          else dw[~dw["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)])
    dw = _entity_filter(dw, name, grain,
                        request.args.get("leader"), request.args.get("region"))

    def _stalled_at(stage, days):
        th = (thresholds or _STALLED_THRESHOLDS).get(stage)
        # bool() + notna: deal que fechou na semana vem com dias NULOS; sem
        # isso a coluna virava 'object' e o ~ do pandas (bitwise em bool do
        # Python: ~False = -1) marcava TODO parado anterior como 'saiu'.
        return bool(th is not None and pd.notna(days) and days > th)
    dw = dw.copy()
    dw["parado_curr"] = dw.apply(lambda r: _stalled_at(r["stage_curr"], r["days_curr"]), axis=1).astype(bool)
    dw["parado_prev"] = dw.apply(lambda r: _stalled_at(r["stage_prev"], r["days_prev"]), axis=1).astype(bool)

    tmap = _deal_type_map()
    rows = []
    def _mk(frame, lbl, stage_col, days_col):
        for _, r in frame.iterrows():
            rows.append({
                "deal_name": r.get("deal_name") or "—",
                "deal_url":  _hs_deal_url(r.get("deal_id")),
                "ae":        r["account_executive"],
                "mov":       lbl,
                "deal_type": tmap.get(str(r["deal_id"]), "—") or "—",
                "stage":     _STALLED_STAGE_LABELS.get(r[stage_col], r[stage_col] or "—"),
                "amount":    float(r["amount"] or 0),
                "days":      int(r[days_col]) if pd.notna(r[days_col]) else 0,
            })
    if direction in ("", "saiu"):
        saiu = dw[dw["parado_prev"] & ~dw["parado_curr"]]
        if stage_ids:
            saiu = saiu[saiu["stage_prev"].isin(stage_ids)]
        _mk(saiu, "Saiu", "stage_prev", "days_prev")   # estágio/dias de ONDE estava parado
    if direction in ("", "entrou"):
        ent = dw[~dw["parado_prev"] & dw["parado_curr"]]
        if stage_ids:
            ent = ent[ent["stage_curr"].isin(stage_ids)]
        _mk(ent, "Entrou", "stage_curr", "days_curr")
    rows.sort(key=lambda x: x["days"], reverse=True)

    n_saiu = sum(1 for x in rows if x["mov"] == "Saiu")
    n_ent = len(rows) - n_saiu
    _titles = {"saiu": "Saiu da inatividade", "entrou": "Entrou na inatividade"}
    title = _titles.get(direction, title_all)
    return {
        "title": title + (f" · {name}" if name else ""),
        "subtitle": (f"{_fmt_date_short(prev_sun)} → {_fmt_date_short(last_sun)} · "
                     f"saiu {n_saiu} · entrou {n_ent}"),
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "mov",       "label": "Movimento"},
            {"key": "deal_type", "label": "Tipo"},
            {"key": "stage",     "label": "Estágio"},
            {"key": "amount",    "label": "Valor",       "type": "money"},
            {"key": "days",      "label": days_label,    "type": "int"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    }


@app.route("/pipeline/stalled/wow-deals")
def pipeline_stalled_wow_deals():
    """Drill da Movimentação WoW da página Stale (inatividade por tempo no stage)."""
    last_sun, prev_sun = _last_two_sundays()
    dw = bq.pipeline_stalled_wow(f"{last_sun.isoformat()} 15:00:00 UTC",
                                 f"{prev_sun.isoformat()} 15:00:00 UTC", pipeline=_pl())
    return jsonify(_stale_wow_deals_payload(dw, prev_sun, last_sun,
                                            "Movimentação WoW · Stale", "Dias parado"))


@app.route("/pipeline/stalled-activity/wow-deals")
def pipeline_activity_wow_deals():
    """Drill da Movimentação WoW da página Inatividade (dias sem retorno de
    contato). Rota/arquivos seguem com o nome antigo 'stalled-activity' — só o
    rótulo de tela mudou, pra não quebrar links salvos."""
    last_sun, prev_sun = _last_two_sundays()
    dw = bq.pipeline_activity_wow(f"{last_sun.isoformat()} 15:00:00 UTC",
                                  f"{prev_sun.isoformat()} 15:00:00 UTC", pipeline=_pl())
    return jsonify(_stale_wow_deals_payload(dw, prev_sun, last_sun,
                                            "Movimentação WoW · Inatividade",
                                            "Dias sem retorno",
                                            thresholds=_ACTIVITY_THRESHOLDS, keep_managers=True))


@app.route("/pipeline/stalled-activity/deals")
def pipeline_activity_deals():
    """Drill-down: deals sem retorno de contato (inatividade por atividade), por
    AE/líder ou TOTAL (sem name)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    payload = _stalled_drill(bq.pipeline_activity(force_refresh=request.args.get("refresh") == "1", pipeline=_pl()),
                             name, grain, days_col="days_since_activity",
                             subtitle_suffix="sem retorno de contato",
                             leader=request.args.get("leader"), region=request.args.get("region"),
                             title=_stalled_stage_title(name or "Inatividade — total"),
                             stage_ids=_stalled_stage_ids_arg(),
                             thresholds=_ACTIVITY_THRESHOLDS, keep_managers=True)
    payload["columns"] = _with_ae_col([
        {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
        {"key": "deal_type", "label": "Tipo"},
        {"key": "stage",     "label": "Estágio"},
        {"key": "amount",    "label": "Valor",              "type": "money"},
        {"key": "days",      "label": "Dias sem atividade", "type": "int"},
        {"key": "last_ret",  "label": "Último retorno"},
    ], "leader" if (grain == "leader" or not name) else "ae")
    return jsonify(payload)


@app.route("/pipeline/deals")
def pipeline_coverage_deals():
    """Drill-down: deals abertos que compõem a cobertura (early/late) de um
    AE/líder, no mesmo recorte de close date da página."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    bucket = request.args.get("bucket")
    if bucket not in ("early", "late") or (name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS):
        return jsonify(_empty_drill(name))
    try:
        cf = pd.to_datetime(request.args.get("date_from")).date() if request.args.get("date_from") else None
        ct = pd.to_datetime(request.args.get("date_to")).date()   if request.args.get("date_to")   else None
    except (ValueError, TypeError):
        cf = ct = None

    d = _get_pipeline_deals()
    d = d[d["leader"].isin(ALLOWED_LEADERS)]
    d = d[~d["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    d = d[d["is_open"] & (d["bucket"] == bucket)]
    d = _entity_filter(d, name, grain, request.args.get("leader"), request.args.get("region"))
    # Mesma máscara de close date do _pipeline_by_ae_pd (só quando há filtro).
    if cf is not None or ct is not None:
        cd = pd.to_datetime(d["hs_closedate"], errors="coerce")
        mask = cd.notna()
        if cf is not None:
            mask &= cd >= pd.Timestamp(cf)
        if ct is not None:
            mask &= cd <= pd.Timestamp(ct)
        d = d[mask]
    d = d.copy()
    d["hs_closedate"] = pd.to_datetime(d["hs_closedate"], errors="coerce")
    d = d.sort_values("amount", ascending=False)

    def _close_cols(ts):
        """Fechamento como campo único 'M3S4' (mês do trimestre + semana), mais a
        segunda da semana como chave de ordenação — ela é monótona, então ordena
        certo mesmo atravessando trimestre/ano (o texto 'M1S1' não ordenaria)."""
        if pd.isna(ts):
            return "—", ""
        m, w, monday = _fiscal_week_of(ts.date())
        return f"M{m}S{w}", monday.isoformat()

    rows = []
    for _, r in d.iterrows():
        cw, csort = _close_cols(r["hs_closedate"])
        rows.append({
            "deal_name":  r["deal_name"] or "—",
            "deal_url":   _hs_deal_url(r["deal_id"]),
            "ae":         r["account_executive"],
            "deal_type":  r.get("deal_type") or "—",
            "stage":      stage_display(r.get("stage_label")),
            "amount":     float(r["amount"] or 0),
            "close_week": cw,
            "close_sort": csort,
        })
    amount = float(sum(x["amount"] for x in rows))
    return jsonify({
        "title": name or f"Cobertura {bucket} — total",
        "subtitle": _drill_money_subtitle(len(rows), amount) + f" em cobertura {bucket}",
        "columns": _with_ae_col([
            {"key": "deal_name",    "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "deal_type",    "label": "Tipo"},
            {"key": "stage",        "label": "Estágio"},
            {"key": "amount",     "label": "Valor", "type": "money"},
            # Campo único M{mês do trimestre}S{semana}; ordena pela data da semana.
            {"key": "close_week", "label": "Fechamento", "sortKey": "close_sort"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


# Colunas do drill-down de forecast (projeção = realizado + pipeline ponderado).
_DRILL_FORECAST_COLUMNS = [
    {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
    {"key": "deal_type", "label": "Tipo"},
    {"key": "categoria", "label": "Categoria"},
    {"key": "amount",    "label": "Valor bruto", "type": "money"},
    {"key": "ponderado", "label": "Ponderado",   "type": "money"},
    {"key": "data",      "label": "Data",        "type": "date"},
]


def _forecast_window(args):
    today = _today()
    try:
        fd = pd.to_datetime(args.get("date_from")).date() if args.get("date_from") else _quarter_start_of(today)
        td = pd.to_datetime(args.get("date_to")).date()   if args.get("date_to")   else _quarter_end_of(today)
    except (ValueError, TypeError):
        fd, td = _quarter_start_of(today), _quarter_end_of(today)
    return pd.Timestamp(fd), pd.Timestamp(td)


@app.route("/forecast/deals")
def forecast_deals():
    """Drill-down: deals que compõem a projeção de um AE/líder — realizado (PO Won
    no período) + pipeline aberto (close date no período, com a categoria de
    forecast). Mesma base do /forecast e /overview."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    lo, hi = _forecast_window(request.args)
    # MESMA base de `_forecast_page`: só quem saiu da empresa fica de fora — a
    # venda direta de gestor está no total e na barra do líder dele. Antes o
    # drill tirava HIDDEN_FROM_AE_VIEWS (gestores inclusive) e o total explodido
    # não fechava com o card.
    dd = apply_leader_overrides(_get_forecast_deals().copy())
    dd = dd[~dd["account_executive"].isin(DEPARTED_AES)]
    dd = dd[dd["leader"].isin(ALLOWED_LEADERS)]
    dd = _entity_filter(dd, name, grain, request.args.get("leader"), request.args.get("region"))
    # Deal Type vem na URL do card/barra clicado, não de `get_filters()`: este
    # drill também serve o Overview, que não tem esse corte na barra — lendo o
    # filtro global, um chip herdado de outra aba recortaria a lista explodida
    # de uma tela que mostra o total. Quem passa o dtype é quem o aplica.
    dd = _pt_apply_dim_filter(dd, "dtype", (request.args.get("dtype") or "").strip())

    # Realizado pela data da RECEITA — `rev_date` (PO, ou close date quando o
    # deal ganho ainda não tem PO), com a extensão de fechamento. É a régua do
    # card; com `rev_po` o won sem PO entrava na tela e sumia da explosão.
    won = dd[dd["is_won"] & dd["rev_date"].notna()
             & (dd["rev_date"] >= lo) & (dd["rev_date"] <= hi)]
    pipe = dd[(~dd["is_won"]) & dd["close_date"].notna()
              & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)]
    # Chips da legenda do gráfico (`cat` = realized + chaves das categorias,
    # ver `_fc_key`): com algum desligado, a explosão leva só os ligados — e o
    # pipe sem categoria (peso 0) sai, porque não compõe nenhum segmento.
    _cat = (request.args.get("cat") or "").strip()
    if _cat:
        _want = {x.strip().lower() for x in _cat.split(",")}
        if "realized" not in _want:
            won = won.iloc[0:0]
        pipe = pipe[pipe["forecast_category"].map(
            lambda c: _fc_key(c) if isinstance(c, str) else "").isin(_want)]

    rows = []
    for _, r in won.iterrows():
        amt = float(r["amount"] or 0)
        _tag = _rev_tag(r["po_emission_date"])
        _dt = r["po_emission_date"] if pd.notna(r["po_emission_date"]) else r.get("close_date")
        rows.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                     "ae": r["account_executive"], "deal_type": r.get("deal_type") or "—",
                     "categoria": (f"Won (realizado · {_tag})" if _tag else
                                   "Won (realizado)" if pd.notna(r["po_emission_date"]) else
                                   "Won (realizado · sem PO)"),
                     "amount": amt, "ponderado": amt,
                     "data": _dt.date().isoformat() if pd.notna(_dt) else ""})
    for _, r in pipe.iterrows():
        rows.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                     "ae": r["account_executive"], "deal_type": r.get("deal_type") or "—",
                     # Valor CRU do HubSpot ("COMMIT", "BEST_CASE") vazava pro
                     # drill: a tela mostrava o peso e a lista explodida ainda
                     # mostrava o nome. Passa pelo mesmo mapa de rótulos das
                     # telas, então os dois sempre dizem a mesma coisa.
                     "categoria": FORECAST_CATEGORY_LABEL.get(
                         r.get("forecast_category"), "Sem categoria"),
                     "amount": float(r["amount"] or 0),
                     "ponderado": float(r.get("weighted") or 0),
                     "data": r["close_date"].date().isoformat() if pd.notna(r["close_date"]) else ""})
    rows.sort(key=lambda x: x["data"], reverse=True)
    amount = float(sum(x["amount"] for x in rows))
    return jsonify({
        "title": name or "Forecast — total",
        "subtitle": (f"{len(won)} realizado(s) + {len(pipe)} em pipeline · "
                     f"valor bruto {fmt_money(amount)}"),
        "columns": _with_ae_col(_DRILL_FORECAST_COLUMNS, "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


def _newlogo_goal_aes(from_d, to_d, region=None, leader=None, ae=None):
    """AEs que CARREGAM meta de new logo no período (roster do trimestre). Fora dele não
    existe meta → conjunto vazio. Espelha `newlogos_goal` de _overview_page: o KPI
    'Realizado de new logo' só soma esses AEs, então o drill-down precisa do mesmo
    recorte pra a contagem bater com o card."""
    rq = _roster_window(from_d, to_d)
    if rq is None:
        return set()
    scope = expand_leader(leader) if leader else None
    out = set()
    for nm, v in _roster_of(rq).items():
        if region and _roster_region() != region:
            continue
        if scope is not None and v["leader"] not in scope:
            continue
        if ae and nm != ae:
            continue
        out.add(nm)
    return out


@app.route("/overview/deals")
def overview_deals():
    """Drill-down: deals fechados (realizado + em aprovação) que compõem o
    resultado de um AE/líder no período — mesma base do /overview e /forecast.
    `kind` recorta o card clicado: 'acq' (aquisição), 'exp' (expansão) ou
    'newlogo' (new logos = aquisições ganhas por AEs com meta)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    lo, hi = _forecast_window(request.args)
    deals = _get_forecast_deals()
    hold_ids = _identify_hold_deal_ids(deals)

    # ESPELHA _overview_page: exclui AEs desligados, recorta região, e só DEPOIS
    # atribui o líder pelo quarter do PO (_attr_deal_leader) pra filtrar
    # ALLOWED_LEADERS. Filtrar pelo líder cru dropava deals do quarter anterior e
    # a contagem não batia com os cards (ex.: expansão 14 vs 19).
    #
    # scope=all (Sales Overtime): SEM recorte de roster — a série trimestral conta
    # a venda de QUALQUER pessoa (inclui desligados e líderes fora do time atual)
    # e não separa hold; o drill precisa da mesma régua pra bater com as barras.
    scope_all = request.args.get("scope") == "all"
    dd = deals.copy()
    if not scope_all:
        dd = dd[~dd["account_executive"].isin(DEPARTED_AES)]
    _region = request.args.get("region")
    if _region:
        dd = dd[dd["region"] == _region]
    # Data que decide se o deal está no período. Receita (Realizado, Total em
    # aquisição/expansão) → `rev_po`, com a extensão de fechamento; contagens e
    # ticket (kind=ticket/newlogo, o acq do Overtime, que é new logo, e
    # basis=cal dos cards de ticket) → o PO no calendário normal. Precisa bater
    # com o card/barra que abriu o drill.
    kind = (request.args.get("kind") or "").strip()
    use_cal = (kind in ("ticket", "newlogo") or (kind == "acq" and scope_all)
               or request.args.get("basis") == "cal")
    dcol = "po_emission_date" if use_cal else "rev_po"
    in_win = dd["po_emission_date"].notna() & (dd[dcol] >= lo) & (dd[dcol] <= hi)
    if scope_all:
        won = dd[dd["is_won"] & in_win].copy()
        hld = dd.iloc[0:0].copy()
    else:
        won = dd[dd["is_won"] & in_win & ~dd["deal_id"].isin(hold_ids)].copy()
        hld = dd[in_win & dd["deal_id"].isin(hold_ids)].copy()
    lmap = _period_leader_map()
    won["leader"] = _attr_deal_leader(won, dcol, lmap)
    hld["leader"] = _attr_deal_leader(hld, dcol, lmap)
    if not scope_all:
        won = won[won["leader"].isin(ALLOWED_LEADERS)]
        hld = hld[hld["leader"].isin(ALLOWED_LEADERS)]
    _leader = request.args.get("leader")
    if _leader:
        _scope = expand_leader(_leader)
        won = won[won["leader"].isin(_scope)]
        hld = hld[hld["leader"].isin(_scope)]
    # Entidade clicada (barra de AE/líder). Sem `name` → total do escopo.
    if name:
        won = _filter_entity(won, name, grain)
        hld = _filter_entity(hld, name, grain)
    # Filtro de AE da tela por cima do clique (líder e região já foram acima,
    # com o líder do trimestre do PO).
    _ae = (request.args.get("ae") or "").strip()
    if _ae:
        won = won[won["account_executive"] == _ae]
        hld = hld[hld["account_executive"] == _ae]

    def _mk(frame, status):
        out = []
        for _, r in frame.iterrows():
            _tag = "" if use_cal else _rev_tag(r["po_emission_date"])
            out.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                        "ae": r["account_executive"], "deal_type": r.get("deal_type") or "—",
                        "amount": float(r["amount"] or 0),
                        "status": f"{status} · {_tag}" if _tag else status,
                        "data": r["po_emission_date"].date().isoformat() if pd.notna(r["po_emission_date"]) else ""})
        return out

    # Ticket médio (Sales Overtime): TODOS os deals ganhos do trimestre, com o
    # tipo — a barra de aquisição e a de expansão saem da mesma população.
    if kind == "ticket":
        _tipo = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}
        rows = []
        for _, r in (pd.concat([won, hld], ignore_index=True) if len(hld) else won).iterrows():
            rows.append({
                "deal_name": r.get("deal_name") or "—",
                "deal_url": _hs_deal_url(r.get("deal_id")),
                "ae": r["account_executive"],
                "tipo": _tipo.get(r.get("deal_type"), r.get("deal_type") or "—"),
                "amount": float(r["amount"] or 0),
                "data": r["po_emission_date"].date().isoformat() if pd.notna(r["po_emission_date"]) else "",
            })
        rows.sort(key=lambda x: x["amount"], reverse=True)
        _acq = [x["amount"] for x in rows if x["tipo"] == "Aquisição"]
        _exp = [x["amount"] for x in rows if x["tipo"] == "Expansão"]
        _tk = lambda v: fmt_money(sum(v) / len(v)) if v else "—"
        return jsonify({
            "title": "Ticket médio · deals ganhos" + (f" · {name}" if name else ""),
            "subtitle": (f"{len(rows)} deals · ticket aquisição {_tk(_acq)} ({len(_acq)} deals) · "
                         f"ticket expansão {_tk(_exp)} ({len(_exp)} deals)"),
            "columns": [
                {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
                {"key": "ae",        "label": "AE"},
                {"key": "tipo",      "label": "Tipo"},
                {"key": "amount",    "label": "MRR",   "type": "money"},
                {"key": "data",      "label": "PO em", "type": "date"},
            ],
            "rows": rows,
        })

    # Recorte do card clicado. acq/exp/newlogo saem SÓ do `won` (validado, sem
    # hold) — mesma base de _stats()/newlogos_actual, pra bater com os cards.
    if kind in ("acq", "exp", "newlogo"):
        _dtype = "Up & cross sell" if kind == "exp" else "Aquisition"
        sel = won[won["deal_type"] == _dtype]
        # Hold conta como venda (regra do Overview): entra com o selo dele.
        sel_h = hld[hld["deal_type"] == _dtype]
        if kind == "newlogo":
            carriers = _newlogo_goal_aes(lo.date(), hi.date(),
                                         region=request.args.get("region"),
                                         leader=request.args.get("leader"),
                                         ae=(name if grain == "ae" else None))
            sel = sel[sel["account_executive"].isin(carriers)]
            sel_h = sel_h[sel_h["account_executive"].isin(carriers)]
        rows = _mk(sel, "Validado") + _mk(sel_h, "Em validação financeira")
        rows.sort(key=lambda x: x["data"], reverse=True)
        amount = float(sum(x["amount"] for x in rows))
        _titles = {"acq": "Deals de aquisição", "exp": "Deals de expansão",
                   "newlogo": "New logos"}
        cols = [{"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
                {"key": "ae",        "label": "AE"},
                {"key": "amount",    "label": "MRR",   "type": "money"},
                {"key": "data",      "label": "PO em", "type": "date"}]
        if len(sel_h) or any(" · " in x["status"] for x in rows):
            # Há deal cuja receita foi para outro quarter pela extensão: sem a
            # coluna, o "PO em 01/10" apareceria no Q3 sem explicação.
            cols.insert(3, {"key": "status", "label": "Status"})
        return jsonify({
            "title": _titles[kind] + (f" · {name}" if name else ""),
            "subtitle": _drill_money_subtitle(len(rows), amount) + " no período",
            "columns": cols, "rows": rows,
        })

    rows = _mk(won, "Validado") + _mk(hld, "Em validação financeira")
    rows.sort(key=lambda x: x["data"], reverse=True)
    amount = float(sum(x["amount"] for x in rows))
    return jsonify({
        "title": name or "Overview — total (realizado)",
        "subtitle": _drill_money_subtitle(len(rows), amount) + " fechado no período",
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "deal_type", "label": "Tipo"},
            {"key": "amount",    "label": "MRR",    "type": "money"},
            {"key": "status",    "label": "Status"},
            {"key": "data",      "label": "PO em",  "type": "date"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


@app.route("/leaders/demand")
def leaders_demand():
    """Geração de Demanda agregada por gestor (Diretor + Gerente).
    Cada barra = soma das opps geradas pelos AEs daquele líder.
    Mesmas premissas e UX do /demand (per-AE), mas barras são líderes."""
    ctx = base_context()
    f = ctx["filters"]

    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else date(2026, 4, 1)
    to_d   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else date(2027, 1, 31)

    force = request.args.get("refresh") == "1"
    grain = request.args.get("grain", "month")
    if grain not in ("week", "month", "quarter"): grain = "month"

    deals = _get_demand_deals(force=force)
    by_ae = _demand_by_ae_pd(deals, from_d, to_d)
    grain_raw = _demand_by_grain_pd(deals, from_d, to_d, grain)

    by_ae = apply_leader_overrides(by_ae)
    by_ae = by_ae[by_ae["leader"].isin(ALLOWED_LEADERS)]
    by_ae = by_ae[~by_ae["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]

    if f["region"]: by_ae = by_ae[by_ae["region"] == f["region"]]
    if f["leader"]: by_ae = by_ae[by_ae["leader"].isin(expand_leader(f["leader"]))]
    # Filtro de AE da barra (antes ignorado aqui: a barra do líder seguia com o
    # time inteiro enquanto a explosão já recortava o AE).
    if f["ae"]:     by_ae = by_ae[by_ae["account_executive"] == f["ae"]]

    # AGREGAÇÃO POR LÍDER — taxonomia organizacional:
    # Diretor = AEs diretos + AEs do(s) gerente(s) que respondem a ele
    # Gerente = só AEs diretos
    # Filtro de líder respeita expand_leader (mostra subordinados também)
    weekly_goal_per_ae = _weekly_ae_goal(from_d, to_d)
    leaders_to_show = (expand_leader(f["leader"]) if f.get("leader")
                       else set(LEADER_ORDER) - {"Pedro Mendes"})
    rows = []
    for L in [x for x in LEADER_ORDER if x != "Pedro Mendes" and x in leaders_to_show]:
        subs = expand_leader(L)  # L + subordinados transitivos
        subset = by_ae[by_ae["leader"].isin(subs)]
        if len(subset) == 0:
            continue
        team_size = int(subset["account_executive"].nunique())
        rows.append({
            "leader": L,
            "account_executive": L,
            "region": "BR",
            "role": LEADER_ROLE.get(L, "—"),
            "team_size": team_size,
            "goal": team_size * weekly_goal_per_ae,
            "auto_acq":     int(subset["auto_acq"].sum()),
            "auto_exp":     int(subset["auto_exp"].sum()),
            "rcv_inbound":   int(subset["rcv_inbound"].sum()),
            "rcv_outbound":  int(subset["rcv_outbound"].sum()),
            "rcv_moved_out": int(subset["rcv_moved_out"].sum()),
            "rcv_moved_in":  int(subset["rcv_moved_in"].sum()),
            "rcv_exp":       int(subset["rcv_exp"].sum()),
        })
    by_leader = pd.DataFrame(rows)
    if len(by_leader):
        by_leader["auto_total"] = by_leader["auto_acq"] + by_leader["auto_exp"]
        by_leader["rcv_outbound_all"] = by_leader["rcv_outbound"] + by_leader["rcv_moved_out"]
        by_leader["rcv_inbound_all"]  = by_leader["rcv_inbound"]  + by_leader["rcv_moved_in"]
        by_leader["rcv_total"]  = (by_leader["rcv_inbound_all"] + by_leader["rcv_outbound_all"]
                                   + by_leader["rcv_exp"])
        by_leader["total_generated"] = by_leader["auto_total"] + by_leader["rcv_total"]
        by_leader["pct_auto"]   = by_leader.apply(
            lambda r: (r["auto_total"] / r["total_generated"] * 100) if r["total_generated"] else 0,
            axis=1,
        )
        by_leader = by_leader.sort_values("total_generated", ascending=False).reset_index(drop=True)
    else:
        by_leader = pd.DataFrame(columns=[
            "leader","account_executive","region","role","team_size","goal",
            "auto_acq","auto_exp","rcv_inbound","rcv_outbound",
            "rcv_moved_out","rcv_moved_in","rcv_exp",
            "rcv_inbound_all","rcv_outbound_all",
            "auto_total","rcv_total","total_generated"
        ])

    # Y do chart 1 escala para incluir a maior meta (caso ninguém tenha batido).
    # NaN-safe: .max() em série vazia retorna NaN e NaN é truthy.
    _auto_max = by_leader["auto_total"].max() if len(by_leader) else 0
    _goal_max = by_leader["goal"].max()       if len(by_leader) else 0
    _rcv_max  = by_leader["rcv_total"].max()  if len(by_leader) else 0
    max_auto = max(int(_auto_max) if pd.notna(_auto_max) else 0,
                   int(_goal_max) if pd.notna(_goal_max) else 0,
                   1)
    max_rcv  = max(int(_rcv_max) if pd.notna(_rcv_max) else 0, 1)

    totals = {
        "n_ae": len(by_leader),
        "total_generated": int(by_leader["total_generated"].sum()),
        "auto_total":  int(by_leader["auto_total"].sum()),
        "rcv_total":   int(by_leader["rcv_total"].sum()),
        "acq_total":   int(by_leader["auto_acq"].sum() + by_leader["rcv_inbound_all"].sum() + by_leader["rcv_outbound_all"].sum()),
        "exp_total":   int(by_leader["auto_exp"].sum() + by_leader["rcv_exp"].sum()),
        "auto_acq":    int(by_leader["auto_acq"].sum()),
        "auto_exp":    int(by_leader["auto_exp"].sum()),
        "rcv_inbound": int(by_leader["rcv_inbound_all"].sum()),
        "rcv_outbound":int(by_leader["rcv_outbound_all"].sum()),
        "rcv_exp":     int(by_leader["rcv_exp"].sum()),
        "rcv_moved_out": int(by_leader["rcv_moved_out"].sum()),
        "rcv_moved_in":  int(by_leader["rcv_moved_in"].sum()),
    }

    # Trend — aplica os mesmos filtros das barras (líder hierárquico, AE)
    trend_pivot = []
    if len(grain_raw):
        gr = grain_raw.copy()
        gr = apply_leader_overrides(gr)
        gr = gr[gr["leader"].isin(ALLOWED_LEADERS)]
        gr = gr[~gr["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
        if f["region"]:
            gr = gr[gr["region"] == f["region"]]
        if f["leader"]:
            gr = gr[gr["leader"].isin(expand_leader(f["leader"]))]
        if f["ae"]:
            gr = gr[gr["account_executive"] == f["ae"]]
        gr["bucket"] = gr["source"] + "_" + gr["kind"]
        gp = gr.pivot_table(index="period", columns="bucket", values="n",
                            aggfunc="sum").fillna(0).reset_index()
        for col in ["auto_acq","auto_exp","received_inbound","received_outbound",
                    "received_moved_out","received_moved_in","received_exp"]:
            if col not in gp.columns: gp[col] = 0
        gp["total"] = (gp["auto_acq"] + gp["auto_exp"]
                       + gp["received_inbound"] + gp["received_outbound"]
                       + gp["received_moved_out"] + gp["received_moved_in"]
                       + gp["received_exp"])
        gp["period"] = pd.to_datetime(gp["period"])

        def _grain_label(d, g=grain):
            d = d.date() if hasattr(d, "date") else d
            if g == "week":    return f"{d.day:02d}/{d.month:02d}"
            if g == "quarter": return f"Q{(d.month-1)//3 + 1}/{d.year % 100:02d}"
            return f"{PT_MONTHS_SHORT[d.month-1]}/{d.year % 100:02d}"
        gp["label"] = gp["period"].apply(_grain_label)

        # Intervalo [período_from, período_to] de cada barra — pro drill-down do
        # total explodir só os deals daquele período (semana/mês/trimestre).
        def _period_end(d, g=grain):
            d = d.date() if hasattr(d, "date") else d
            if g == "week":
                return d + timedelta(days=6)
            if g == "quarter":
                q_first_month = ((d.month - 1) // 3) * 3 + 1
                nxt = date(d.year + (q_first_month + 3 > 12), (q_first_month + 3 - 1) % 12 + 1, 1)
                return nxt - timedelta(days=1)
            nxt = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
            return nxt - timedelta(days=1)
        gp["period_from"] = gp["period"].apply(lambda d: d.date().isoformat())
        gp["period_to"] = gp["period"].apply(lambda d: _period_end(d).isoformat())
        trend_pivot = gp.sort_values("period").to_dict("records")

    grain_args = {k: v for k, v in request.args.items() if k not in ("refresh", "grain") and v}
    grain_urls = {
        g: request.path + "?" + urllib.parse.urlencode({**grain_args, "grain": g})
        for g in ("week", "month", "quarter")
    }

    return render_template(
        "leaders_demand.html", ctx=ctx,
        by_ae=by_leader.to_dict("records"),  # template var name
        trend=trend_pivot,
        grain=grain, grain_urls=grain_urls,
        totals=totals,
        max_auto=max_auto, max_rcv=max_rcv,
        weekly_goal_per_ae=weekly_goal_per_ae,
        range_from=from_d.isoformat(), range_to=to_d.isoformat(),
    )


@app.route("/sdr/effort")
def sdr_effort():
    """Pré Sales BR — placeholder. A aba existe no dashbar mas não está
    conectada a nenhuma fonte de dados ativa no momento."""
    ctx = base_context()
    return render_template("sdr_effort.html", ctx=ctx)


@app.route("/pipeline")
def pipeline():
    return _pipeline_page("ae")


@app.route("/leaders/pipeline")
def leaders_pipeline():
    return _pipeline_page("leader")


def _entity_only_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    """Só os filtros de entidade (region/leader/ae/role), SEM data. Usado por
    gráfico que tem janela própria e não pode herdar o recorte de data da página
    (o `ctx['df']` já vem cortado por quarter pelo `apply_filters`)."""
    if f["region"]: df = df[df["region"] == f["region"]]
    if f["leader"]: df = df[df["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:     df = df[df["account_executive"] == f["ae"]]
    if f["role"]:   df = df[df["role"] == f["role"]]
    return df


def _quota_by_ae_for_window(df: pd.DataFrame, cov_from: date, cov_to: date,
                            f: dict) -> pd.DataFrame:
    """Quota por AE numa janela. Janela dentro de um trimestre com roster: a verdade
    é o roster dele (inclui
    AE novo sem linha curada, que senão ficaria com quota 0 e sumiria do chart);
    fora dela soma a curada nos quarters que intersectam a janela."""
    rq = _roster_window(cov_from, cov_to)
    if rq is not None:
        scope = expand_leader(f["leader"]) if f["leader"] else None
        rows = []
        for name, v in _roster_of(rq).items():
            if f["region"] and (_roster_region() != f["region"]):
                continue
            if scope is not None and v["leader"] not in scope:
                continue
            if f["ae"] and name != f["ae"]:
                continue
            rows.append({"account_executive": name, "quota_brl": float(v["quota"]),
                         "leader": v["leader"], "region": _roster_region()})
        return pd.DataFrame(rows, columns=["account_executive", "quota_brl", "leader", "region"])
    df_q = (df[(df["quarter"] <= cov_to) & (df["quarter_end"] >= cov_from)]
            if "quarter_end" in df.columns else df)
    out = (df_q.groupby("account_executive", dropna=False)
               .agg(quota_brl=("quota_in_brl", "sum")).reset_index())
    meta = (df.sort_values("quarter")
              .drop_duplicates("account_executive", keep="last")
              [["account_executive", "leader", "region"]])
    return out.merge(meta, on="account_executive", how="left")


def _validated_hold_by_ae(fdeals, hold_ids, lmap, lo, hi, f):
    """Realizado VALIDADO e ON-HOLD por AE numa janela, deal-level — mesma lógica
    do /overview (validado = won+PO fora do hold; hold = em aprovação; ambos pelo
    quarter do PO). Devolve (validated, hold, acq, exp) como Series por AE.
    Tudo aqui é RECEITA: janela e líder pela data da receita (`rev_po`, com a
    extensão de fechamento), que é o que abate a quota remanescente."""
    dd = fdeals[~fdeals["account_executive"].isin(DEPARTED_AES)].copy()
    if f["region"]:
        dd = dd[dd["region"] == f["region"]]
    if f["ae"]:
        dd = dd[dd["account_executive"] == f["ae"]]
    po = dd["rev_po"]
    wonv = dd[dd["is_won"] & po.notna() & ~dd["deal_id"].isin(hold_ids)
              & (po >= lo) & (po <= hi)].copy()
    hldv = dd[dd["deal_id"].isin(hold_ids) & po.notna()
              & (po >= lo) & (po <= hi)].copy()
    wonv["leader"] = _attr_deal_leader(wonv, "rev_po", lmap)
    hldv["leader"] = _attr_deal_leader(hldv, "rev_po", lmap)
    wonv = wonv[wonv["leader"].isin(ALLOWED_LEADERS)]
    hldv = hldv[hldv["leader"].isin(ALLOWED_LEADERS)]
    if f["leader"]:
        sc = expand_leader(f["leader"])
        wonv = wonv[wonv["leader"].isin(sc)]
        hldv = hldv[hldv["leader"].isin(sc)]
    g = lambda x: x.groupby("account_executive")["amount"].sum()
    return (g(wonv), g(hldv),
            g(wonv[wonv["deal_type"] == "Aquisition"]),
            g(wonv[wonv["deal_type"] != "Aquisition"]))


def _pipeline_page(grain="ae"):
    """Cobertura de pipeline por AE (grain='ae') ou por LÍDER (grain='leader',
    cada barra/linha = soma do time via expand_leader).
    - Pipeline = deals ABERTOS no funil Sales BR; 2 buckets (por dealstage_id):
        early = Scoping (93878633)
        late  = Negotiation + Decision + Procurement
    - Filtro de período do dashboard aplica em hs_closedate (Close Date do
      HubSpot — quando o AE espera fechar). Sem filtro = todo pipe atual.
    - Quota / MRR vendido / on-hold = filtrados pelo mesmo período.
    - remaining_quota = quota - new_mrr - on_hold
    - cobertura = pipeline / remaining_quota (em multiplicador Nx)."""
    ctx = base_context()
    f = ctx["filters"]
    df = ctx["df"]
    force = request.args.get("refresh") == "1"

    # Resolve período pra filtro do close date.
    pipe_from = pd.to_datetime(f["date_from"]).date() if f["date_from"] else None
    pipe_to   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else None

    # ---- Pipeline aberto, filtrado por hs_closedate dentro do período
    deals = _get_pipeline_deals(force=force)
    pipe_by_ae = _pipeline_by_ae_pd(deals, pipe_from, pipe_to)
    pipe_by_ae = pipe_by_ae[pipe_by_ae["leader"].isin(ALLOWED_LEADERS)]
    pipe_by_ae = pipe_by_ae[~pipe_by_ae["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f["region"]:
        pipe_by_ae = pipe_by_ae[pipe_by_ae["region"] == f["region"]]
    if f["leader"]:
        pipe_by_ae = pipe_by_ae[pipe_by_ae["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        pipe_by_ae = pipe_by_ae[pipe_by_ae["account_executive"] == f["ae"]]

    # ---- Janela da cobertura = TRIMESTRE VIGENTE por padrão. Cobertura olha
    # pra frente: quanto da quota DESTE trimestre ainda falta. Se o usuário escolher
    # um período, respeita; senão o trimestre de hoje. (Sem isso, o df cai em TODOS
    # os quarters e a quota/realizado somam o histórico inteiro → remaining ~0 →
    # cobertura estoura.) Até 08/10/2026 isto era o Q3 FIXO: a tela sem filtro
    # seguia medindo a cobertura contra a quota do Q3 já no Q4.
    cov_from = pipe_from or _quarter_start_of(_today())
    cov_to   = pipe_to   or _quarter_end_of(_today())
    _lo, _hi = pd.to_datetime(cov_from), pd.to_datetime(cov_to)

    # ---- Realizado VALIDADO + HOLD por AE, DEAL-LEVEL — MESMA lógica do /overview
    # (validado = won+PO FORA do hold; hold = em aprovação financeira; ambos pelo
    # quarter do PO, recortados na janela). Substitui o new_mrr_in_brl/
    # total_mrr_on_hold_brl CRU da curada (que arrastava deal de quarter errado /
    # double-count — bug do Dantt).
    _fdeals = _get_forecast_deals(force=force)
    _hold_ids = _identify_hold_deal_ids(_fdeals, force=force)
    _lmap = _period_leader_map(force=force)
    # split do validado por tipo (p/ a tabela de detalhe): aquisição vs expansão
    validated_by_ae, hold_by_ae, acq_by_ae, exp_by_ae = _validated_hold_by_ae(
        _fdeals, _hold_ids, _lmap, _lo, _hi, f)

    # ---- Quota por AE. MESMA fonte do /overview: na janela de um trimestre com
    # roster, a verdade é o roster dele (inclui AEs novos SEM linha curada — ex Breno/Diego/Gabriel — que de
    # outro modo ficariam com quota 0 e sumiriam do chart). Fora do Q3, soma a curated
    # nos quarters que intersectam a janela (recorte evita somar TODO o histórico).
    quota_by_ae = _quota_by_ae_for_window(df, cov_from, cov_to, f)

    # ---- Cruzamento: union de AEs (com pipeline ou com quota no período)
    by_ae = quota_by_ae.merge(
        pipe_by_ae[["account_executive", "early_amount", "late_amount", "early_deals", "late_deals"]],
        on="account_executive", how="outer")
    # Preenche cols pra AEs que aparecem só no pipeline (sem quota no período)
    for c in ("quota_brl", "early_amount", "late_amount"):
        by_ae[c] = pd.to_numeric(by_ae[c], errors="coerce").fillna(0.0)
    for c in ("early_deals", "late_deals"):
        by_ae[c] = pd.to_numeric(by_ae[c], errors="coerce").fillna(0).astype(int)
    # Backfill leader/region a partir do pipe quando vier vazio do quota_by_ae
    if len(pipe_by_ae):
        pipe_meta = pipe_by_ae.set_index("account_executive")[["leader", "region"]]
        by_ae = by_ae.set_index("account_executive")
        by_ae["leader"] = by_ae["leader"].fillna(pipe_meta["leader"])
        by_ae["region"] = by_ae["region"].fillna(pipe_meta["region"])
        by_ae = by_ae.reset_index()

    # ---- remaining_quota = quota − VALIDADO − HOLD (deal-level, como no /overview).
    # A cobertura (multiplicador) é calculada por estágio, RAW, em _cov_items.
    by_ae["validated"] = by_ae["account_executive"].map(validated_by_ae).fillna(0.0)
    by_ae["hold"]      = by_ae["account_executive"].map(hold_by_ae).fillna(0.0)
    by_ae["acq"]       = by_ae["account_executive"].map(acq_by_ae).fillna(0.0)
    by_ae["exp"]       = by_ae["account_executive"].map(exp_by_ae).fillna(0.0)
    by_ae["remaining_quota"] = (by_ae["quota_brl"] - by_ae["validated"] - by_ae["hold"]).clip(lower=0)

    # Grain: 'ae' usa o by_ae direto; 'leader' agrega por líder (soma do time).
    if grain == "leader":
        rank_src = _pipe_leader_df(by_ae, f["leader"],
                                   targets=_leader_targets(_roster_window(cov_from, cov_to)))
        pipe_path, drill_key = "/leaders/pipeline", "leader"
    else:
        # Líder sem quota no trimestre (Arthur no Q4) não é barra de vendedor: o
        # funil dele segue no total e no grain de líder, como o dos outros gestores.
        rank_src = by_ae[~by_ae["account_executive"].isin(
            _leaders_without_quota(_roster_window(cov_from, cov_to)))]
        pipe_path, drill_key = "/pipeline", "ae"

    # 2 populações: AEs/líderes com pipe early / late no período (barra amount > 0).
    by_ae_early = rank_src[rank_src["early_amount"] > 0].reset_index(drop=True)
    by_ae_late  = rank_src[rank_src["late_amount"]  > 0].reset_index(drop=True)

    coverage_goal_x = 1.0   # limiar do verde no Early (Late usa a meta dinâmica)

    # ---- Charts de cobertura (combo bar+line — MESMO componente do /overview):
    # verde = amount em cobertura (pipeline do estágio); cinza = quota remanescente;
    # linha = multiplicador de cobertura = amount ÷ remaining (RAW). Ordena por
    # cobertura DESC; AE que já bateu meta (remaining=0) fica sem linha e vai pro fim.
    _cov_drill = {k: v for k, v in (("date_from", f["date_from"]), ("date_to", f["date_to"]),
                                    ("leader", f["leader"]), ("region", f["region"])) if v}

    def _cov_items(rows_df, amt_col):
        items = []
        for _, r in rows_df.iterrows():
            rem = float(r["remaining_quota"])
            amt = float(r[amt_col])
            lead = r["leader"] if isinstance(r["leader"], str) else ""
            items.append({
                "name": r["account_executive"],
                "short": short_name(r["account_executive"]),
                "leader": lead,
                "realized": amt,                                 # verde = amount em cobertura
                "quota": rem,                                    # cinza = quota remanescente
                "attainment": (amt / rem if rem > 0 else None),  # linha = cobertura Nx (raw)
            })
        items.sort(key=lambda it: (it["attainment"] is not None,
                                   it["attainment"] if it["attainment"] is not None else -1.0),
                   reverse=True)
        return items

    # Meta DINÂMICA de cobertura Late (progressão semanal do trimestre: 2x→4x).
    # Vira o limiar do verde (hit) E uma linha tracejada de meta. Só no chart Late.
    late_week, late_goal = _late_coverage_goal()
    late_goal_label = "Meta S" + str(late_week) + " · " + _fmt_mult(late_goal)

    # ---- EARLY: a META vem do ciclo (rampa 2,5x→5x sobre M3+M1+M2), mas a JANELA
    # de close date segue o filtro da página — é o que permite perguntar "quanto de
    # pipe early tenho pro Q4" e "pro Q1", isolados. Sem filtro, cai no trimestre-
    # alvo do próprio ciclo.
    # O denominador acompanha o trimestre olhado: trimestre corrente → quota
    # REMANESCENTE (já vendeu parte); trimestre futuro → quota CHEIA (não vendeu
    # nada dele ainda). Generaliza a regra do M3.
    early = _early_coverage_goal()
    if pipe_from and pipe_to:
        e_from, e_to = pipe_from, pipe_to
    else:
        e_from, e_to = early["close_from"], early["close_to"]
    early = {**early, "close_from": e_from, "close_to": e_to,
             "use_full_quota": _quarter_start_of(e_from) > _quarter_start_of(_today())}

    e_pipe = _pipeline_by_ae_pd(deals, e_from, e_to)
    e_pipe = e_pipe[e_pipe["leader"].isin(ALLOWED_LEADERS)]
    e_pipe = e_pipe[~e_pipe["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f["region"]:
        e_pipe = e_pipe[e_pipe["region"] == f["region"]]
    if f["leader"]:
        e_pipe = e_pipe[e_pipe["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        e_pipe = e_pipe[e_pipe["account_executive"] == f["ae"]]

    # Quota do trimestre-alvo: parte do df SEM recorte de data (o ctx['df'] já vem
    # cortado pelo filtro da página, que pode nem conter o trimestre-alvo).
    _df_nodate = _entity_only_filters(_get_clean_ae_perf(force=force), f)
    e_quota = _quota_by_ae_for_window(_df_nodate, e_from, e_to, f)
    e_by_ae = e_quota.merge(
        e_pipe[["account_executive", "early_amount", "early_deals"]],
        on="account_executive", how="outer")
    e_by_ae["quota_brl"]    = pd.to_numeric(e_by_ae["quota_brl"], errors="coerce").fillna(0.0)
    e_by_ae["early_amount"] = pd.to_numeric(e_by_ae["early_amount"], errors="coerce").fillna(0.0)
    if len(e_pipe):
        _em = e_pipe.set_index("account_executive")[["leader", "region"]]
        e_by_ae = e_by_ae.set_index("account_executive")
        e_by_ae["leader"] = e_by_ae["leader"].fillna(_em["leader"])
        e_by_ae["region"] = e_by_ae["region"].fillna(_em["region"])
        e_by_ae = e_by_ae.reset_index()

    if early["use_full_quota"]:
        # M3: quota cheia do trimestre-alvo — nada validado ainda pra descontar.
        e_by_ae["validated"] = 0.0
        e_by_ae["hold"] = 0.0
    else:
        e_val, e_hld, _, _ = _validated_hold_by_ae(
            _fdeals, _hold_ids, _lmap, pd.to_datetime(e_from), pd.to_datetime(e_to), f)
        e_by_ae["validated"] = e_by_ae["account_executive"].map(e_val).fillna(0.0)
        e_by_ae["hold"]      = e_by_ae["account_executive"].map(e_hld).fillna(0.0)
    e_by_ae["remaining_quota"] = (
        e_by_ae["quota_brl"] - e_by_ae["validated"] - e_by_ae["hold"]).clip(lower=0)
    # Colunas que o rollup por líder exige mas não fazem sentido aqui (o chart é
    # só Early): zeradas pra reusar `_pipe_leader_df` sem duplicar a agregação.
    for _c in ("late_amount", "acq", "exp"):
        e_by_ae[_c] = 0.0
    e_by_ae["early_deals"] = pd.to_numeric(
        e_by_ae.get("early_deals"), errors="coerce").fillna(0).astype(int)
    e_by_ae["late_deals"] = 0

    if grain == "leader":
        # Meta do líder = Comp. Target do trimestre-alvo do Early (no M3 é o
        # próximo); trimestre sem Comp. Target declarado soma a quota do time.
        e_rank_src = _pipe_leader_df(e_by_ae, f["leader"],
                                     targets=_leader_targets(_roster_window(e_from, e_to)))
    else:
        e_rank_src = e_by_ae[~e_by_ae["account_executive"].isin(
            _leaders_without_quota(_roster_window(e_from, e_to)))]
    e_rows = e_rank_src[e_rank_src["early_amount"] > 0].reset_index(drop=True)

    early_goal_label = "Meta S" + str(early["week"]) + " · " + _fmt_mult(early["goal"])
    # Drill do Early usa a janela DELE, não a da página.
    _early_drill = {k: v for k, v in (("date_from", e_from.isoformat()),
                                      ("date_to", e_to.isoformat()),
                                      ("leader", f["leader"]), ("region", f["region"])) if v}

    cov_early = (_build_rank_chart(_cov_items(e_rows, "early_amount"),
                                   _early_drill, axis_mode="currency", line_mode="multiplier",
                                   line_goal=early["goal"], drill_path=pipe_path,
                                   drill_key=drill_key, goal_line=early["goal"])
                 if len(e_rows) else None)
    cov_late = (_build_rank_chart(_cov_items(by_ae_late, "late_amount"),
                                  _cov_drill, axis_mode="currency", line_mode="multiplier",
                                  line_goal=late_goal, drill_path=pipe_path, drill_key=drill_key,
                                  goal_line=late_goal)
                if len(by_ae_late) else None)

    return render_template(
        "pipeline.html", ctx=ctx,
        cov_early=cov_early, cov_late=cov_late,
        coverage_goal_x=coverage_goal_x,
        late_goal_label=late_goal_label, late_week=late_week, late_goal=late_goal,
        early=early, early_goal_label=early_goal_label,
        early_period=f"{_fmt_date_short(e_from)} → {_fmt_date_short(e_to)}",
        grain=grain,
    )


@app.route("/pipeline/stalled")
def pipeline_stalled():
    return _pipeline_stalled_page("ae")


@app.route("/leaders/pipeline/stalled")
def leaders_pipeline_stalled():
    return _pipeline_stalled_page("leader")


@app.route("/pipeline/stalled-activity")
def pipeline_activity():
    return _pipeline_activity_page("ae")


@app.route("/leaders/pipeline/stalled-activity")
def leaders_pipeline_activity():
    return _pipeline_activity_page("leader")


def _wa_status(force: bool = False) -> dict:
    """Frescor da carga de WhatsApp pra tela de Inatividade. `stale` = a última
    carga tem mais de 8 dias. A rotina é SEMANAL (domingo 12h, pedido do Pedro
    em 08/10/2026 — "pra segunda termos o dado correto") e roda no Mac dele;
    se uma segunda passar sem rodar, a tela avisa. No meio da semana o
    WhatsApp está até 7 dias atrás por desenho, e a frase "lido até" já diz isso."""
    out = {"active": False, "stale": False, "until": None, "until_label": ""}
    try:
        fr = bq.wa_freshness(force_refresh=force)
    except Exception as e:  # noqa: BLE001 — sem a tabela, a aba segue com email+call
        app.logger.warning("wa_freshness indisponível: %s", e)
        return out
    if fr is None or not len(fr):
        return out
    until = pd.to_datetime(fr.iloc[0]["notes_until"], errors="coerce", utc=True)
    if pd.isna(until):
        return out
    age_h = (pd.Timestamp.now(tz="UTC") - until).total_seconds() / 3600
    loc = until.tz_convert(BRT)
    out.update(active=True, stale=age_h > 8 * 24, until=until,
               until_label=f"{loc:%d/%m} {loc:%H:%M}")
    return out


def _gong_status(force: bool = False) -> dict:
    """Frescor das reuniões do Gong pra tela de Inatividade. Até 29/09/2026 elas
    vêm da cópia da nekt; dali em diante, da carga semanal pelo conector
    (tools/gong_ingest.py, na mesma rotina de domingo do WhatsApp). `until` é o
    último dia carregado SEM buraco desde 29/09; `stale` = mais de 8 dias atrás
    (uma rotina de domingo que não rodou)."""
    out = {"active": False, "stale": False, "until_label": ""}
    try:
        fr = bq.gong_freshness(force_refresh=force)
    except Exception as e:  # noqa: BLE001 — sem as tabelas, a aba segue sem o canal novo
        app.logger.warning("gong_freshness indisponível: %s", e)
        return out
    if fr is None or not len(fr):
        return out
    r = fr.iloc[0]
    until = pd.to_datetime(r["until_date"], errors="coerce")
    gap = pd.to_datetime(r["first_gap"], errors="coerce")
    if pd.notna(gap):
        until = gap - pd.Timedelta(days=1)
    if pd.isna(until):
        return out
    fim_do_dia = until.tz_localize("UTC") + pd.Timedelta(days=1)
    age_d = (pd.Timestamp.now(tz="UTC") - fim_do_dia).total_seconds() / 86400
    out.update(active=True, stale=age_d > 8, until_label=f"{until:%d/%m}")
    return out


# E-mail e ligação vêm de um sync da nekt que não é nosso. Sem atividade nova há
# mais que isto, a tela avisa: fim de semana prolongado ainda passa calado.
_EMAIL_CALL_STALE_DAYS = 3


def _email_call_status(force: bool = False) -> list[dict]:
    """Canais (e-mail, ligação) cujo sync parou — [] quando os dois estão em dia
    ou quando não deu pra medir (aí a tela segue sem o aviso, como antes)."""
    try:
        fr = bq.email_call_freshness(force_refresh=force)
    except Exception as e:  # noqa: BLE001
        app.logger.warning("email_call_freshness indisponível: %s", e)
        return []
    if fr is None or not len(fr):
        return []
    out = []
    for col, nome in (("email_until", "e-mail"), ("call_until", "ligação")):
        until = pd.to_datetime(fr.iloc[0][col], errors="coerce", utc=True)
        if pd.isna(until):
            continue
        dias = (pd.Timestamp.now(tz="UTC") - until).total_seconds() / 86400
        if dias > _EMAIL_CALL_STALE_DAYS:
            out.append({"nome": nome, "until_label": f"{until.tz_convert(BRT):%d/%m}"})
    return out


def _pipeline_activity_page(grain="ae"):
    """Inatividade por ATIVIDADE (V2): mesma cara da página de inatividade por
    stage, mas o eixo de tempo é 'dias desde o último RETORNO de um contato do
    deal' (email entrante / call conectada / WhatsApp) em vez de 'dias no stage'.
    Limite único de ACTIVITY_SLA_DAYS (7) em todos os estágios desde 09/10/2026 —
    antes reusava os SLAs por estágio do Stale. Desde a mesma data cobre também o
    Stage 1 - Scheduled e conta os deals dos gestores (no total e em Líderes).
    WhatsApp (fala do cliente nas notas do Sniper) conta como retorno desde 08/10/2026;
    reunião gravada no Gong com um contato do deal, desde 19/10/2026 (pedido do
    Pedro) — ver bq._activity_cte e sql/gong_meeting_contacts_live.sql."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # ----- 1) Estado atual (now) -----
    df_now = bq.pipeline_activity(force_refresh=force, pipeline=_pl())
    df_now = _apply_roster_current(df_now)
    df_now = df_now[df_now["leader"].isin(ALLOWED_LEADERS)]
    # Deal de gestor entra (total + visão de Líderes); só quem saiu fica fora.
    df_now = drop_departed_people(df_now)
    if f.get("region"):
        df_now = df_now[df_now["region"] == f["region"]]
    if f.get("leader"):
        df_now = df_now[df_now["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
        df_now = df_now[df_now["account_executive"] == f["ae"]]

    # WhatsApp já vem DENTRO da SQL (bq._activity_cte, canal 'whatsapp' lido de
    # pepeuhacker.wa_deal_returns) — então a página, o drill e a Movimentação
    # WoW contam o mesmo retorno. Até 08/10/2026 ele era somado aqui, via API do
    # HubSpot, só na página e só com token; sem token (o caso desde agosto) o
    # WhatsApp simplesmente não contava. A carga roda na rotina diária do
    # Claude (tools/wa_ingest.py); aqui só se mede quanto ela está em dia.
    wa = _wa_status(force)
    gong = _gong_status(force)
    sync_parado = _email_call_status(force)
    wa_active = wa["active"]
    _lw = (pd.to_datetime(df_now["last_whatsapp_at"], errors="coerce", utc=True)
           if "last_whatsapp_at" in df_now.columns else pd.Series(pd.NaT, index=df_now.index))
    _la = pd.to_datetime(df_now["last_activity_at"], errors="coerce", utc=True)
    # Deals cujo retorno MAIS RECENTE foi no WhatsApp (sem ele, contariam mais dias).
    n_wa_returns = int((_lw.notna() & (_lw >= _la)).sum()) if len(df_now) else 0
    _lg = (pd.to_datetime(df_now["last_gong_at"], errors="coerce", utc=True)
           if "last_gong_at" in df_now.columns else pd.Series(pd.NaT, index=df_now.index))
    # Deals cujo retorno mais recente foi uma reunião do Gong.
    n_gong_returns = int((_lg.notna() & (_lg >= _la)).sum()) if len(df_now) else 0

    # "Inativo por atividade" = dias sem retorno > threshold do stage
    df_now["is_stalled"] = df_now.apply(
        lambda r: bool(pd.notna(r["days_since_activity"])
                       and _ACTIVITY_THRESHOLDS.get(r["stage_curr"], 999) < r["days_since_activity"]),
        axis=1).astype(bool)
    stalled_now = df_now[df_now["is_stalled"]].copy()

    if len(stalled_now) > 0:
        ct = stalled_now.pivot_table(index="account_executive", columns="stage_curr",
                                     values="deal_id", aggfunc="count", fill_value=0)
    else:
        ct = pd.DataFrame(index=pd.Index([], name="account_executive"))
    for st in _ACTIVITY_STAGE_ORDER:
        if st not in ct.columns:
            ct[st] = 0
    ct["total"] = ct[_ACTIVITY_STAGE_ORDER].sum(axis=1)
    ct = ct.sort_values("total", ascending=False)

    _ae_lead = dict(zip(df_now["account_executive"], df_now["leader"]))
    _nkeys = [f"n_{_STALLED_STAGE_KEYS[s]}" for s in _ACTIVITY_STAGE_ORDER]
    rows_now = [{
        "account_executive": ae,
        "leader": _ae_lead.get(ae, ""),
        "total": int(ct.loc[ae, "total"]),
        **{f"n_{_STALLED_STAGE_KEYS[s]}": int(ct.loc[ae, s]) for s in _ACTIVITY_STAGE_ORDER},
    } for ae in ct.index]
    _mgrs = _nao_vendedores()
    n_mgr_stalled = int(stalled_now["account_executive"].isin(_mgrs).sum()) if len(stalled_now) else 0
    if grain == "leader":
        rows_now = _leader_rollup(rows_now, f.get("leader"), ["total"] + _nkeys)
        for r in rows_now:                      # o rollup soma em float ("201.0" na barra)
            for k in ["total"] + _nkeys:
                r[k] = int(r[k])
        rows_now.sort(key=lambda r: r["total"], reverse=True)
    else:
        # Gestor não vira barra de vendedor (regra de 24/08/2026), mas o deal
        # dele está no card de estágio e no Total — que somam por estágio, e
        # não as barras.
        rows_now = [r for r in rows_now if r["account_executive"] not in _mgrs]

    summary_by_stage = [{
        "key":   _STALLED_STAGE_KEYS[s],
        "label": _STALLED_STAGE_LABELS[s],
        "color": _STALLED_STAGE_COLORS[s],
        "ink":   _STALLED_STAGE_INKS[s],
        "threshold": _ACTIVITY_THRESHOLDS[s],
        "n_deals": int(ct[s].sum()) if len(ct) else 0,
    } for s in _ACTIVITY_STAGE_ORDER]

    max_total_now = max([r["total"] for r in rows_now], default=0)
    total_stalled = int(ct["total"].sum())   if len(ct) else 0

    # Cobertura: quantos deals têm retorno real de contato (vs. fallback stage)
    n_deals_total   = int(len(df_now))
    n_with_activity = int(df_now["has_activity"].sum()) if len(df_now) else 0

    # ----- 2) WoW (saiu vs entrou da inatividade por atividade) -----
    last_sun, prev_sun = _last_two_sundays()
    t_curr_iso = f"{last_sun.isoformat()} 15:00:00 UTC"
    t_prev_iso = f"{prev_sun.isoformat()} 15:00:00 UTC"
    dw = bq.pipeline_activity_wow(t_curr_iso, t_prev_iso, force_refresh=force, pipeline=_pl())
    dw = _apply_roster_current(dw)
    dw = dw[dw["leader"].isin(ALLOWED_LEADERS)]
    dw = drop_departed_people(dw)
    if f.get("region"):
        dw = dw[dw["region"] == f["region"]]
    if f.get("leader"):
        dw = dw[dw["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
        dw = dw[dw["account_executive"] == f["ae"]]

    def _stalled_at(stage, days):
        th = _ACTIVITY_THRESHOLDS.get(stage)
        # bool() + notna: deal que fechou na semana vem com dias NULOS; sem
        # isso a coluna virava 'object' e o ~ do pandas (bitwise em bool do
        # Python: ~False = -1) marcava TODO parado anterior como 'saiu'.
        return bool(th is not None and pd.notna(days) and days > th)

    dw["parado_curr"] = dw.apply(lambda r: _stalled_at(r["stage_curr"], r["days_curr"]), axis=1).astype(bool)
    dw["parado_prev"] = dw.apply(lambda r: _stalled_at(r["stage_prev"], r["days_prev"]), axis=1).astype(bool)
    dw_saiu   = dw[(dw["parado_prev"]) & (~dw["parado_curr"])]
    dw_entrou = dw[(~dw["parado_prev"]) & (dw["parado_curr"])]

    # Breakdown por estágio-âncora — mesmos chips/recompute da página Stale.
    saiu_stage   = _stale_wow_by_stage(dw_saiu, "stage_prev")
    entrou_stage = _stale_wow_by_stage(dw_entrou, "stage_curr")
    _skeys = [_STALLED_STAGE_KEYS[s] for s in _ACTIVITY_STAGE_ORDER]

    saiu_by_ae   = dw_saiu.groupby("account_executive")["deal_id"].count()
    entrou_by_ae = dw_entrou.groupby("account_executive")["deal_id"].count()
    _ae_lead_wow = dict(zip(dw["account_executive"], dw["leader"]))
    aes_wow = sorted(set(saiu_by_ae.index) | set(entrou_by_ae.index))
    rows_wow = [{
        "account_executive": ae,
        "leader": _ae_lead_wow.get(ae, ""),
        "saiu":   int(saiu_by_ae.get(ae, 0)),
        "entrou": int(entrou_by_ae.get(ae, 0)),
        "net":    int(saiu_by_ae.get(ae, 0)) - int(entrou_by_ae.get(ae, 0)),
        **{f"saiu_{k}": saiu_stage.get(ae, {}).get(k, 0) for k in _skeys},
        **{f"entrou_{k}": entrou_stage.get(ae, {}).get(k, 0) for k in _skeys},
    } for ae in aes_wow]
    if grain == "leader":
        _roll_keys = (["saiu", "entrou"] + [f"saiu_{k}" for k in _skeys]
                      + [f"entrou_{k}" for k in _skeys])
        rows_wow = _leader_rollup(rows_wow, f.get("leader"), _roll_keys)
        for r in rows_wow:
            for k in _roll_keys:
                r[k] = int(r[k])
            r["net"] = int(r["saiu"]) - int(r["entrou"])
    else:
        rows_wow = [r for r in rows_wow if r["account_executive"] not in _mgrs]
    rows_wow.sort(key=lambda r: r["net"], reverse=True)

    wow_y_max = max([max(r["saiu"], r["entrou"]) for r in rows_wow], default=1)

    return render_template(
        "pipeline_activity.html", ctx=ctx,
        rows_now=rows_now,
        summary_by_stage=summary_by_stage,
        max_total_now=max_total_now,
        total_stalled=total_stalled,
        n_deals_total=n_deals_total,
        n_with_activity=n_with_activity,
        wa_active=wa_active,
        n_wa_returns=n_wa_returns,
        wa=wa, sync_parado=sync_parado,
        gong=gong, n_gong_returns=n_gong_returns,
        rows_wow=rows_wow,
        wow_y_max=wow_y_max,
        # Totais por DEAL, não soma de barras: na visão de Líderes o diretor
        # engloba o gerente e a soma contava o time do gerente duas vezes.
        wow_total_saiu=int(len(dw_saiu)),
        wow_total_entrou=int(len(dw_entrou)),
        wow_stage_totals={_STALLED_STAGE_KEYS[st]: {
            "saiu": int((dw_saiu["stage_prev"] == st).sum()),
            "entrou": int((dw_entrou["stage_curr"] == st).sum())} for st in _ACTIVITY_STAGE_ORDER},
        n_mgr_stalled=n_mgr_stalled,
        sla_days=ACTIVITY_SLA_DAYS,
        snap_curr_label=_fmt_date_short(last_sun) + " · 12h",
        snap_prev_label=_fmt_date_short(prev_sun) + " · 12h",
        grain=grain,
    )


def _pipeline_stalled_page(grain="ae"):
    """Inatividade no funil: estado atual + comparação WoW (domingo passado vs
    domingo retrasado, 12h BRT). grain='ae' → linhas por AE; grain='leader' →
    linhas por líder (soma do time via expand_leader). Sistema pra comparar:
      1) Stacked chart — quantos deals parados em cada stage agora.
      2) Diverging chart WoW — saiu vs entrou da inatividade entre 2 domingos.
    SLAs: Qualifying 14d · Project Scoping 21d · Negotiation 14d ·
          Decision 14d · Procurement 21d.
    "Dias parado" = dias desde a última transição de stage no HubSpot."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # ----- 1) Estado atual (now) -----
    df_now = bq.pipeline_stalled(force_refresh=force, pipeline=_pl())
    df_now = _apply_roster_current(df_now)   # roster Q3 + remove quem saiu (BQ traz líder cru)
    df_now = df_now[df_now["leader"].isin(ALLOWED_LEADERS)]
    df_now = df_now[~df_now["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        df_now = df_now[df_now["region"] == f["region"]]
    if f.get("leader"):
        df_now = df_now[df_now["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
        df_now = df_now[df_now["account_executive"] == f["ae"]]

    # Marca cada linha como "parado" segundo a threshold do stage
    df_now["is_stalled"] = df_now.apply(
        lambda r: bool(pd.notna(r["days_stalled"])
                       and _STALLED_THRESHOLDS.get(r["stage_curr"], 999) < r["days_stalled"]),
        axis=1).astype(bool)
    stalled_now = df_now[df_now["is_stalled"]].copy()

    # Pivot AE × stage (n deals)
    if len(stalled_now) > 0:
        ct = stalled_now.pivot_table(index="account_executive", columns="stage_curr",
                                     values="deal_id", aggfunc="count", fill_value=0)
    else:
        ct = pd.DataFrame(index=pd.Index([], name="account_executive"))
    for st in _STALLED_STAGE_ORDER:
        if st not in ct.columns:
            ct[st] = 0
    ct["total"] = ct[_STALLED_STAGE_ORDER].sum(axis=1)
    ct = ct.sort_values("total", ascending=False)

    _ae_lead = dict(zip(df_now["account_executive"], df_now["leader"]))
    _nkeys = [f"n_{_STALLED_STAGE_KEYS[s]}" for s in _STALLED_STAGE_ORDER]
    rows_now = [{
        "account_executive": ae,
        "leader": _ae_lead.get(ae, ""),
        "total": int(ct.loc[ae, "total"]),
        **{f"n_{_STALLED_STAGE_KEYS[s]}": int(ct.loc[ae, s]) for s in _STALLED_STAGE_ORDER},
    } for ae in ct.index]
    if grain == "leader":
        rows_now = _leader_rollup(rows_now, f.get("leader"), ["total"] + _nkeys)
        for r in rows_now:                      # o rollup soma em float ("201.0" na barra)
            for k in ["total"] + _nkeys:
                r[k] = int(r[k])
        rows_now.sort(key=lambda r: r["total"], reverse=True)

    # Resumo total por stage
    summary_by_stage = [{
        "key":   _STALLED_STAGE_KEYS[s],
        "label": _STALLED_STAGE_LABELS[s],
        "color": _STALLED_STAGE_COLORS[s],
        "ink":   _STALLED_STAGE_INKS[s],
        "threshold": _STALLED_THRESHOLDS[s],
        "n_deals": int(ct[s].sum()) if len(ct) else 0,
    } for s in _STALLED_STAGE_ORDER]

    max_total_now = max([r["total"] for r in rows_now], default=0)
    total_stalled = int(ct["total"].sum())   if len(ct) else 0

    # ----- 2) WoW (saiu vs entrou da inatividade) -----
    last_sun, prev_sun = _last_two_sundays()
    t_curr_iso = f"{last_sun.isoformat()} 15:00:00 UTC"
    t_prev_iso = f"{prev_sun.isoformat()} 15:00:00 UTC"
    dw = bq.pipeline_stalled_wow(t_curr_iso, t_prev_iso, force_refresh=force, pipeline=_pl())
    dw = _apply_roster_current(dw)           # roster Q3 + remove quem saiu (BQ traz líder cru)
    dw = dw[dw["leader"].isin(ALLOWED_LEADERS)]
    dw = dw[~dw["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        dw = dw[dw["region"] == f["region"]]
    if f.get("leader"):
        dw = dw[dw["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
        dw = dw[dw["account_executive"] == f["ae"]]

    def _stalled_at(stage, days):
        th = _STALLED_THRESHOLDS.get(stage)
        # bool() + notna: deal que fechou na semana vem com dias NULOS; sem
        # isso a coluna virava 'object' e o ~ do pandas (bitwise em bool do
        # Python: ~False = -1) marcava TODO parado anterior como 'saiu'.
        return bool(th is not None and pd.notna(days) and days > th)

    dw["parado_curr"] = dw.apply(lambda r: _stalled_at(r["stage_curr"], r["days_curr"]), axis=1).astype(bool)
    dw["parado_prev"] = dw.apply(lambda r: _stalled_at(r["stage_prev"], r["days_prev"]), axis=1).astype(bool)
    dw_saiu   = dw[(dw["parado_prev"]) & (~dw["parado_curr"])]   # estavam parados, saíram
    dw_entrou = dw[(~dw["parado_prev"]) & (dw["parado_curr"])]   # cruzaram threshold

    # Breakdown por ESTÁGIO-âncora (onde estava parado / onde parou): alimenta os
    # chips de estágio do chart WoW — o JS recalcula saiu/entrou/net por seleção.
    saiu_stage   = _stale_wow_by_stage(dw_saiu, "stage_prev")
    entrou_stage = _stale_wow_by_stage(dw_entrou, "stage_curr")
    _skeys = [_STALLED_STAGE_KEYS[s] for s in _STALLED_STAGE_ORDER]

    saiu_by_ae   = dw_saiu.groupby("account_executive")["deal_id"].count()
    entrou_by_ae = dw_entrou.groupby("account_executive")["deal_id"].count()
    _ae_lead_wow = dict(zip(dw["account_executive"], dw["leader"]))
    aes_wow = sorted(set(saiu_by_ae.index) | set(entrou_by_ae.index))
    rows_wow = [{
        "account_executive": ae,
        "leader": _ae_lead_wow.get(ae, ""),
        "saiu":   int(saiu_by_ae.get(ae, 0)),
        "entrou": int(entrou_by_ae.get(ae, 0)),
        "net":    int(saiu_by_ae.get(ae, 0)) - int(entrou_by_ae.get(ae, 0)),
        **{f"saiu_{k}": saiu_stage.get(ae, {}).get(k, 0) for k in _skeys},
        **{f"entrou_{k}": entrou_stage.get(ae, {}).get(k, 0) for k in _skeys},
    } for ae in aes_wow]
    if grain == "leader":
        _roll_keys = (["saiu", "entrou"] + [f"saiu_{k}" for k in _skeys]
                      + [f"entrou_{k}" for k in _skeys])
        rows_wow = _leader_rollup(rows_wow, f.get("leader"), _roll_keys)
        for r in rows_wow:
            for k in _roll_keys:
                r[k] = int(r[k])
            r["net"] = int(r["saiu"]) - int(r["entrou"])
    rows_wow.sort(key=lambda r: r["net"], reverse=True)

    wow_y_max = max([max(r["saiu"], r["entrou"]) for r in rows_wow], default=1)

    return render_template(
        "pipeline_stalled.html", ctx=ctx,
        rows_now=rows_now,
        summary_by_stage=summary_by_stage,
        max_total_now=max_total_now,
        total_stalled=total_stalled,
        rows_wow=rows_wow,
        wow_y_max=wow_y_max,
        # Totais por DEAL, não soma de barras (em Líderes o diretor engloba o gerente).
        wow_total_saiu=int(len(dw_saiu)),
        wow_total_entrou=int(len(dw_entrou)),
        wow_stage_totals={_STALLED_STAGE_KEYS[st]: {
            "saiu": int((dw_saiu["stage_prev"] == st).sum()),
            "entrou": int((dw_entrou["stage_curr"] == st).sum())} for st in _STALLED_STAGE_ORDER},
        snap_curr_label=_fmt_date_short(last_sun) + " · 12h",
        snap_prev_label=_fmt_date_short(prev_sun) + " · 12h",
        grain=grain,
    )


# ============================================================
# Pipeline Total — todo o dinheiro do funil, cortado por qualquer eixo
# ============================================================
# Eixo X = estágio do funil (onde o dinheiro ESTÁ). Os 4 cortes de negócio
# (deal type, source, product, forecast category) funcionam como filtro E como
# empilhamento do gráfico — o mesmo registro alimenta os dois, então adicionar
# uma dimensão nova é uma entrada aqui e nada mais.
#
# `col`     coluna crua no frame do BQ
# `values`  [(chave curta na URL, valor cru no BQ, label, cor de preenchimento,
#            tinta p/ texto)] — cor e tinta separadas pelo mesmo motivo das
#           outras páginas (ver nota em .leg-toggle.is-on no style.css).
# Chave do bucket "campo vazio no HubSpot". Sem underscore de propósito: a chave
# vira atributo `data-<chave>` no template e é lida via `dataset[<chave>]` no JS —
# underscore/hífen ali viram outra coisa na conversão camelCase do dataset.
_PT_DIM_NONE = "nocat"

# Bucket do deal com MAIS DE UM produto marcado. Existe porque `product_of_deal`
# é multi-select no HubSpot: sem um bucket próprio, um deal Smart Trac + TracOS
# teria que ser contado nas duas colunas, e aí a soma dos segmentos passaria a
# ser MAIOR que a barra (~6% dos deals abertos têm mais de um produto). O
# empilhamento é por deal, não por produto, então cada deal cai num lugar só.
_PT_MULTI = "__multi__"

_PT_DIMS = {
    "dtype": {
        "col": "deal_type", "label": "Deal Type",
        "values": [
            ("acq", "Aquisition",      "Aquisição", "#2563EB", "#1D4ED8"),
            ("exp", "Up & cross sell", "Expansão",  "#047857", "#065F46"),
        ],
    },
    "source": {
        "col": "deal_source_agg", "label": "Source",
        "values": [
            ("inbound",   "Inbound",   "Inbound",   "#10B981", "#047857"),
            ("outbound",  "Outbound",  "Outbound",  "#6366F1", "#4338CA"),
            ("expansion", "Expansion", "Expansion", "#0891B2", "#0E7490"),
        ],
    },
    # Propriedade "Product" do HubSpot. `multi: True` avisa o resto do módulo
    # que a coluna guarda uma LISTA serializada com ';' — filtro e empilhamento
    # têm caminho próprio (ver `_pt_apply_dim_filter` / `_pt_series_key`).
    #
    # Filtrar por um produto = "o deal INCLUI esse produto" (Smart Trac também
    # traz os Smart Trac + TracOS), que é a pergunta de negócio real. Empilhar
    # por produto = "o deal é SÓ esse produto", com os combinados em
    # Multiprodutos — as duas semânticas diferem de propósito, porque só a
    # segunda fecha com o total da barra.
    #
    # Ordem = tamanho no funil Sales BR (Smart Trac 11.668 deals · Band Trac 7),
    # e o vocabulário é fechado: são estes 8 valores no HubSpot, nada mais.
    "prod": {
        "col": "product", "label": "Product", "multi": True,
        "values": [
            ("smarttrac",   "Smart Trac",    "Smart Trac",    "#2563EB", "#1D4ED8"),
            ("tracos",      "TracOS",        "TracOS",        "#7C3AED", "#6D28D9"),
            ("energytrac",  "Energy Trac",   "Energy Trac",   "#F59E0B", "#B45309"),
            ("smarttracex", "Smart Trac EX", "Smart Trac EX", "#0EA5E9", "#0369A1"),
            ("oeetrac",     "OEE Trac",      "OEE Trac",      "#DB2777", "#BE185D"),
            ("unitrac",     "Uni Trac",      "Uni Trac",      "#0891B2", "#0E7490"),
            ("omnitrac",    "Omni Trac",     "Omni Trac",     "#10B981", "#047857"),
            ("bandtrac",    "Band Trac",     "Band Trac",     "#65A30D", "#4D7C0F"),
            ("multi",      _PT_MULTI, "Multiprodutos", "#64748B", "#334155"),
            (_PT_DIM_NONE,  None,     "Sem produto",   "#94A3B8", "#475569"),
        ],
    },
    "fcat": {
        "col": "forecast_category", "label": "Forecast Category",
        # Ordem do mais comprometido pro menos. `OMIT` = "Not forecasted" e o
        # bucket vazio são a MAIORIA do pipe aberto (863 de 1349 sem categoria
        # nenhuma), então precisam existir como opção — se ficassem de fora o
        # gráfico empilhado não fecharia com o total e o número pareceria errado.
        "values": [
            # Rótulo = peso do modelo. Vem escrito aqui e em
            # FORECAST_CATEGORY_LABEL, que é a mesma lista em dois lugares desde
            # antes — se mudar um, mudar o outro, senão as duas abas passam a
            # chamar a mesma categoria de nomes diferentes.
            ("commit",    "COMMIT",     "100%",           "#047857", "#065F46"),
            ("bestcase",  "BEST_CASE",  "75%",            "#10B981", "#047857"),
            ("probable",  "PROBABLE",   "50%",            "#2563EB", "#1D4ED8"),
            ("funnel",    "PIPELINE",   "25%",            "#93C5FD", "#1E40AF"),
            ("omit",      "OMIT",       "0%",             "#F59E0B", "#B45309"),
            (_PT_DIM_NONE, None,        "Sem categoria",  "#94A3B8", "#475569"),
        ],
    },
}


def _pt_dim_meta(dim: str) -> list[dict]:
    """Valores de uma dimensão como dicts prontos pro template (chip + segmento).

    Em `fcat`, anexa o PESO EFETIVO do modelo de forecast
    (`FORECAST_CATEGORY_PCT_EFF`: 100 · 75 · 50 · 25 · 0 já × DIR_FORECAST_FACTOR),
    a mesma fonte da aba Forecast. Vem de lá e não de uma cópia local justamente
    pra as duas abas nunca discordarem sobre quanto vale uma categoria. Aqui
    `pct` é só PESO — o número que multiplica o amount, no Python e no JS da
    barra. O RÓTULO é o `label`, que segue sendo o parâmetro cru do HubSpot
    ("75%"); por isso o pct nunca deve ser colado atrás dele — daria "75% · 67,5%"."""
    out = []
    for k, raw, lbl, c, ink in _PT_DIMS[dim]["values"]:
        item = {"key": k, "raw": raw, "label": lbl, "color": c, "ink": ink}
        if dim == "fcat":
            # OMIT é 0% no modelo; "sem categoria" não tem peso definido (None) —
            # os dois são explicitamente NÃO ponderáveis, e a página diz isso em
            # vez de fingir que valem 0 ou 100.
            item["pct"] = (FORECAST_CATEGORY_PCT_EFF.get(raw)
                           if raw is not None else None)
        out.append(item)
    return out


def _pt_multi_list(s: pd.Series) -> pd.Series:
    """Coluna multi-select do HubSpot → lista de valores por linha.

    O HubSpot serializa multi-select como 'Smart Trac;TracOS'. Separar em lista
    aqui (e não comparar a string) é o que evita os dois bugs óbvios: filtrar
    por 'Smart Trac' com `==` perderia todo deal combinado, e filtrar com
    `str.contains` casaria 'Smart Trac EX' junto — dois produtos diferentes
    somados calados. Com a lista, a comparação é por valor INTEIRO."""
    return (s.fillna("").astype(str)
             .map(lambda v: [x.strip() for x in v.split(";") if x.strip()]))


def _pt_dim_label(dim: str, sel: str) -> str:
    """Chave curta da URL → rótulo de negócio ("acq" → "Aquisição"). Chave vazia
    ou desconhecida devolve "" — quem chama trata como "sem corte"."""
    if not sel:
        return ""
    return next((lbl for k, _raw, lbl, *_ in _PT_DIMS[dim]["values"] if k == sel), "")


def _pt_apply_dim_filter(df: pd.DataFrame, dim: str, sel: str) -> pd.DataFrame:
    """Aplica o filtro de uma dimensão pela chave curta da URL. Chave
    desconhecida = filtro ignorado (URL editável não pode virar 500)."""
    if not sel:
        return df
    entry = next((v for v in _PT_DIMS[dim]["values"] if v[0] == sel), None)
    if entry is None:
        return df
    col, raw = _PT_DIMS[dim]["col"], entry[1]
    if _PT_DIMS[dim].get("multi"):
        # Dimensão multi-valor: filtrar por um produto quer dizer "o deal INCLUI
        # esse produto", não "o deal é exatamente esse produto" — quem pergunta
        # "quanto de pipe tem Smart Trac" quer os combinados dentro também.
        vals = _pt_multi_list(df[col])
        if raw is _PT_MULTI:
            mask = vals.map(len) > 1
        elif raw is None:
            mask = vals.map(len) == 0
        else:
            mask = vals.map(lambda xs: raw in xs)
        return df[mask]
    if raw is None:                      # bucket "vazio no HubSpot"
        return df[df[col].isna() | (df[col].astype(str).str.strip() == "")]
    return df[df[col] == raw]


def _pt_series_key(df: pd.DataFrame, dim: str) -> pd.Series:
    """Coluna crua → chave curta da dimensão, com tudo que não mapeia caindo no
    bucket vazio. Garante que a soma dos segmentos == total da barra."""
    col = _PT_DIMS[dim]["col"]
    lookup = {raw: k for k, raw, *_ in _PT_DIMS[dim]["values"]
              if raw is not None and raw is not _PT_MULTI}
    if _PT_DIMS[dim].get("multi"):
        # Aqui a semântica é a do EMPILHAMENTO, não a do filtro: cada deal cai em
        # UM segmento, senão a soma das cores estouraria a altura da barra. Deal
        # com 2+ produtos vai pro bucket Multiprodutos em vez de ser rateado —
        # ratear exigiria dividir o amount, e o HubSpot não diz quanto do valor é
        # de cada produto.
        multi_key = next(k for k, raw, *_ in _PT_DIMS[dim]["values"]
                         if raw is _PT_MULTI)

        def _key(xs):
            if not xs:
                return _PT_DIM_NONE
            if len(xs) > 1:
                return multi_key
            return lookup.get(xs[0], _PT_DIM_NONE)

        return _pt_multi_list(df[col]).map(_key)
    return df[col].map(lookup).fillna(_PT_DIM_NONE)


def _pt_filtered_frame(f: dict, force: bool = False):
    """Universo do Pipeline Total já filtrado: deals ABERTOS do Sales BR com
    roster aplicado + todos os cortes da página (região, líder, AE, close date,
    deal type, source, product, forecast category).

    O corte de close date tem DUAS partes independentes: a presença (`cdate`:
    todos · com · sem) e a janela de datas da barra do topo. Ver o bloco marcado
    lá embaixo pra por que uma desliga a outra.

    Compartilhado pelo gráfico e pelo drill-down DE PROPÓSITO: se cada um
    montasse seu próprio filtro, a lista explodida passaria a discordar da barra
    no primeiro ajuste — é o bug clássico de drill. Devolve (df, n_sem_close),
    onde n_sem_close = deals descartados por não ter close date quando há janela.
    """
    df = bq.pipeline_total_raw(force_refresh=force, pipeline=_pl())
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    # Gestor FICA: aqui o eixo é o estágio do funil, não a pessoa, então o deal
    # dele entra no total e na visão do líder sem virar barra de vendedor.
    df = drop_departed_people(df)

    # Só funil aberto. amount nulo (363 deals) = 0, pra o deal continuar sendo
    # contado na quantidade sem inventar dinheiro que o HubSpot não tem.
    df = df[df["is_open"].fillna(False).astype(bool)].copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    if f.get("region"):
        df = df[df["region"] == f["region"]]
    if f.get("leader"):
        df = df[df["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]

    # ---- Close date: PRESENÇA (filtro da página) + JANELA (barra de datas) ----
    #
    # O filtro de presença vem primeiro porque ele decide se a janela faz sentido.
    # "Sem close date" e janela de datas são mutuamente excludentes POR DEFINIÇÃO:
    # deal sem data nunca cai num intervalo. Se as duas fossem aplicadas juntas o
    # resultado seria sempre vazio, e a tela pareceria quebrada em vez de
    # contraditória — então a janela é IGNORADA quando o usuário pede o bucket sem
    # data, e a página diz que ignorou (`cdate_ignora_janela`). Metade do pipe
    # aberto não tem close date preenchido no HubSpot, então esse bucket é grande
    # demais pra só existir como "ficaram de fora" no subtítulo.
    cdate = f.get("cdate", "")
    if cdate in ("com", "sem"):
        _cd = pd.to_datetime(df["close_date"], errors="coerce")
        df = df[_cd.isna() if cdate == "sem" else _cd.notna()]

    # Presença de Customer Owner (CS). id vazio/NaN = sem dono de conta.
    cown = f.get("cown", "")
    if cown in ("com", "sem"):
        _co = df["customer_owner_id"].astype("string").str.strip().replace("", pd.NA)
        df = df[_co.isna() if cown == "sem" else _co.notna()]

    lo, hi = _parse_filter_date(f.get("date_from")), _parse_filter_date(f.get("date_to"))
    n_sem_close = 0
    if (lo or hi) and cdate != "sem":
        cd = pd.to_datetime(df["close_date"], errors="coerce")
        sem_close = cd.isna()
        n_sem_close = int(sem_close.sum())
        keep = ~sem_close
        # Comparação em datetime64 (Timestamp), NUNCA em `.dt.date`: quando o recorte
        # não tem NENHUM close date preenchido, `cd` fica todo-NaT e o `.dt.date` de
        # série todo-NaT devolve datetime64[ns] em vez de object-de-date — comparar
        # isso com um datetime.date levanta TypeError e a página inteira vira 500. O
        # `keep = ~sem_close` NÃO protege: pandas avalia os dois lados do `&` antes de
        # combinar, então não há curto-circuito. Cenário real, não teórico: basta
        # filtrar por um AE recém-contratado, cujos deals ainda estão todos sem Close
        # Date — o recorte de UMA pessoa é o que torna "coluna toda nula" alcançável,
        # ao contrário dos cortes que rodam sobre o frame inteiro.
        # `.dt.normalize()` (e não Timestamp cru sobre `cd`) preserva a semântica de
        # DIA que o filtro sempre teve: zera a hora dos dois lados, então a borda de
        # cima continua incluindo o último dia INTEIRO. Hoje `close_date` é dbdate
        # (0 de 16.499 linhas com hora != 00:00), logo os dois jeitos empatam — mas o
        # empate seria acidente da fonte, e no dia em que ela virar timestamp o
        # `<= Timestamp(hi)` passaria a descartar calado o que fecha depois da meia
        # noite do último dia. NaT normaliza pra NaT e compara False, que é o que se
        # quer: deal sem close date já sai por `~sem_close` e é contado em n_sem_close.
        cdn = cd.dt.normalize()
        if lo:
            keep &= cdn >= pd.Timestamp(lo)
        if hi:
            keep &= cdn <= pd.Timestamp(hi)
        df = df[keep]

    # ---- Cortes de negócio ----
    for dim in _PT_DIMS:
        df = _pt_apply_dim_filter(df, dim, f.get(dim, ""))
    return df, n_sem_close


def _pt_combo_key(xs: list[str]) -> str:
    """Combinação de produtos de um deal, com a ordem NORMALIZADA.

    O HubSpot guarda o multi-select na ordem em que o vendedor clicou, então a
    mesma combinação aparece gravada de formas diferentes: 'Smart Trac;TracOS'
    (16 deals) e 'TracOS;Smart Trac' (1) são a MESMA coisa. Sem ordenar, o
    detalhamento quebraria uma combinação em duas linhas e a maior delas
    apareceria menor do que é — hoje são 26 strings cruas para 17 combinações
    reais. `set` também remove produto repetido na mesma string."""
    return " + ".join(sorted(set(xs)))


def _pt_prod_breakdown(df: pd.DataFrame, total_amount: float):
    """Abre a dimensão Product nas duas leituras que a barra empilhada não dá.

    A barra é EXCLUSIVA (cada deal num segmento só), então ela responde "quanto
    do pipe é só Smart Trac" e joga todo deal combinado num bucket Multiprodutos
    opaco. Faltam duas perguntas, e são as que se faz de verdade:

      `presence`  quanto de pipe TEM cada produto, sozinho ou acompanhado — é a
                  mesma semântica do filtro Product do topo. As linhas somam
                  MAIS que o total de propósito: um deal Smart Trac + TracOS
                  conta nas duas, e é isso que se quer saber aqui.
      `combos`    o que exatamente tem dentro de Multiprodutos: uma linha por
                  combinação real, com ordem normalizada (ver `_pt_combo_key`).

    Devolve ([], []) fora da dimensão Product ou com frame vazio."""
    if not len(df):
        return [], []
    lists = _pt_multi_list(df["product"])
    n = lists.map(len)

    presence = []
    for v in _pt_dim_meta("prod"):
        raw = v["raw"]
        if raw is None or raw is _PT_MULTI:   # buckets, não produtos
            continue
        has = lists.map(lambda xs, _r=raw: _r in xs)
        if not has.any():
            continue
        alone, with_others = has & (n == 1), has & (n > 1)
        amt = float(df.loc[has, "amount"].sum())
        presence.append({
            **v,
            "deals": int(has.sum()), "amount": amt,
            "share": (amt / total_amount) if total_amount else 0.0,
            "deals_alone": int(alone.sum()),
            "amount_alone": float(df.loc[alone, "amount"].sum()),
            "deals_with": int(with_others.sum()),
            "amount_with": float(df.loc[with_others, "amount"].sum()),
        })
    presence.sort(key=lambda r: r["amount"], reverse=True)

    combos = []
    multi = df[n > 1]
    if len(multi):
        keys = _pt_multi_list(multi["product"]).map(_pt_combo_key)
        multi_amount = float(multi["amount"].sum())
        for k, g in multi.groupby(keys, sort=False):
            amt = float(g["amount"].sum())
            combos.append({
                "key": k, "label": k, "deals": int(len(g)), "amount": amt,
                "n_prod": len(k.split(" + ")),
                # % DO multiproduto, não do pipe: a pergunta aqui é a composição
                # de dentro do bucket.
                "share": (amt / multi_amount) if multi_amount else 0.0,
            })
        combos.sort(key=lambda r: r["amount"], reverse=True)
    return presence, combos


def _pt_stage_ids(df: pd.DataFrame) -> pd.Series:
    """stage_label cru → dealstage_id canônico. No Show tem id próprio; o que não
    resolve cai em '_outros' em vez de sumir da soma."""
    sids = pd.Series([stage_id(s) or "" for s in df["stage_label"]], index=df.index)
    miss = sids == ""
    sids[miss] = ["53862044" if "no show" in str(s).lower() else "_outros"
                  for s in df.loc[miss, "stage_label"]]
    return sids


def _pt_stage_label(sid: str) -> str:
    if sid == "53862044":
        return "No Show"
    if sid == "_outros":
        return "Outros"
    s = _STAGE_BY_ID.get(sid)
    return s["label"] if s else sid


# Colunas do drill do Pipeline Total. Além do básico (deal, estágio, amount), traz
# os parâmetros de análise de pipeline: tipo, origem, forecast category com o peso
# do modelo, o valor ponderado, o close date e a idade do deal.
_DRILL_PT_COLUMNS = [
    {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
    {"key": "stage",     "label": "Estágio"},
    {"key": "amount",    "label": "Amount",    "type": "money"},
    {"key": "fcat",      "label": "Forecast"},
    {"key": "weighted",  "label": "Ponderado", "type": "money"},
    {"key": "deal_type", "label": "Tipo"},
    {"key": "source",    "label": "Origem"},
    # Product vem cru (com os ';' do multi-select) de propósito: no drill a
    # pergunta é "que produto ESTE deal tem", e agrupar em "Multiprodutos" aqui
    # esconderia justamente a resposta.
    {"key": "product",   "label": "Product", "wrap": True},
    {"key": "close",     "label": "Close date"},
    {"key": "age",       "label": "Idade (d)",  "type": "int"},
]


@app.route("/pipeline/total/deals")
def pipeline_total_deals():
    """Drill-down do Pipeline Total. Recorta pelo que foi clicado — `stage` (uma
    barra) e/ou `seg`+`by` (um segmento da barra) — sempre por cima dos filtros
    da página, que vêm na querystring e são reaplicados pelo mesmo
    `_pt_filtered_frame` do gráfico."""
    f = get_filters()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    df, _ = _pt_filtered_frame(f, force=request.args.get("refresh") == "1")

    sid = (request.args.get("stage") or "").strip()
    by  = request.args.get("by", "")
    seg = (request.args.get("seg") or "").strip()
    # Recortes do detalhamento de Product. Vêm em parâmetros PRÓPRIOS, e não
    # reescrevendo o `prod` da página, porque são aplicados POR CIMA dos filtros
    # (como `stage` e `seg`): sobrescrever `prod` faria a lista explodida ignorar
    # o filtro do topo e discordar da tabela que foi clicada.
    has   = (request.args.get("has") or "").strip()
    combo = (request.args.get("combo") or "").strip()

    title_bits = []
    if len(df):
        df["_sid"] = _pt_stage_ids(df)
        if sid:
            df = df[df["_sid"] == sid]
            title_bits.append(_pt_stage_label(sid))
        if has:
            # "Inclui o produto X" — mesma semântica do filtro Product, então
            # reusa a função dele em vez de reimplementar o casamento por token.
            entry = next((v for v in _PT_DIMS["prod"]["values"] if v[0] == has), None)
            if entry is not None:
                df = _pt_apply_dim_filter(df, "prod", has)
                title_bits.append(f"inclui {entry[2]}")
        if combo:
            keys = _pt_multi_list(df["product"]).map(_pt_combo_key)
            df = df[keys == combo]
            title_bits.append(combo)
        if by in _PT_DIMS and seg:
            # `seg` pode vir com VÁRIAS chaves separadas por vírgula: a legenda do
            # gráfico deixa o usuário ligar/desligar segmentos em qualquer
            # combinação, e o drill tem que explodir exatamente o que sobrou na
            # tela — um único valor faria a lista discordar da barra clicada.
            lbls = {v["key"]: v["label"] for v in _pt_dim_meta(by)}
            # Chave desconhecida é descartada, não filtrada (mesma política de
            # `_pt_apply_dim_filter`): URL editada à mão não pode virar lista
            # vazia sem explicação.
            keys = [k for k in (x.strip() for x in seg.split(",")) if k in lbls]
            if keys:
                df = df[_pt_series_key(df, by).isin(keys)]
                title_bits.append(" + ".join(lbls[k] for k in keys))

    # Peso da forecast category por deal (mesma fonte da aba Forecast).
    pct_by_raw = {v["raw"]: v.get("pct") for v in _pt_dim_meta("fcat")}
    fcat_label = {v["raw"]: v["label"] for v in _pt_dim_meta("fcat")}
    # Rótulos de negócio no lugar dos valores crus do HubSpot ("Aquisition" →
    # "Aquisição"), pro drill falar a mesma língua dos chips e do resto do site.
    dtype_label = {v["raw"]: v["label"] for v in _pt_dim_meta("dtype")}
    today = _today()

    rows = []
    if len(df):
        df = df.sort_values("amount", ascending=False)
        for _, r in df.iterrows():
            raw = r.get("forecast_category")
            raw = None if (pd.isna(raw) or str(raw).strip() == "") else raw
            pct = pct_by_raw.get(raw)
            amt = float(r["amount"] or 0)
            created = pd.to_datetime(r.get("created_date"), errors="coerce")
            closed  = pd.to_datetime(r.get("close_date"), errors="coerce")
            rows.append({
                "deal_name": r.get("deal_name") or "—",
                "deal_url":  _hs_deal_url(r.get("deal_id")),
                "ae":        r["account_executive"],
                "stage":     _pt_stage_label(r["_sid"]),
                "amount":    amt,
                # O rótulo JÁ É o peso ("75%"), então nada é anexado — antes o
                # nome vinha do HubSpot e o peso era colado atrás.
                "fcat":      fcat_label.get(raw, "—"),
                "weighted":  (amt * pct) if pct is not None else None,
                "deal_type": dtype_label.get(r.get("deal_type"), r.get("deal_type") or "—"),
                "source":    r.get("deal_source_agg") or "—",
                "product":   (str(r.get("product")).replace(";", " · ")
                              if r.get("product") and not pd.isna(r.get("product"))
                              else "—"),
                "close":     closed.strftime("%d/%m/%Y") if pd.notna(closed) else "—",
                "age":       int((today - created.date()).days) if pd.notna(created) else 0,
            })

    amount = sum(x["amount"] for x in rows)
    wsum   = sum(x["weighted"] for x in rows if x["weighted"] is not None)
    show_ae = "leader" if (grain == "leader" or not f.get("ae")) else "ae"
    sub = _drill_money_subtitle(len(rows), amount)
    if wsum:
        sub += f" · ponderado {fmt_money(wsum)}"
    return jsonify({
        "title": " · ".join(title_bits) or "Pipeline aberto",
        "subtitle": sub,
        "columns": _with_ae_col(_DRILL_PT_COLUMNS, show_ae),
        "rows": rows,
    })


@app.route("/pipeline/total")
def pipeline_total():
    return _pipeline_total_page("ae")


@app.route("/leaders/pipeline/total")
def leaders_pipeline_total():
    return _pipeline_total_page("leader")


def _pipeline_total_page(grain="ae"):
    """Amount em pipeline por estágio do funil, cortável por deal type, source,
    product, forecast category, líder, AE e close date.

    Universo = deals ABERTOS (nem won nem lost) do Sales BR: a pergunta é "quanto
    dinheiro tem no meu funil hoje", então deal fechado não entra.

    Sobre o close date: é OPCIONAL aqui (ver PIPELINE_TOTAL_PATHS). Metade dos
    deals abertos não tem a data preenchida no HubSpot; a página conta quantos
    ficaram de fora quando há janela, em vez de encolher o total sem explicar.
    O filtro `cdate` (todos · com · sem) transforma esse bucket em recorte de
    primeira classe — dá pra olhar SÓ os deals sem data, que é o que estava
    invisível antes."""
    ctx = base_context()
    f = ctx["filters"]
    df, n_sem_close = _pt_filtered_frame(f, force=request.args.get("refresh") == "1")

    # ---- Empilhamento: qual dimensão colore as barras ----
    by = request.args.get("by", "")
    if by not in _PT_DIMS:
        by = ""
    segs = _pt_dim_meta(by) if by else []
    if by and len(df):
        df["_seg"] = _pt_series_key(df, by)
        # Segmento sem NENHUM deal no recorte sai da legenda. Ele já não aparecia
        # na tabela de composição (que pula n==0), e como chip virava um filtro
        # que não filtra nada — visível agora que Product tem 10 valores e o
        # recorte típico usa 8. Some da legenda, não da conta: o bucket vazio
        # contribui 0 pra qualquer soma, então nada muda nos totais.
        _live = set(df["_seg"].unique())
        segs = [s for s in segs if s["key"] in _live]

    # ---- Agregação por estágio (ordem do funil, não alfabética) ----
    # Usa _FUNNEL_ORDER_MAP pra No Show cair entre Scheduled e Qualifying; deals
    # com stage que não resolve entram como "Outros" no fim, em vez de sumir.
    if len(df):
        df["_sid"] = _pt_stage_ids(df)

    rows = []
    if len(df):
        for sid, g in df.groupby("_sid", sort=False):
            row = {
                "sid": sid,
                "label": _pt_stage_label(sid),
                "amount": float(g["amount"].sum()),
                "deals": int(len(g)),
            }
            if by:
                row["segs"] = {s["key"]: float(g.loc[g["_seg"] == s["key"], "amount"].sum())
                               for s in segs}
                # Contagem por segmento junto do amount: a legenda recalcula os
                # dois no cliente, e sem isso o "N deals" embaixo da barra
                # continuaria mostrando o estágio inteiro depois de desligar
                # segmentos — número contradizendo o R$ logo acima.
                row["segn"] = {s["key"]: int((g["_seg"] == s["key"]).sum())
                               for s in segs}
            rows.append(row)
        rows.sort(key=lambda r: _FUNNEL_ORDER_MAP.get(r["sid"], 99))

    total_amount = float(df["amount"].sum()) if len(df) else 0.0
    total_deals  = int(len(df))
    max_amount   = max([r["amount"] for r in rows], default=0.0)

    # Composição do total pela dimensão empilhada — a leitura "quanto do meu pipe
    # é Commit" sem precisar somar as barras de cabeça.
    seg_totals = []
    weighted_total = None
    weighted_cov = None
    if by and len(df):
        for s in segs:
            amt = float(df.loc[df["_seg"] == s["key"], "amount"].sum())
            n   = int((df["_seg"] == s["key"]).sum())
            if n:
                row = {**s, "amount": amt, "deals": n,
                       "share": (amt / total_amount) if total_amount else 0.0}
                # Ponderado = amount × peso da forecast category (modelo). Só na
                # dimensão fcat; nas outras não existe probabilidade associada.
                if by == "fcat":
                    w = s.get("pct")
                    row["weighted"] = (amt * w) if w is not None else None
                seg_totals.append(row)
        if by == "fcat":
            weighted_total = sum(r["weighted"] for r in seg_totals
                                 if r.get("weighted") is not None)
            # Quanto do pipe o ponderado NÃO representa. Sem isso o card
            # "Ponderado" é lido como forecast do funil inteiro, quando hoje
            # 42% dos deals não têm categoria e 41% estão em Not forecasted
            # (peso 0) — ou seja, ele cobre ~17% do dinheiro. É informação de
            # qualidade de dado do HubSpot, não do cálculo.
            unclassified = sum(r["amount"] for r in seg_totals
                               if r.get("weighted") is None)
            zero_weight  = sum(r["amount"] for r in seg_totals
                               if r.get("pct") == 0)
            weighted_cov = {
                "unclassified_pct": (unclassified / total_amount) if total_amount else 0.0,
                "zero_pct":         (zero_weight / total_amount) if total_amount else 0.0,
            }

    # Detalhamento da dimensão Product: presença por produto e o que tem dentro
    # do bucket Multiprodutos. Só na dimensão Product — nas outras cada deal já
    # tem um valor único e não há nada pra abrir.
    prod_presence, prod_combos = ([], [])
    if by == "prod":
        prod_presence, prod_combos = _pt_prod_breakdown(df, total_amount)

    # Filtros ativos, sem as chaves vazias — é a base das URLs do switcher de
    # empilhamento. Montado aqui (e não no template) porque url_for com valor ""
    # emite `?dtype=&source=` na URL, que suja o link e vira chip fantasma.
    keep = {k: v for k, v in f.items() if v and k != "role"}

    # Chips de presença de close date. Montados aqui pelo mesmo motivo do `keep`:
    # url_for com valor "" emitiria `?cdate=` e viraria chip fantasma.
    _cd_base = {k: v for k, v in keep.items() if k != "cdate"}
    if by:
        _cd_base["by"] = by
    cdate_urls = {
        "":    url_for(request.endpoint, **_cd_base),
        "com": url_for(request.endpoint, cdate="com", **_cd_base),
        "sem": url_for(request.endpoint, cdate="sem", **_cd_base),
    }
    _co_base = {k: v for k, v in keep.items() if k != "cown"}
    if by:
        _co_base["by"] = by
    cown_urls = {
        "":    url_for(request.endpoint, **_co_base),
        "com": url_for(request.endpoint, cown="com", **_co_base),
        "sem": url_for(request.endpoint, cown="sem", **_co_base),
    }
    # A janela de datas foi descartada por incompatibilidade, não por bug.
    cdate_ignora_janela = bool(f.get("cdate") == "sem"
                               and (f.get("date_from") or f.get("date_to")))

    return render_template(
        "pipeline_total.html", ctx=ctx, grain=grain,
        cdate=f.get("cdate", ""), cdate_urls=cdate_urls,
        cown=f.get("cown", ""), cown_urls=cown_urls,
        cdate_ignora_janela=cdate_ignora_janela,
        rows=rows, segs=segs, by=by, seg_totals=seg_totals,
        total_amount=total_amount, total_deals=total_deals, max_amount=max_amount,
        ticket=(total_amount / total_deals if total_deals else 0.0),
        n_sem_close=n_sem_close, keep=keep, weighted_total=weighted_total,
        weighted_cov=weighted_cov,
        prod_presence=prod_presence, prod_combos=prod_combos,
        dims=[{"key": k, "label": v["label"], "opts": _pt_dim_meta(k)}
              for k, v in _PT_DIMS.items()],
    )


# ============================================================
# Aba Legal — o jurídico entrou no deal? mudou ticket médio e conversão?
# ============================================================
# Rótulos dos estágios ABERTOS do pipeline de ticket do jurídico (stageOrder
# 0..4). Signed(5)/concluded(6) e Lost(7) não entram aqui porque não são "onde a
# minuta está", são desfecho — viram `legal_status` logo abaixo.
_LEGAL_OPEN_STAGE = {
    0: "Requested",
    1: "Minuta / análise",
    2: "Revisão interna",
    3: "Revisão do cliente",
    4: "Enviado p/ assinatura",
}

# Definição de "jurídico envolvido". São três porque os dois sinais medem coisas
# diferentes e não se contêm: o TICKET é o processo formal (o jurídico foi
# acionado), o E-MAIL é a interação nominal das três caixas do time. Existem 240
# deals fechados que chegaram a Negociação+ com e-mail do jurídico e ZERO ticket
# — jurídico acionado por fora do processo, que a definição por ticket não vê.
_LEGAL_ENVS = {
    "ticket": {"label": "Ticket do jurídico",
               "hint": "deal com ticket associado nos pipelines Legal BR / Legal NA"},
    "mail":   {"label": "Interação nominal",
               "hint": "e-mail com Ana Meneguelli, Luiza Daudt ou juridico@ na conversa do deal"},
    "any":    {"label": "Ticket ou interação",
               "hint": "qualquer um dos dois sinais"},
}

# Coorte da comparação. O padrão NÃO é "todos os fechados" de propósito: o
# jurídico só é acionado quando o contrato entra em cena, ou seja no fim do
# funil. Comparar contra a base inteira mede a ORDEM DOS EVENTOS (quem chegou
# perto de fechar aciona o jurídico) e não o efeito do jurídico — dá ~84% de
# conversão contra ~6%, um número que ninguém deveria levar pra reunião. Restrito
# a quem pisou em Negociação ou além, onde acionar o jurídico era uma escolha de
# verdade, a diferença cai pra ~86% contra ~27%: menor, e defensável.
_LEGAL_COHORTS = {
    "neg": {"label": "Chegaram a Negociação+", "min_stage": 4,
            "hint": "deals que pisaram em Negociação, Decision ou Procurement — "
                    "onde acionar o jurídico era uma escolha real"},
    "all": {"label": "Todos os fechados", "min_stage": 0,
            "hint": "base inteira; inflada pelo viés de etapa (ver aviso)"},
}

# Default de cada corte. Existe como dado (e não repetido em literal) porque o
# valor aparece em três lugares — resolução, montagem de URL e o `keep` da
# página — e divergir num deles faz o chip aceso discordar do número na tela.
_LEGAL_DEFAULTS = {"env": "ticket", "coh": "neg"}


def _legal_env(v: str) -> str:
    return v if v in _LEGAL_ENVS else _LEGAL_DEFAULTS["env"]


def _legal_coh(v: str) -> str:
    return v if v in _LEGAL_COHORTS else _LEGAL_DEFAULTS["coh"]


def _legal_frame(f: dict, force: bool = False):
    """Universo da aba Legal: deals FECHADOS do Sales BR (won ou lost), com o
    estado do jurídico anexado e todos os cortes da página aplicados.

    Só deal fechado entra porque a pergunta central é conversão, e conversão
    exige desfecho: deal aberto não é "não convertido", é "ainda não sabemos" —
    ele no denominador puxaria a taxa dos dois grupos pra baixo em proporções
    diferentes (o pipe aberto se concentra em quem ainda não chamou o jurídico).

    Compartilhado pelos painéis e pelo drill-down DE PROPÓSITO, pela mesma razão
    de `_pt_filtered_frame`: dois filtros paralelos divergem no primeiro ajuste.

    Devolve (df, mail_ok). `mail_ok=False` quando a query de e-mail falhou, pra
    a página DIZER que o sinal nominal está indisponível — sem isso ela mostraria
    "zero interação" como se fosse um fato observado, e não falta de dado.
    """
    df = bq.legal_deals_raw(force_refresh=force, pipeline=_pl())
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]

    # Só deals com desfecho. `amount` nulo → 0: o deal continua contando na
    # conversão (é o que ele responde) sem inventar dinheiro que o HubSpot não
    # tem. Por isso o ticket médio só olha WON e ignora amount 0 (ver abaixo).
    df = df[df["dealstage"].isin(("closedwon", "closedlost"))].copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["is_won"] = df["dealstage"].eq("closedwon")

    # Sinal de e-mail. Best-effort: se a query cara falhar, a aba continua de pé
    # com a definição por ticket em vez de devolver 500.
    mail_ok = True
    try:
        m = bq.legal_mail_touch_raw(force_refresh=force, pipeline=_pl())
    except Exception as e:  # noqa: BLE001 — sinal opcional, aba não depende dele
        print(f"pepeuhacker · legal_mail_touch indisponível: {e}", flush=True)
        m, mail_ok = None, False
    _mail_cols = ["mail_ana", "mail_luiza", "mail_juridico"]
    if m is not None and len(m):
        df = df.merge(m[["deal_id"] + _mail_cols], on="deal_id", how="left")
    # A coluna pode NÃO EXISTIR (query de e-mail fora do ar) ou existir com NaN
    # (deal sem e-mail do jurídico, do LEFT JOIN). O `in df.columns` é o que
    # separa os dois: `df.get(col)` num frame sem a coluna devolve None, e
    # `pd.to_numeric(None)` devolve um float escalar — não uma Series — então o
    # `.fillna()` seguinte estouraria AttributeError e derrubaria a aba
    # justamente no cenário que o try/except acima existe pra sobreviver.
    for c in _mail_cols:
        df[c] = (pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
                 if c in df.columns else 0)

    if f.get("region"):
        df = df[df["region"] == f["region"]]
    if f.get("leader"):
        df = df[df["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]

    # Janela de datas = CLOSE DATE (é a data do desfecho que estamos medindo).
    # Normalizada em datetime64 dos dois lados pelo motivo documentado em
    # `_pt_filtered_frame`: `.dt.date` sobre coluna toda-NaT devolve
    # datetime64 e comparar com date levanta TypeError — recorte de um AE só
    # torna "coluna toda nula" alcançável.
    lo, hi = _parse_filter_date(f.get("date_from")), _parse_filter_date(f.get("date_to"))
    if lo or hi:
        cd = pd.to_datetime(df["close_date"], errors="coerce").dt.normalize()
        if lo:
            df = df[cd >= pd.Timestamp(lo)]
            cd = cd.loc[df.index]
        if hi:
            df = df[cd <= pd.Timestamp(hi)]

    df = df.copy()

    # ---- Os dois sinais de envolvimento, cada um na sua coluna ----
    df["has_ticket"] = df["legal_tickets"].fillna(0).astype(int) > 0
    df["has_mail"]   = df[_mail_cols].sum(axis=1) > 0

    # ---- Toque nominal por pessoa: ticket OU e-mail ----
    # Duas colunas por pessoa viram uma só aqui porque a pergunta do Pedro é
    # "houve interação dela?", não "por qual canal". O canal continua visível na
    # tabela por pessoa e nos badges do drill.
    df["t_ana"]      = df["tk_ana"].fillna(False).astype(bool) | (df["mail_ana"] > 0)
    df["t_luiza"]    = df["tk_luiza"].fillna(False).astype(bool) | (df["mail_luiza"] > 0)
    df["t_juridico"] = df["mail_juridico"] > 0

    # ---- Desfecho do jurídico ----
    sig  = df["legal_signed"].fillna(0).astype(int) > 0
    lost = df["legal_lost"].fillna(0).astype(int) > 0
    andn = df["legal_open_order"].notna()
    # Atribuído em ordem CRESCENTE de prioridade (cada .loc sobrescreve o
    # anterior): assinado ganha de "em andamento", que ganha de perdido. Um deal
    # com contrato assinado E outra minuta abandonada conta como assinado — o
    # que interessa é se ALGUM contrato saiu.
    df["legal_status"] = "Com ticket, estágio indefinido"
    df.loc[~df["has_ticket"], "legal_status"] = "Sem ticket"
    df.loc[df["has_ticket"] & lost, "legal_status"] = "Perdido no jurídico"
    df.loc[df["has_ticket"] & andn, "legal_status"] = "Minuta em andamento"
    df.loc[df["has_ticket"] & sig,  "legal_status"] = "Contrato assinado"
    # String vazia (e não None) pro caso "não há minuta em andamento": `.map()`
    # devolvendo None numa coluna float volta como NaN, e NaN é TRUTHY — o
    # `if r.legal_stage_label` do drill passava e escrevia "Sem ticket · nan".
    _ord = pd.to_numeric(df["legal_open_order"], errors="coerce")
    df["legal_stage_label"] = [
        _LEGAL_OPEN_STAGE.get(int(v), "") if pd.notna(v) else "" for v in _ord]

    return df, mail_ok


def _legal_env_mask(df, env: str):
    """Máscara de 'jurídico envolvido' segundo a definição escolhida."""
    if env == "mail":
        return df["has_mail"]
    if env == "any":
        return df["has_ticket"] | df["has_mail"]
    return df["has_ticket"]


def _legal_cohort(df, coh: str):
    """Recorta a coorte da comparação (ver `_LEGAL_COHORTS`)."""
    return df[df["max_stage_num"] >= _LEGAL_COHORTS[coh]["min_stage"]]


def _legal_stats(df) -> dict:
    """Bloco de números de um grupo de deals fechados.

    `ticket` = ticket médio dos deals GANHOS com valor > 0. Só won porque um deal
    perdido não tem ticket (nunca virou receita), e valor > 0 porque 363 deals do
    Sales BR têm amount vazio no HubSpot — contá-los como R$ 0 no denominador
    afunda a média com dinheiro que existe mas não foi preenchido. `ticket_cov`
    diz de quantos deals a média saiu, pra o número não ser lido como se
    cobrisse os won todos."""
    n = int(len(df))
    won = df[df["is_won"]] if n else df
    n_won = int(len(won))
    priced = won[won["amount"] > 0] if n_won else won
    return {
        "deals":      n,
        "won":        n_won,
        "lost":       n - n_won,
        "conv":       (n_won / n * 100) if n else None,
        "mrr":        float(won["amount"].sum()) if n_won else 0.0,
        "ticket":     (float(priced["amount"].mean()) if len(priced) else None),
        "ticket_cov": int(len(priced)),
    }


def _legal_drill_args(keep: dict, grain: str, **extra) -> dict:
    """Args do drill-down: filtros da página + o recorte clicado. Mantém a aba e
    a lista explodida falando do mesmo conjunto."""
    return {**keep, "grain": ("leader" if grain == "leader" else "ae"), **extra}


_DRILL_LEGAL_COLUMNS = [
    {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
    {"key": "outcome",   "label": "Desfecho"},
    {"key": "amount",    "label": "Valor", "type": "money"},
    {"key": "legal",     "label": "Jurídico"},
    {"key": "who",       "label": "Interação"},
    {"key": "tickets",   "label": "Tickets", "type": "int"},
    {"key": "deal_type", "label": "Tipo"},
    {"key": "stage_max", "label": "Etapa máx."},
    {"key": "close",     "label": "Fechou em"},
]


@app.route("/pipeline/legal/deals")
def pipeline_legal_deals():
    """Drill-down da aba Legal. Recorta pelo que foi clicado por cima dos filtros
    da página, sempre pelo mesmo `_legal_frame` dos painéis:
      env/coh      — definição e coorte ativas na tela (vêm nos filtros)
      side=com|sem — o lado do comparativo
      cell=tk_ml|tk_only|ml_only|none — uma célula da matriz ticket × e-mail
      who=ana|luiza|juridico — deals com interação daquela pessoa
      status=<legal_status> — uma linha da tabela de desfecho no jurídico
    """
    f = get_filters()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    df, _ = _legal_frame(f, force=request.args.get("refresh") == "1")
    env, coh = _legal_env(f.get("env", "")), _legal_coh(f.get("coh", ""))
    df = _legal_cohort(df, coh)

    # O rótulo do recorte é anexado sempre que o argumento é VÁLIDO, e não só
    # quando sobrou linha: senão um recorte legítimo mas vazio abriria a modal
    # com o título genérico "Deals fechados", que descreve o conjunto errado.
    # O `len(df)` guarda apenas a filtragem, que num frame vazio não tem o que
    # fazer.
    bits = []
    _CELLS = {"tk_ml": "Ticket + interação nominal", "tk_only": "Só ticket",
              "ml_only": "Só interação nominal", "none": "Nenhum sinal"}

    side = (request.args.get("side") or "").strip()
    if side in ("com", "sem"):
        bits.append(("Com" if side == "com" else "Sem") + " jurídico")
        if len(df):
            mask = _legal_env_mask(df, env)
            df = df[mask if side == "com" else ~mask]

    cell = (request.args.get("cell") or "").strip()
    if cell in _CELLS:
        bits.append(_CELLS[cell])
        if len(df):
            tk, ml = df["has_ticket"], df["has_mail"]
            df = df[{"tk_ml": tk & ml, "tk_only": tk & ~ml,
                     "ml_only": ~tk & ml, "none": ~tk & ~ml}[cell]]

    who = (request.args.get("who") or "").strip()
    if who in ("ana", "luiza", "juridico"):
        bits.append(next(p["label"] for p in bq.LEGAL_PEOPLE if p["key"] == who))
        if len(df):
            df = df[df[f"t_{who}"]]

    status = (request.args.get("status") or "").strip()
    if status:
        bits.append(status)
        if len(df):
            df = df[df["legal_status"] == status]

    people = {p["key"]: p["label"] for p in bq.LEGAL_PEOPLE}
    rows = []
    if len(df):
        df = df.sort_values("amount", ascending=False)
        for r in df.itertuples(index=False):
            # Canal explícito por pessoa: "Ana (ticket+e-mail)" responde de onde
            # o sinal veio sem obrigar a abrir o HubSpot pra descobrir.
            who_bits = []
            for key, tk_flag, n_mail in (
                    ("ana",      bool(r.tk_ana),   int(r.mail_ana)),
                    ("luiza",    bool(r.tk_luiza), int(r.mail_luiza)),
                    ("juridico", False,            int(r.mail_juridico))):
                if not (tk_flag or n_mail):
                    continue
                ch = "+".join([c for c, on in (("ticket", tk_flag), ("e-mail", n_mail > 0)) if on])
                who_bits.append(f"{people[key]} ({ch})")
            legal = r.legal_status
            if r.legal_stage_label:
                legal += f" · {r.legal_stage_label}"
            closed = pd.to_datetime(r.close_date, errors="coerce")
            _st = _STAGE_BY_NUM.get(int(r.max_stage_num))
            rows.append({
                "deal_name": r.deal_name or "—",
                "deal_url":  _hs_deal_url(r.deal_id),
                "ae":        r.account_executive,
                "outcome":   "Won" if r.is_won else "Lost",
                "amount":    float(r.amount or 0),
                "legal":     legal,
                "who":       " · ".join(who_bits) or "—",
                "tickets":   int(r.legal_tickets or 0),
                "deal_type": r.deal_type or "—",
                "stage_max": _st["label"] if _st else "—",
                "close":     closed.strftime("%d/%m/%Y") if pd.notna(closed) else "—",
            })

    n_won = sum(1 for x in rows if x["outcome"] == "Won")
    mrr_won = sum(x["amount"] for x in rows if x["outcome"] == "Won")
    sub = f"{len(rows)} deal{'s' if len(rows) != 1 else ''} fechado{'s' if len(rows) != 1 else ''}"
    if rows:
        sub += (f" · {n_won} won ({n_won / len(rows) * 100:.1f}%)".replace(".", ",")
                + f" · {fmt_money(mrr_won)} ganhos")
    show_ae = "leader" if (grain == "leader" or not f.get("ae")) else "ae"
    return jsonify({
        "title": " · ".join(bits) or "Deals fechados",
        "subtitle": sub,
        "columns": _with_ae_col(_DRILL_LEGAL_COLUMNS, show_ae),
        "rows": rows,
    })


@app.route("/pipeline/legal")
def pipeline_legal():
    return _pipeline_legal_page("ae")


@app.route("/leaders/pipeline/legal")
def leaders_pipeline_legal():
    return _pipeline_legal_page("leader")


def _tickets_parados_desde(force: bool = False) -> str:
    """'dd/mm' do último ticket no BQ quando o sync está parado há mais de
    `_EMAIL_CALL_STALE_DAYS` dias; '' quando está em dia ou não deu pra medir."""
    try:
        fr = bq.tickets_freshness(force_refresh=force)
        until = pd.to_datetime(fr.iloc[0]["ticket_until"], errors="coerce", utc=True)
    except Exception as e:  # noqa: BLE001
        app.logger.warning("tickets_freshness indisponível: %s", e)
        return ""
    if pd.isna(until):
        return ""
    dias = (pd.Timestamp.now(tz="UTC") - until).total_seconds() / 86400
    return f"{until.tz_convert(BRT):%d/%m}" if dias > _EMAIL_CALL_STALE_DAYS else ""


def _pipeline_legal_page(grain="ae"):
    """Ticket médio e conversão dos deals COM o jurídico envolvido contra os SEM.

    Universo = deals FECHADOS do Sales BR (ver `_legal_frame` pra por que só
    fechados). Dois eixos de escolha na página, ambos com consequência grande no
    número e por isso explícitos na tela em vez de escondidos no código:
    a DEFINIÇÃO de envolvimento (`env`: ticket · interação nominal · qualquer um)
    e a COORTE da comparação (`coh`: Negociação+ · todos), documentadas em
    `_LEGAL_ENVS` e `_LEGAL_COHORTS`.

    Quatro painéis, do mais agregado ao mais específico:
      1. o comparativo com/sem (cards) + o delta
      2. a matriz ticket × e-mail — os dois sinais são independentes e a célula
         "só e-mail" é a descoberta que a visão por ticket não mostra
      3. por pessoa (Ana · Luiza · juridico@), que é a sinalização nominal pedida
      4. por desfecho no jurídico (assinado · em andamento · perdido)
    """
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    df_all, mail_ok = _legal_frame(f, force=force)

    env = _legal_env(f.get("env", ""))
    # México: só o sinal por TICKET (pipeline Legal North America). A interação
    # nominal procura o jurídico do Brasil (Ana, Luiza, juridico@) e lá dá zero
    # por construção — o Pedro mandou desconsiderar (12/10/2026). Some a escolha
    # e somem os dois painéis do sinal nominal, em vez de mostrar zeros.
    nominal = not _is_mx()
    if not nominal:
        env = "ticket"
    coh = _legal_coh(f.get("coh", ""))
    df = _legal_cohort(df_all, coh)
    # Quantos ficaram de fora pela coorte — sem isso o total da aba muda de
    # tamanho ao trocar o chip e parece que o filtro de cima quebrou.
    n_fora_coorte = int(len(df_all) - len(df))

    mask = _legal_env_mask(df, env)
    com, sem = _legal_stats(df[mask]), _legal_stats(df[~mask])
    total = _legal_stats(df)

    # Delta: conversão em PONTOS PERCENTUAIS (a diferença entre duas taxas não é
    # uma taxa) e ticket médio em %, que é como se lê variação de preço.
    delta = {
        "conv_pp": ((com["conv"] - sem["conv"])
                    if (com["conv"] is not None and sem["conv"] is not None) else None),
        "ticket_pct": ((com["ticket"] / sem["ticket"] - 1) * 100
                       if (com["ticket"] and sem["ticket"]) else None),
    }

    # ---- `keep`: filtros ativos sem as chaves vazias ----
    # Montado aqui e não no template pelo mesmo motivo do Pipeline Total: url_for
    # com valor "" emite `?dtype=&source=` e vira chip fantasma na barra.
    keep = {k: v for k, v in f.items() if v and k != "role"}
    _drill = _legal_drill_args(keep, grain)

    def _url(**kw):
        """URL da própria página trocando um dos chips. Remove a chave quando o
        valor é o default, pra não sujar a URL com o estado que já é padrão."""
        args = {k: v for k, v in keep.items() if k not in kw}
        args.update({k: v for k, v in kw.items() if v})
        return url_for(request.endpoint, **args)

    # `_q` devolve a forma de QUERYSTRING de um corte: vazia quando é o default,
    # pra trocar um chip não gravar o valor padrão do OUTRO na URL.
    _q = lambda k, v: "" if v == _LEGAL_DEFAULTS[k] else v  # noqa: E731
    envs = [{"key": k, "active": k == env, "url": _url(env=_q("env", k), coh=_q("coh", coh)), **v}
            for k, v in _LEGAL_ENVS.items() if nominal or k == "ticket"]
    cohs = [{"key": k, "active": k == coh, "url": _url(coh=_q("coh", k), env=_q("env", env)), **v}
            for k, v in _LEGAL_COHORTS.items()]

    # ---- Painel 2: matriz ticket × e-mail ----
    # Os dois sinais cruzados, não somados: cada célula tem conversão e ticket
    # próprios, e é aí que "só e-mail" (jurídico acionado por fora do processo)
    # aparece como grupo com número em vez de virar ruído dentro de "sem ticket".
    tk, ml = df["has_ticket"], df["has_mail"]
    matrix = [
        {"key": "tk_ml",   "label": "Ticket + interação nominal", "tone": "green",
         "hint": "processo formal aberto e alguém do jurídico na conversa"},
        {"key": "tk_only", "label": "Só ticket",                  "tone": "teal",
         "hint": "ticket aberto, sem e-mail das três caixas na janela do deal"},
        {"key": "ml_only", "label": "Só interação nominal",       "tone": "amber",
         "hint": "jurídico na conversa sem ticket aberto — acionado por fora do processo"},
        {"key": "none",    "label": "Nenhum sinal",               "tone": "slate",
         "hint": "nem ticket nem e-mail do jurídico"},
    ]
    _sel = {"tk_ml": tk & ml, "tk_only": tk & ~ml, "ml_only": ~tk & ml, "none": ~tk & ~ml}
    for row in matrix:
        row.update(_legal_stats(df[_sel[row["key"]]]))
        row["share"] = (row["deals"] / total["deals"]) if total["deals"] else 0.0
        row["drill"] = url_for("pipeline_legal_deals", cell=row["key"], **_drill)

    # ---- Painel 3: por pessoa ----
    # Contra o TOTAL da coorte (e não entre si): os conjuntos se sobrepõem — o
    # mesmo deal costuma ter Ana e juridico@ — então as linhas não somam, e
    # apresentá-las como partes de um todo mentiria.
    people = []
    for p in bq.LEGAL_PEOPLE:
        col = f"t_{p['key']}"
        st = _legal_stats(df[df[col]])
        st.update({
            "key": p["key"], "label": p["label"], "email": p["email"],
            # Canal onde a pessoa é observável. Luiza e juridico@ não movem
            # estágio de ticket no HubSpot (0 toques nos 1.437 tickets do Legal),
            # então pra elas o único sinal É o e-mail — dizer isso evita a
            # leitura de que elas participaram menos.
            "channels": ("ticket + e-mail" if p["user_id"] and df[f"tk_{p['key']}"].any()
                         else "e-mail"),
            "drill": url_for("pipeline_legal_deals", who=p["key"], **_drill),
        })
        people.append(st)

    # ---- Painel 4: desfecho no jurídico ----
    # Ordem fixa (não por volume) pra a leitura seguir o caminho da minuta:
    # assinado → em andamento → perdido → sem ticket.
    _ORDER = ["Contrato assinado", "Minuta em andamento", "Perdido no jurídico",
              "Com ticket, estágio indefinido", "Sem ticket"]
    status_rows = []
    for s in _ORDER:
        g = df[df["legal_status"] == s]
        if not len(g):
            continue
        st = _legal_stats(g)
        st.update({
            "label": s,
            "drill": url_for("pipeline_legal_deals", status=s, **_drill),
            # Só faz sentido no bucket em andamento — nos outros o deal do
            # jurídico já terminou e não há "onde parou".
            "stages": (", ".join(sorted(g["legal_stage_label"].dropna().unique()))
                       if s == "Minuta em andamento" else ""),
        })
        status_rows.append(st)

    return render_template(
        "pipeline_legal.html", ctx=ctx, grain=grain,
        tickets_parados=_tickets_parados_desde(request.args.get("refresh") == "1"),
        env=env, coh=coh, envs=envs, cohs=cohs, nominal=nominal,
        com=com, sem=sem, total=total, delta=delta,
        matrix=matrix, people=people, status_rows=status_rows,
        mail_ok=mail_ok, n_fora_coorte=n_fora_coorte, keep=keep,
        drill_total=url_for("pipeline_legal_deals", **_drill),
        drill_com=url_for("pipeline_legal_deals", side="com", **_drill),
        drill_sem=url_for("pipeline_legal_deals", side="sem", **_drill),
    )


@app.route("/pipeline/funnel")
def pipeline_funnel():
    return _pipeline_funnel_page("ae")


@app.route("/leaders/pipeline/funnel")
def leaders_pipeline_funnel():
    return _pipeline_funnel_page("leader")


def _pipeline_funnel_page(grain="ae"):
    """Visão de funil WoW: pra cada stage do funil Sales BR, qtd de deals e
    amount no snapshot do último domingo 12h vs domingo retrasado 12h. Reusa
    `pipeline_wow_snapshot` (deal-level com state em cada T) e agrega por stage.
    grain='leader' → o detalhe por AE vira por líder (soma do time)."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # Janela igual ao PoP Early/Late: sem filtro = 2 últimos domingos; o filtro
    # de período do topo escolhe outro par de domingos (de = anterior, até = atual).
    last_sun, prev_sun = _wow_window(f)
    t_curr_iso = f"{last_sun.isoformat()} 15:00:00 UTC"
    t_prev_iso = f"{prev_sun.isoformat()} 15:00:00 UTC"

    df = bq.pipeline_wow_snapshot(t_curr_iso, t_prev_iso, force_refresh=force, pipeline=_pl())
    df = _apply_roster_current(df)           # roster Q3 + remove quem saiu (BQ traz líder cru)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        df = df[df["region"] == f["region"]]
    if f.get("leader"):
        df = df[df["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
        df = df[df["account_executive"] == f["ae"]]

    # Movimentação por TRANSIÇÃO (histórico completo na janela, não diff de
    # snapshots): captura o caminho INTRA-semana — deal criado e movido de
    # Scheduled a Scoping na mesma semana conta em cada etapa por onde passou
    # (o diff de pontas perdia esses). Uma linha por mudança de stage.
    tr = bq.funnel_transitions(t_curr_iso, t_prev_iso, force_refresh=force, pipeline=_pl())
    tr = _apply_roster_current(tr)
    tr = tr[tr["leader"].isin(ALLOWED_LEADERS)]
    tr = tr[~tr["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        tr = tr[tr["region"] == f["region"]]
    if f.get("leader"):
        tr = tr[tr["leader"].isin(expand_leader(f["leader"]))]
    if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
        tr = tr[tr["account_executive"] == f["ae"]]
    tr = tr.copy()
    tr["dir"] = tr.apply(lambda r: _funnel_dir(r["from_stage"], r["to_stage"]), axis=1)

    # Os 4 parâmetros do comportamento no período (definição do Pedro):
    #   entrou  = chegou na etapa (inclui deal criado na semana)
    #   avançou = saiu pra etapa seguinte (ou Won)
    #   lost    = saiu pra closedlost
    #   voltou  = regrediu a partir da etapa
    funnel_ids_set = {s[0] for s in _FUNNEL_STAGES}
    tr_in   = tr[tr["to_stage"].isin(funnel_ids_set)]
    tr_adv  = tr[(tr["dir"] == "advance") & tr["from_stage"].isin(funnel_ids_set)]
    tr_lost = tr[(tr["dir"] == "lost") & tr["from_stage"].isin(funnel_ids_set)]
    tr_back = tr[(tr["dir"] == "regress") & tr["from_stage"].isin(funnel_ids_set)]

    in_by_stage       = tr_in.groupby("to_stage").size()
    in_amt_by_stage   = tr_in.groupby("to_stage")["amount"].sum()
    adv_by_stage      = tr_adv.groupby("from_stage").size()
    lost_by_stage     = tr_lost.groupby("from_stage").size()
    back_by_stage     = tr_back.groupby("from_stage").size()
    adv_amt_by_stage  = tr_adv.groupby("from_stage")["amount"].sum()
    lost_amt_by_stage = tr_lost.groupby("from_stage")["amount"].sum()
    back_amt_by_stage = tr_back.groupby("from_stage")["amount"].sum()

    stages = []
    for stage_id, label in _FUNNEL_STAGES:
        m_curr = df["stage_curr"] == stage_id
        m_prev = df["stage_prev"] == stage_id
        n_curr = int(m_curr.sum())
        n_prev = int(m_prev.sum())
        a_curr = float(df.loc[m_curr, "amount_curr"].sum())
        a_prev = float(df.loc[m_prev, "amount_prev"].sum())
        stages.append({
            "id": stage_id,
            "label": label,
            "n_curr": n_curr, "n_prev": n_prev,
            "n_delta": n_curr - n_prev,
            "n_delta_pct": ((n_curr - n_prev) / n_prev * 100) if n_prev else None,
            "a_curr": a_curr, "a_prev": a_prev,
            "a_delta": a_curr - a_prev,
            "a_delta_pct": ((a_curr - a_prev) / a_prev * 100) if a_prev else None,
            "entrou":     int(in_by_stage.get(stage_id, 0)),
            "entrou_amt": float(in_amt_by_stage.get(stage_id, 0)),
            "adv":      int(adv_by_stage.get(stage_id, 0)),
            "lost":     int(lost_by_stage.get(stage_id, 0)),
            "voltou":   int(back_by_stage.get(stage_id, 0)),
            "adv_amt":  float(adv_amt_by_stage.get(stage_id, 0)),
            "lost_amt": float(lost_amt_by_stage.get(stage_id, 0)),
            "voltou_amt": float(back_amt_by_stage.get(stage_id, 0)),
        })

    # ----- Movimentação por AE × stage (estado absoluto + delta WoW + adv/lost) -----
    # Cada cell agrega:
    #   n_curr/a_curr  → quantos deals e qual amount o AE TEM agora no stage
    #   n_prev/a_prev  → quantos tinha no domingo anterior
    #   n_delta/a_delta = curr - prev (variação total)
    #   adv  → dos deals que estavam ali em t_prev, quantos avançaram (próx. stage ou Won)
    #   lost → dos deals que estavam ali em t_prev, quantos viraram closedlost
    # Pós-1:1: olha total atual + variação + decomposição pra ver se a saída foi por avanço ou perda.
    # No grain de líder, o detalhe por-entidade agrega por líder: explode cada deal
    # para os líderes-ancestrais (o total global `stages` acima continua do df cru).
    df_ent = _explode_leaders(df, f.get("leader")) if grain == "leader" else df
    funnel_ids = [s[0] for s in _FUNNEL_STAGES]
    cdf = df_ent[df_ent["stage_curr"].isin(funnel_ids)]
    pdf = df_ent[df_ent["stage_prev"].isin(funnel_ids)]

    def _pivot_dict(d, idx_col, val_col, agg):
        if len(d) == 0:
            return {}
        return d.pivot_table(index="account_executive", columns=idx_col,
                             values=val_col, aggfunc=agg, fill_value=0).to_dict()

    n_curr_d = _pivot_dict(cdf, "stage_curr", "deal_id",     "count")
    a_curr_d = _pivot_dict(cdf, "stage_curr", "amount_curr", "sum")
    n_prev_d = _pivot_dict(pdf, "stage_prev", "deal_id",     "count")
    a_prev_d = _pivot_dict(pdf, "stage_prev", "amount_prev", "sum")

    # Movimentos por AE × etapa — das TRANSIÇÕES (mesma régua do agregado).
    tr_ent = _explode_leaders(tr, f.get("leader")) if grain == "leader" else tr
    _t_in   = tr_ent[tr_ent["to_stage"].isin(funnel_ids_set)]
    _t_adv  = tr_ent[(tr_ent["dir"] == "advance") & tr_ent["from_stage"].isin(funnel_ids_set)]
    _t_lost = tr_ent[(tr_ent["dir"] == "lost") & tr_ent["from_stage"].isin(funnel_ids_set)]
    _t_back = tr_ent[(tr_ent["dir"] == "regress") & tr_ent["from_stage"].isin(funnel_ids_set)]
    in_grp       = _t_in.groupby(["account_executive", "to_stage"]).size()
    in_amt_grp   = _t_in.groupby(["account_executive", "to_stage"])["amount"].sum()
    adv_grp      = _t_adv.groupby(["account_executive", "from_stage"]).size()
    lost_grp     = _t_lost.groupby(["account_executive", "from_stage"]).size()
    back_grp     = _t_back.groupby(["account_executive", "from_stage"]).size()
    adv_amt_grp  = _t_adv.groupby(["account_executive", "from_stage"])["amount"].sum()
    lost_amt_grp = _t_lost.groupby(["account_executive", "from_stage"])["amount"].sum()
    back_amt_grp = _t_back.groupby(["account_executive", "from_stage"])["amount"].sum()

    all_aes = set()
    for dct in (n_curr_d, n_prev_d):
        for st, by_ae in dct.items():
            all_aes.update(by_ae.keys())
    all_aes |= set(in_grp.index.get_level_values(0))
    all_aes |= set(adv_grp.index.get_level_values(0))
    all_aes |= set(lost_grp.index.get_level_values(0))

    ae_data = []
    for ae in sorted(all_aes):
        cells = []
        total_adv  = 0
        total_lost = 0
        for stage_id, label in _FUNNEL_STAGES:
            nc = int(n_curr_d.get(stage_id, {}).get(ae, 0))
            ac = float(a_curr_d.get(stage_id, {}).get(ae, 0))
            np_ = int(n_prev_d.get(stage_id, {}).get(ae, 0))
            ap = float(a_prev_d.get(stage_id, {}).get(ae, 0))
            entrou     = int(in_grp.get((ae, stage_id), 0))
            entrou_amt = float(in_amt_grp.get((ae, stage_id), 0))
            adv      = int(adv_grp.get((ae, stage_id), 0))
            lost     = int(lost_grp.get((ae, stage_id), 0))
            voltou   = int(back_grp.get((ae, stage_id), 0))
            adv_amt  = float(adv_amt_grp.get((ae, stage_id), 0))
            lost_amt = float(lost_amt_grp.get((ae, stage_id), 0))
            voltou_amt = float(back_amt_grp.get((ae, stage_id), 0))
            cells.append({
                "stage_id": stage_id, "label": label,
                "abbr": _FUNNEL_STAGE_ABBR[stage_id],
                "n_curr": nc, "a_curr": ac,
                "n_prev": np_, "a_prev": ap,
                "n_delta": nc - np_,
                "a_delta": ac - ap,
                "n_delta_pct": ((nc - np_) / np_ * 100) if np_ else None,
                "a_delta_pct": ((ac - ap) / ap * 100) if ap else None,
                "entrou": entrou, "entrou_amt": entrou_amt,
                "adv": adv, "lost": lost,
                "voltou": voltou, "voltou_amt": voltou_amt,
                "adv_amt": adv_amt, "lost_amt": lost_amt,
            })
            total_adv  += adv
            total_lost += lost
        # Escala visual por AE (max entre prev e curr) — cada card normaliza
        # nas suas próprias barras pra não ficar minúsculo se o AE for pequeno.
        n_max_ae = max(max((c["n_curr"] for c in cells), default=0),
                       max((c["n_prev"] for c in cells), default=0), 1)
        a_max_ae = max(max((c["a_curr"] for c in cells), default=0),
                       max((c["a_prev"] for c in cells), default=0), 1.0)
        ae_data.append({
            "account_executive": ae,
            "adv":   total_adv,
            "lost":  total_lost,
            "total_mov": total_adv + total_lost,
            "win_rate": (total_adv / (total_adv + total_lost) * 100)
                        if (total_adv + total_lost) else None,
            "n_max": n_max_ae, "a_max": a_max_ae,
            "cells": cells,
        })
    # Filtra: só AEs com presença OU movimentação nos stages do funil
    ae_data = [a for a in ae_data
               if a["total_mov"] > 0 or any(c["n_curr"] > 0 or c["n_prev"] > 0 for c in a["cells"])]
    ae_data.sort(key=lambda r: (r["total_mov"], r["adv"]), reverse=True)

    n_max = max((s["n_curr"] for s in stages), default=1)
    a_max = max((s["a_curr"] for s in stages), default=1)
    n_max = max(n_max, 1)
    a_max = max(a_max, 1)

    totals = {
        "n_curr":  sum(s["n_curr"] for s in stages),
        "n_prev":  sum(s["n_prev"] for s in stages),
        "a_curr":  sum(s["a_curr"] for s in stages),
        "a_prev":  sum(s["a_prev"] for s in stages),
    }
    totals["n_delta"]     = totals["n_curr"] - totals["n_prev"]
    totals["a_delta"]     = totals["a_curr"] - totals["a_prev"]
    totals["n_delta_pct"] = (totals["n_delta"] / totals["n_prev"] * 100) if totals["n_prev"] else None
    totals["a_delta_pct"] = (totals["a_delta"] / totals["a_prev"] * 100) if totals["a_prev"] else None

    return render_template(
        "pipeline_funnel.html", ctx=ctx,
        stages=stages, totals=totals,
        n_max=n_max, a_max=a_max,
        ae_data=ae_data,
        snap_curr_label=_fmt_date_short(last_sun) + " · 12h",
        snap_prev_label=_fmt_date_short(prev_sun) + " · 12h",
        grain=grain,
    )


# ─────────────────────────────── Funil II ────────────────────────────────────
# Funil de conversão com denominador PRÓPRIO por etapa. O que o período recorta
# depende da BASE escolhida no chip (`base` na URL):
#
#   base=stage (padrão)  a base da etapa N são os deals que ENTRARAM nela (1ª
#                        entrada) dentro da janela. Responde "qual a chance de um
#                        deal que CHEGA em Negotiation ser ganho".
#   base=created         a base é uma coorte única — deals CRIADOS na janela —
#                        e a etapa N conta quantos dessa safra chegaram nela em
#                        algum momento. Responde "da safra criada no Q1, quanto
#                        andou e quanto virou Won". Funil decrescente por
#                        construção; safra recente é coorte imatura.
#
# Nas duas bases as taxas medem para onde esses deals foram DEPOIS — em qualquer
# momento, sem recortar o destino pela janela.
#
# Difere de propósito da conversão do Sales Overtime (_conv_series), que é
# snapshot na mesma janela nas duas pontas: lá o destino também precisa cair no
# período; aqui não. Duas perguntas diferentes, dois números diferentes.
#
# No Show (53862044) NÃO é etapa do funil e nem é lido por
# `bq.funnel_stage_entries` — um deal que passou por lá continua contando
# normalmente na etapa de onde veio e na que alcançou depois.
_F2_STAGES = [(s["num"], s["label"]) for s in STAGE_REGISTRY]        # 1..6
_F2_LABEL  = {num: lbl for num, lbl in _F2_STAGES}
_F2_LABEL[7]  = "Won"
_F2_LABEL[99] = "Lost"
# Colunas do funil: as 6 etapas + os 2 terminais. Todas com a MESMA régua de
# contagem dentro de uma base, pra as barras serem comparáveis entre si.
_F2_ROWS = _F2_STAGES + [(7, "Won"), (99, "Lost")]
# Bases do funil (chip da tela). A ordem é a de exibição.
_F2_BASES = [
    ("stage",   "Entrada na etapa"),
    ("created", "Criação do deal"),
]
# Cor por etapa (Radix scale 9, mesma família do Stale) + terminais verde/vermelho.
_F2_COLORS = {1: "#3E63DD", 2: "#00A2C7", 3: "#6E56CF", 4: "#D6409F",
              5: "#F76B15", 6: "#8B6914", 7: "#15803D", 99: "#B91C1C"}
# Métricas por etapa. `next` é resolvido por etapa (a seguinte de 6 é Won).
_F2_METRICS = ("base", "next", "won", "lost", "open")


def _f2_window(f):
    """Janela do funil = filtro de período global. Ponta vazia = aberta (sem
    limite), então "sem filtro" mostra o histórico inteiro em vez de nada."""
    lo = pd.Timestamp(f.get("date_from"), tz="UTC") if f.get("date_from") else None
    hi = (pd.Timestamp(f.get("date_to"), tz="UTC") + pd.Timedelta(days=1)
          ) if f.get("date_to") else None
    return lo, hi


def _f2_scope(f, force=False):
    """Pivot de entradas recortado pelos filtros da página: AE/líder e deal type.
    Reusa o `dtype` do Pipeline Total (mesmas chaves acq/exp na URL)."""
    piv = _get_funnel_entries(force=force)
    dt = (f.get("dtype") or "").strip()
    raw = next((r for k, r, *_ in _PT_DIMS["dtype"]["values"] if k == dt), None)
    if raw:
        piv = piv[piv["deal_type"] == raw]
    if f.get("ae"):
        piv = piv[piv["account_executive"] == f["ae"]]
    elif f.get("leader"):
        _aes = {a for a, ld in _current_leader_by_ae().items()
                if ld in expand_leader(f["leader"])}
        piv = piv[piv["account_executive"].isin(_aes)]
    return piv


def _f2_base_mode() -> str:
    """Base do funil vinda da URL. Qualquer valor estranho cai no padrão."""
    b = (request.args.get("base") or "").strip()
    return b if b in dict(_F2_BASES) else "stage"


def _f2_created_mask(piv, lo, hi):
    """Deals CRIADOS na janela. `created_date` é tz-naive no pivot e as bordas da
    janela são UTC-aware — comparar direto levanta TypeError, então converte."""
    cd = piv["created_date"]
    m = cd.notna()
    if lo is not None:
        m &= (cd >= lo.tz_localize(None))
    if hi is not None:
        m &= (cd < hi.tz_localize(None))
    return m.fillna(False)


def _f2_entered(piv, num, lo, hi, mode="stage"):
    """Base da etapa `num`, conforme a base escolhida:
      stage   → 1ª entrada na etapa dentro da janela;
      created → deal criado na janela que ALCANÇOU a etapa em algum momento."""
    ent = piv[f"s{num}"]
    if mode == "created":
        return (ent.notna() & _f2_created_mask(piv, lo, hi)).fillna(False)
    m = ent.notna()
    if lo is not None:
        m &= (ent >= lo)
    if hi is not None:
        m &= (ent < hi)
    return m.fillna(False)


def _f2_reached(piv, num, target):
    """Da coorte de `num`, quem alcançou `target` DEPOIS (>= a entrada em num).
    O `>=` (e não `>`) é de propósito: movimentações em lote gravam o mesmo
    timestamp nas duas etapas, e com `>` esses deals sumiriam do numerador."""
    ent, tgt = piv[f"s{num}"], piv[f"s{target}"]
    return (tgt.notna() & ent.notna() & (tgt >= ent)).fillna(False)


def _f2_next_of(num):
    """Etapa seguinte no funil. Depois de Procurement (6) o próximo passo é Won."""
    return num + 1 if num < 6 else 7


def _f2_mask(piv, num, metric, lo, hi, mode="stage"):
    """Máscara de uma célula do funil — a MESMA usada pelo número na tela e pelo
    drill, pra lista explodida e total nunca divergirem."""
    base = _f2_entered(piv, num, lo, hi, mode)
    if metric == "base" or num in (7, 99):
        return base
    if metric == "next":
        return base & _f2_reached(piv, num, _f2_next_of(num))
    if metric == "won":
        return base & _f2_reached(piv, num, 7)
    if metric == "lost":
        return base & _f2_reached(piv, num, 99)
    if metric == "open":
        # Em aberto = a coorte que não chegou a nenhum terminal. Não é
        # `n - won - lost`: um deal reaberto pode ter passado pelos dois, e a
        # subtração devolveria negativo.
        return base & ~_f2_reached(piv, num, 7) & ~_f2_reached(piv, num, 99)
    return pd.Series(False, index=piv.index)


def _f2_period_label(f):
    a, b = f.get("date_from"), f.get("date_to")
    if a and b:
        return f"{_fmt_date_short(pd.to_datetime(a).date())} → {_fmt_date_short(pd.to_datetime(b).date())}"
    if a:
        return f"desde {_fmt_date_short(pd.to_datetime(a).date())}"
    if b:
        return f"até {_fmt_date_short(pd.to_datetime(b).date())}"
    return "todo o histórico"


def _f2_base_label(mode: str) -> str:
    """Como o período está sendo aplicado — vai pro subtítulo e pro drill, pra um
    print da tela nunca ficar ambíguo sobre qual base gerou os números."""
    return "criados em" if mode == "created" else "entrada na etapa em"


@app.route("/pipeline/funnel-ii")
def pipeline_funnel2():
    return _pipeline_funnel2_page("ae")


@app.route("/leaders/pipeline/funnel-ii")
def leaders_pipeline_funnel2():
    return _pipeline_funnel2_page("leader")


def _pipeline_funnel2_page(grain="ae"):
    """Funil de vendas com 3 taxas por etapa (→ etapa seguinte, → Won, → Lost).
    A base do período — entrada na etapa ou criação do deal — vem do chip."""
    ctx = base_context()
    f = ctx["filters"]
    mode = _f2_base_mode()
    piv = _f2_scope(f, force=request.args.get("refresh") == "1")
    lo, hi = _f2_window(f)

    # Filtros que os drills precisam carregar pra bater com a tela. `base` entra
    # à mão: não é filtro global (não vive em ctx.filters), mas muda os números.
    keep = {k: v for k, v in f.items() if v and k in
            ("date_from", "date_to", "leader", "ae", "dtype", "money")}
    keep["base"] = mode

    def _drill(num, metric):
        return url_for("pipeline_funnel2_deals", stage=num, metric=metric, **keep)

    # URLs do chip de base — preserva todo o resto da querystring.
    _bargs = {k: v for k, v in request.args.items() if v and k not in ("base", "refresh")}
    bases = [{"key": k, "label": lbl, "active": (k == mode),
              "url": url_for(request.endpoint, base=k, **_bargs)}
             for k, lbl in _F2_BASES]

    rows = []
    universe = pd.Series(False, index=piv.index)     # deals distintos analisados
    for num, label in _F2_ROWS:
        base = _f2_mask(piv, num, "base", lo, hi, mode)
        n = int(base.sum())
        universe |= base
        row = {
            "num": num, "label": label, "n": n,
            "amount": float(piv.loc[base, "amount"].sum()),
            "color": _F2_COLORS[num],
            "is_terminal": num in (7, 99),
            "drill": _drill(num, "base"),
        }
        if num not in (7, 99):
            # Duas flags porque funil e tabela mostram o passo em lugares
            # diferentes: `has_step` é a taxa colada na borda entre duas faixas
            # do funil (existe até em Procurement, cujo passo É o Won, e aí a
            # pílula de Won da linha é suprimida via `won_in_step` pra não sair
            # duplicada); `has_next` é a coluna "Próxima etapa" da tabela, que
            # em Procurement fica "—" porque a coluna Won ao lado já traz o número.
            row["has_step"] = num <= 6
            row["won_in_step"] = (num == 6)
            row["has_next"] = num < 6
            row["next_label"] = _F2_LABEL[_f2_next_of(num)]
            for metric in ("next", "won", "lost", "open"):
                k = int(_f2_mask(piv, num, metric, lo, hi, mode).sum())
                row[metric] = k
                row[f"{metric}_pct"] = (100.0 * k / n) if n else None
                row[f"{metric}_drill"] = _drill(num, metric)
        rows.append(row)

    stage_rows = [r for r in rows if not r["is_terminal"]]

    # ---- Geometria do funil ----
    # Faixas = as 6 etapas + Won como a PONTA. Lost fica fora da forma de
    # propósito: como quase todo deal morre, a faixa dele reabriria o funil no
    # fim e a silhueta deixaria de ser um funil. A perda aparece na pílula Lost
    # de cada etapa, no card do topo e na tabela.
    bands = [r for r in rows if r["num"] != 99]
    b_max = max([r["n"] for r in bands], default=0) or 1
    for i, r in enumerate(bands):
        # Cada faixa é um trapézio: largura do TOPO = ela mesma, largura da BASE
        # = a faixa seguinte. Empilhadas sem gap, o afunilamento fica contínuo.
        nxt_n = bands[i + 1]["n"] if i + 1 < len(bands) else r["n"]
        r["w_top"] = 100.0 * r["n"] / b_max
        r["w_bot"] = 100.0 * nxt_n / b_max

    total_n = int(universe.sum())
    won_row  = next(r for r in rows if r["num"] == 7)
    lost_row = next(r for r in rows if r["num"] == 99)
    closed = won_row["n"] + lost_row["n"]

    return render_template(
        "pipeline_funnel2.html", ctx=ctx, grain=grain,
        rows=rows, stage_rows=stage_rows, bands=bands,
        total_n=total_n, total_amount=float(piv.loc[universe, "amount"].sum()),
        won_row=won_row, lost_row=lost_row,
        win_rate=(100.0 * won_row["n"] / closed) if closed else None,
        period_label=_f2_period_label(f), bases=bases, base_mode=mode,
        base_label=_f2_base_label(mode),
        total_drill=url_for("pipeline_funnel2_deals", stage="all", metric="base", **keep),
    )


@app.route("/pipeline/funnel-ii/deals")
def pipeline_funnel2_deals():
    """Drill do Funil II: os deals por trás de qualquer célula (base da etapa ou
    uma das 3 taxas). Mesma máscara do número clicado."""
    ctx_f = get_filters()
    mode = _f2_base_mode()
    piv = _f2_scope(ctx_f)
    lo, hi = _f2_window(ctx_f)
    stage  = (request.args.get("stage") or "all").strip()
    metric = (request.args.get("metric") or "base").strip()
    if metric not in _F2_METRICS:
        metric = "base"

    if stage == "all":
        sel_mask = pd.Series(False, index=piv.index)
        for num, _lbl in _F2_ROWS:
            sel_mask |= _f2_mask(piv, num, "base", lo, hi, mode)
        title = "Todas as oportunidades"
    else:
        num = _safe_int(stage, 0)
        if num not in _F2_LABEL:
            return jsonify(_empty_drill(""))
        sel_mask = _f2_mask(piv, num, metric, lo, hi, mode)
        suffix = {
            "base": "", "open": " · em aberto",
            "next": f" → {_F2_LABEL[_f2_next_of(num)]}",
            "won": " → Won", "lost": " → Lost",
        }.get(metric, "")
        title = f"{_F2_LABEL[num]}{suffix}"

    sel = piv[sel_mask]
    _tipo = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}
    rows = []
    for _, r in sel.iterrows():
        rows.append({
            "deal_name": r.get("deal_name") or "—",
            "deal_url":  _hs_deal_url(r.get("deal_id")),
            "ae":        r.get("account_executive") or "—",
            "tipo":      _tipo.get(r.get("deal_type"), r.get("deal_type") or "—"),
            "amount":    float(r.get("amount") or 0),
        })
    rows.sort(key=lambda x: x["amount"], reverse=True)
    return jsonify({
        # A base entra no título: sem ela a mesma lista de deals apareceria com
        # dois totais diferentes e nada na modal explicaria a diferença.
        "title": f"{title} · {_f2_base_label(mode)} {_f2_period_label(ctx_f)}",
        "subtitle": _drill_money_subtitle(len(rows), sum(x["amount"] for x in rows)),
        "columns": [
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "ae",        "label": "AE"},
            {"key": "tipo",      "label": "Tipo"},
            {"key": "amount",    "label": "MRR", "type": "money"},
        ],
        "rows": rows,
    })


@app.route("/pipeline/wow/early")
def pipeline_wow_early():
    return _render_pipeline_wow("early")


@app.route("/pipeline/wow/late")
def pipeline_wow_late():
    return _render_pipeline_wow("late")


@app.route("/leaders/pipeline/wow/early")
def leaders_pipeline_wow_early():
    return _render_pipeline_wow("early", grain="leader")


@app.route("/leaders/pipeline/wow/late")
def leaders_pipeline_wow_late():
    return _render_pipeline_wow("late", grain="leader")


def _wow_events_categorize(ev, focus):
    """Classifica cada EVENTO da janela WoW (de `bq.wow_events`) e calcula o
    `delta` que ele provocou no pipe do foco. Substituiu a classificação por
    diff das 2 pontas (que vivia aqui como `_wow_categorize`): comparar só as
    pontas perdia o caminho intra-semana e, no pior caso, PERDIA O DEAL — criado
    na quarta e ganho na sexta não casava com nenhuma máscara e saía como
    'irrelevant'/delta 0. Na janela 26/07→02/08 eram 22 deals no Early e 4 no
    Late, e a ponte errava R$ 42 mil (Early) e R$ 945 mil (Late).

    O delta NÃO vem da categoria — vem de `v_depois − v_antes`, com
    `v = amount se o deal está no foco, senão 0`. Somado sobre TODOS os eventos
    isso telescopa exatamente em `v(t_curr) − v(t_prev)`, então a identidade
      total_prev + Σdelta == total_curr
    fecha por construção (validada em 0,00 nos dois focos). A categoria é só o
    RÓTULO do evento, pra explicar o número na tela — nunca a origem dele.

    Categorias: entradas no foco = novo (de none/pre) · transferiu (do outro
    grupo) · reabriu (de won/lost); saídas = won · lost · regrediu (voltou pra
    pre/none) · late (só no foco Early: avançou pra Late) / transferiu (no Late);
    valor mexendo DENTRO do foco = cresceu / encolheu."""
    other = "late" if focus == "early" else "early"
    ev = ev.copy()
    is_stage = ev["kind"] == "stage"
    gf, gt = ev["stage_from"].apply(_hs_stage_group), ev["stage_to"].apply(_hs_stage_group)
    gat = ev["stage_at"].apply(_hs_stage_group)
    amt = ev["amount_at"].astype(float)
    a_from, a_to = ev["amt_from"].astype(float), ev["amt_to"].astype(float)

    # Evento de estágio move o deal PARA DENTRO/FORA do foco com o valor
    # vigente; evento de valor mexe no montante sem sair do lugar.
    v_ante = amt.where(gf == focus, 0.0).where(is_stage, a_from.where(gat == focus, 0.0))
    v_pos  = amt.where(gt == focus, 0.0).where(is_stage, a_to.where(gat == focus, 0.0))
    ev["delta"] = v_pos - v_ante

    ev["category"] = "irrelevant"
    ent = is_stage & (gt == focus) & (gf != focus)
    sai = is_stage & (gf == focus) & (gt != focus)
    is_amt = (~is_stage) & (gat == focus)
    d = a_to - a_from
    # "novo" = CHEGOU no foco, não importa de onde. No Early isso é só vindo de
    # pre/none (o caminho normal); no LATE inclui quem veio do Early, que é o
    # caminho normal DELE. Sem isso a página Late escondia a entrada inteira:
    # o avanço Early→Late é card de destaque no Early ("Movido p/ Late",
    # R$ 1,08M/48 deals em 26/07→02/08) mas caía em 'transferiu' no Late, que
    # fica fora do saldo e do gráfico — só aparecia na linha do rodapé. O Late
    # exibia "Novo deal 0" e saldo −R$ 311k enquanto o pipe dele CRESCEU
    # R$ 685k. Decisão do Pedro (2026-08-03): contar como Novo, simétrico ao
    # Early. Assim `transferiu` sobra só pra volta Late→Early, que é regressão.
    ent_de_fora = ent & gf.isin(["none", "pre"])
    ent_do_outro = ent & (gf == other)
    if focus == "late":
        ent_novo, ent_xfer = ent_de_fora | ent_do_outro, ent & (gf == "__nunca__")
    else:
        ent_novo, ent_xfer = ent_de_fora, ent_do_outro
    for m, c in [
        (ent_novo,                        "novo"),
        (ent & gf.isin(["won", "lost"]),  "reabriu"),
        (ent_xfer,                        "transferiu"),
        (sai & (gt == "won"),             "won"),
        (sai & (gt == "lost"),            "lost"),
        (sai & gt.isin(["none", "pre"]),  "regrediu"),
        (sai & (gt == other),             "late" if focus == "early" else "transferiu"),
        (is_amt & (d > 0),                "cresceu"),
        (is_amt & (d < 0),                "encolheu"),
    ]:
        ev.loc[m, "category"] = c
    return ev


_WOW_CAT_LABEL = {"novo": "Novo", "cresceu": "Cresceu", "encolheu": "Encolheu",
                  "lost": "Lost", "won": "Won", "late": "Movido p/ Late",
                  "transferiu": "Transferiu", "regrediu": "Voltou de estágio",
                  "reabriu": "Reabriu"}

# Categorias que somam o SALDO do período (pipe que entrou/saiu de verdade).
# won/late/transferiu/regrediu/reabriu ficam fora: são movimento de fronteira,
# aparecem na ponte auditável do rodapé, não no saldo.
_WOW_NET_CATS = ("novo", "cresceu", "encolheu", "lost")


def _wow_event_rows(ev):
    """Linhas do drill a partir de eventos JÁ categorizados — uma linha por
    EVENTO, não por deal: o mesmo deal aparece 2× se cresceu e depois avançou na
    mesma semana, que é justamente o que o diff de pontas escondia.

    Compartilhado entre o drill da PoP (par de domingos) e o do Histórico (uma
    semana da série): os dois explodem o mesmo tipo de frame, e duas cópias
    divergiriam na primeira mudança de coluna."""
    def _lbl(sid):
        return stage_display(sid) if sid else "—"

    def _quando(ts):
        """Timestamp do evento em BRT — as telas falam em "domingo 12h BRT",
        então mostrar UTC aqui confundiria a leitura. UTC−3 fixo, a mesma
        convenção que o resto do PoP já usa ao montar os snapshots como 15h UTC."""
        try:
            t = pd.Timestamp(ts)
            if t.tzinfo is not None:
                t = t.tz_convert(None)
            return (t - timedelta(hours=3)).strftime("%d/%m %H:%M")
        except (ValueError, TypeError, AttributeError):
            return "—"

    rows = []
    for _, r in ev.iterrows():
        if r["kind"] == "stage":
            mov = f"{_lbl(r['stage_from'])} → {_lbl(r['stage_to'])}"
            val = float(r["amount_at"])
        else:
            mov = f"{_lbl(r['stage_at'])} · valor alterado"
            val = float(r["amt_to"])
        rows.append({
            "deal_name": r.get("deal_name") or "—",
            "deal_url":  _hs_deal_url(r.get("deal_id")),
            "ae":        r["account_executive"],
            "categoria": _WOW_CAT_LABEL.get(r["category"], r["category"]),
            "movimento": mov,
            "quando":    _quando(r["ts"]),
            "delta":     float(r["delta"]),
            "amount":    val,
        })
    rows.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return rows


def _wow_drill_columns(grain, name):
    """Colunas do drill de eventos PoP — idênticas na PoP e no Histórico."""
    return _with_ae_col([
        {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
        {"key": "categoria", "label": "Movimento"},
        {"key": "movimento", "label": "Estágio (de → para)"},
        {"key": "quando",    "label": "Quando", "align": "right"},
        {"key": "delta",     "label": "Δ (R$)", "type": "money_signed"},
        {"key": "amount",    "label": "Amount no evento", "type": "money"},
    ], "leader" if (grain == "leader" or not name) else "ae")


def _wow_snapshot_filtered():
    """Snapshot WoW dos 2 domingos comparados (padrão: os 2 últimos), com
    roster/allowed/hidden aplicados. Só as barras de ESTADO (prev/curr) usam
    isto — o movimento vem de `_wow_events_filtered`."""
    last_sun, prev_sun = _wow_window(get_filters())
    tc = f"{last_sun.isoformat()} 15:00:00 UTC"
    tp = f"{prev_sun.isoformat()} 15:00:00 UTC"
    df = bq.pipeline_wow_snapshot(tc, tp, pipeline=_pl())
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = drop_departed_people(df)          # gestor fica (ver _render_pipeline_wow)
    return df


def _wow_events_filtered(force=False):
    """Eventos da janela WoW com o MESMO recorte do snapshot. Janela FIXA
    (`_wow_fixed_pair`), a mesma da página — o drill não pode cair num par de
    domingos diferente do gráfico que foi clicado."""
    last_sun, prev_sun = _wow_fixed_pair()
    tc = f"{last_sun.isoformat()} 15:00:00 UTC"
    tp = f"{prev_sun.isoformat()} 15:00:00 UTC"
    ev = bq.wow_events(tc, tp, force_refresh=force, pipeline=_pl())
    ev = _apply_roster_current(ev)
    ev = ev[ev["leader"].isin(ALLOWED_LEADERS)]
    ev = drop_departed_people(ev)          # gestor fica (ver _render_pipeline_wow)
    # Deal Type também aqui: a lista explodida tem que ser o recorte da tela, e
    # não o total — é o bug clássico de drill que discorda da barra clicada.
    return _pt_apply_dim_filter(ev, "dtype", get_filters().get("dtype", ""))


@app.route("/pipeline/wow/<focus>/deals")
def pipeline_wow_deals(focus):
    """Drill-down: deals que se movimentaram (novo/cresceu/encolheu/lost/won) no
    período de um AE/líder, no foco early/late.

    `cat` restringe as categorias (uma, ou várias separadas por vírgula). Toda
    explosão mostra SÓ o que compõe o número clicado: o card de saldo manda as 4
    que somam o saldo (sem won), o card de won manda só won. Sem `cat`, traz as 5."""
    if focus not in ("early", "late"):
        return jsonify(_empty_drill(""))
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    # 5ª categoria depende do foco: Early acompanha o que AVANÇOU pra Late; o
    # won só existe como card/coluna no foco Late.
    _all = ["novo", "cresceu", "encolheu", "lost",
            "late" if focus == "early" else "won"]
    _pedido = (request.args.get("cat") or "").strip()
    if _pedido:
        pedidas = [c.strip().lower() for c in _pedido.split(",")]
        # `cat` pedido mas inválido pra este foco → lista VAZIA, nunca "todas".
        # Cair em todas mostraria deals que não compõem o número clicado.
        cats = [c for c in _all if c in pedidas]
    else:
        cats = list(_all)
    cat = cats[0] if len(cats) == 1 else ""
    ev = _entity_filter(_wow_events_filtered(), name, grain,
                        request.args.get("leader"), request.args.get("region"))
    ev = _wow_events_categorize(ev, focus)
    ev = ev[ev["category"].isin(cats)]

    rows = _wow_event_rows(ev)
    # Saldo = MESMA conta da página: só o que aumentou/diminuiu de pipe. A 5ª
    # categoria (won no Late, movido-pra-Late no Early) fica fora e aparece à
    # parte, senão o subtítulo divergia do card "Saldo do período".
    _fifth_lbls = (_WOW_CAT_LABEL["won"], _WOW_CAT_LABEL["late"])
    net = sum(x["delta"] for x in rows if x["categoria"] not in _fifth_lbls)
    won = -sum(x["delta"] for x in rows if x["categoria"] in _fifth_lbls)
    # Linha = evento, então "N deals" tem que contar deals DISTINTOS; senão um
    # deal que se moveu 2× na semana seria anunciado como 2 deals. Quando os dois
    # números diferem, mostra os dois — é o intra-semana ficando visível.
    n_deals = int(ev["deal_id"].nunique())
    _d = f"{n_deals} deal{'s' if n_deals != 1 else ''}"
    if len(rows) != n_deals:
        _d += f" · {len(rows)} movimentos"
    if cat == "won":
        subtitle = f"{_d} won · {fmt_money(won)}" + " que saiu do pipe"
    elif cat == "late":
        subtitle = f"{_d} · {fmt_money(won)}" + " movidos pra Late stage"
    elif cat:
        # uma categoria só (novo/cresceu/encolheu/lost) → soma dela, não "saldo"
        _v = sum(x["delta"] for x in rows)
        subtitle = (f"{_d} · "
                    f"{'+' if _v >= 0 else '−'}{fmt_money(abs(_v))}")
    else:
        subtitle = (f"{_d} · saldo "
                    f"{'+' if net >= 0 else '−'}{fmt_money(abs(net))}")
        if won:
            _w_lbl = "movido p/ Late" if focus == "early" else "won"
            subtitle += f" · {_w_lbl} {fmt_money(won)}"
    _escopo = name or f"PoP {focus} — total"
    _sufixo = _WOW_CAT_LABEL.get(cat, "") if cat else ("" if len(cats) == 5 else "Saldo")
    return jsonify({
        "title": (f"{_escopo} · {_sufixo}" if _sufixo else _escopo),
        "subtitle": subtitle,
        "columns": _wow_drill_columns(grain, name),
        "rows": rows,
    })


def _funnel_moves_scope():
    """TRANSIÇÕES de estágio dentro da janela WoW (histórico completo — inclui
    o caminho intra-semana de deals criados e movidos na mesma semana), com o
    recorte de name/grain/líder/região aplicado. Compartilhado entre o drill
    por etapa (nível 1) e a lista de deals de uma etapa (nível 2)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    last_sun, prev_sun = _wow_window(get_filters())
    tr = bq.funnel_transitions(f"{last_sun.isoformat()} 15:00:00 UTC",
                               f"{prev_sun.isoformat()} 15:00:00 UTC", pipeline=_pl())
    tr = _apply_roster_current(tr)
    tr = tr[tr["leader"].isin(ALLOWED_LEADERS)]
    tr = tr[~tr["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    tr = _entity_filter(tr, name, grain,
                        request.args.get("leader"), request.args.get("region"))
    tr = tr.copy()
    tr["dir"] = tr.apply(lambda r: _funnel_dir(r["from_stage"], r["to_stage"]), axis=1)
    funnel_ids = {s[0] for s in _FUNNEL_STAGES}
    return tr, funnel_ids, last_sun, prev_sun, name, grain


@app.route("/pipeline/funnel/moves")
def pipeline_funnel_moves():
    """Nível 1 do drill do Funil WoW: movimentação POR ETAPA — Entrou ·
    Avançou · Lost · Voltou, cada número clicável abre o nível 2 só daquela
    etapa. Base = transições (um deal criado e movido na semana conta em cada
    etapa por onde passou)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    tr, funnel_ids, last_sun, prev_sun, name, grain = _funnel_moves_scope()

    _carry = {k: v for k, v in request.args.items() if v and k != "money"}
    def _u(stage, direction):
        return url_for("pipeline_funnel_move_deals", **{**_carry, "stage": stage, "dir": direction})

    rows = []
    tot = {"entrou": 0, "avancou": 0, "lost": 0, "voltou": 0}
    for sid, label in _FUNNEL_STAGES:
        n_in   = int((tr["to_stage"] == sid).sum())
        m_from = tr["from_stage"] == sid
        n_adv  = int((m_from & (tr["dir"] == "advance")).sum())
        n_lost = int((m_from & (tr["dir"] == "lost")).sum())
        n_back = int((m_from & (tr["dir"] == "regress")).sum())
        for k, v in (("entrou", n_in), ("avancou", n_adv), ("lost", n_lost), ("voltou", n_back)):
            tot[k] += v
        rows.append({"estagio": label,
                     "entrou": n_in, "avancou": n_adv, "lost": n_lost, "voltou": n_back,
                     "d_in": _u(sid, "entrou") if n_in else None,
                     "d_adv": _u(sid, "avancou") if n_adv else None,
                     "d_lost": _u(sid, "lost") if n_lost else None,
                     "d_back": _u(sid, "voltou") if n_back else None})
    rows.append({"estagio": "Total (todas as etapas)",
                 "entrou": tot["entrou"], "avancou": tot["avancou"],
                 "lost": tot["lost"], "voltou": tot["voltou"],
                 "d_in": _u("all", "entrou"), "d_adv": _u("all", "avancou"),
                 "d_lost": _u("all", "lost"), "d_back": _u("all", "voltou")})
    return jsonify({
        "title": (name or "Funil — total") + " · movimentação por etapa",
        "subtitle": (f"{_fmt_date_short(prev_sun)} → {_fmt_date_short(last_sun)} · "
                     "clique num número pra ver os deals daquela etapa"),
        "columns": [
            {"key": "estagio", "label": "Etapa"},
            {"key": "entrou",  "label": "Entrou",  "type": "int", "drill": "d_in"},
            {"key": "avancou", "label": "Avançou", "type": "int", "drill": "d_adv"},
            {"key": "lost",    "label": "Lost",    "type": "int", "drill": "d_lost"},
            {"key": "voltou",  "label": "Voltou",  "type": "int", "drill": "d_back"},
        ],
        "rows": rows,
    })


@app.route("/pipeline/funnel/moves/deals")
def pipeline_funnel_move_deals():
    """Nível 2 do drill do Funil WoW: as transições de UMA etapa específica.
    `dir` = entrou | avancou | lost | voltou (sem dir = tudo que tocou a etapa,
    com a coluna Lado dizendo se entrou ou saiu). stage=all → todas as etapas."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    tr, funnel_ids, last_sun, prev_sun, name, grain = _funnel_moves_scope()
    stage = (request.args.get("stage") or "all").strip()
    direction = (request.args.get("dir") or "").strip()

    m_to   = tr["to_stage"].isin(funnel_ids) if stage == "all" else (tr["to_stage"] == stage)
    m_from = tr["from_stage"].isin(funnel_ids) if stage == "all" else (tr["from_stage"] == stage)
    if direction == "entrou":
        sel = tr[m_to]
    elif direction == "avancou":
        sel = tr[m_from & (tr["dir"] == "advance")]
    elif direction == "lost":
        sel = tr[m_from & (tr["dir"] == "lost")]
    elif direction == "voltou":
        sel = tr[m_from & (tr["dir"] == "regress")]
    else:
        sel = tr[m_to | m_from]

    def _lbl(sid):
        if sid is None or (isinstance(sid, float) and pd.isna(sid)):
            return "— (criado)"
        return stage_display(sid)

    def _classify(r):
        if r["from_stage"] is None or pd.isna(r["from_stage"]):
            return "Criado (novo deal)"
        d = r["dir"]
        if d == "advance":
            return "Won" if r["to_stage"] == "closedwon" else "Avançou"
        if d == "lost":
            return "Perdeu (Lost)"
        if d == "regress":
            return "Voltou"
        return "Entrou no funil"

    tmap = _deal_type_map()
    rows = []
    for _, r in sel.iterrows():
        row = {
            "deal_name": r.get("deal_name") or "—",
            "deal_url":  _hs_deal_url(r.get("deal_id")),
            "ae":        r["account_executive"],
            "de":        _lbl(r.get("from_stage")),
            "para":      _lbl(r.get("to_stage")),
            "classe":    _classify(r),
            "deal_type": tmap.get(str(r["deal_id"]), "—") or "—",
            "amount":    float(r["amount"] or 0),
            "quando":    pd.to_datetime(r["ts"]).date().isoformat() if pd.notna(r["ts"]) else "",
        }
        if not direction and stage != "all":
            row["lado"] = "Entrou" if r.get("to_stage") == stage else "Saiu"
        rows.append(row)
    rows.sort(key=lambda x: x["amount"], reverse=True)

    _stage_lbl = "todas as etapas" if stage == "all" else stage_display(stage)
    _titles = {"entrou": f"Entrou em {_stage_lbl}", "avancou": f"Avançou de {_stage_lbl}",
               "lost": f"Lost saindo de {_stage_lbl}", "voltou": f"Voltou de {_stage_lbl}"}
    title = _titles.get(direction, f"Movimentações · {_stage_lbl}") + (f" · {name}" if name else "")
    cols = [{"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"}]
    if not direction and stage != "all":
        cols.append({"key": "lado", "label": "Lado"})
    cols += [
        {"key": "de",        "label": "De"},
        {"key": "para",      "label": "Para"},
        {"key": "classe",    "label": "Movimento"},
        {"key": "deal_type", "label": "Tipo"},
        {"key": "amount",    "label": "Valor",  "type": "money"},
        {"key": "quando",    "label": "Quando", "type": "date"},
    ]
    return jsonify({
        "title": title,
        "subtitle": (f"{_fmt_date_short(prev_sun)} → {_fmt_date_short(last_sun)} · "
                     + _drill_money_subtitle(len(rows), float(sum(x['amount'] for x in rows)))),
        "columns": _with_ae_col(cols, "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


@app.route("/pipeline/funnel/deals")
def pipeline_funnel_deals():
    """Drill-down: deals abertos de um AE/líder num estágio do funil (estado atual
    do snapshot WoW). `stage` = grupo do funil (early/late) ou id de stage."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    stage = request.args.get("stage") or ""
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    df = _entity_filter(_wow_snapshot_filtered(), name, grain,
                        request.args.get("leader"), request.args.get("region"))
    df = df.copy()
    df["gp_curr"] = df["stage_curr"].apply(_hs_stage_group)
    if stage in ("early", "late"):
        df = df[df["gp_curr"] == stage]
    elif stage:                       # id de stage específico do funil
        df = df[df["stage_curr"] == stage]
    else:                             # sem stage → todo o funil aberto (early/late)
        df = df[df["gp_curr"].isin(["early", "late"])]
    df = df.sort_values("amount_curr", ascending=False)
    tmap = _deal_type_map()
    rows = [{
        "deal_name": r.get("deal_name") or "—",
        "deal_url":  _hs_deal_url(r["deal_id"]),
        "ae":        r["account_executive"],
        "deal_type": tmap.get(str(r["deal_id"]), "—") or "—",
        "stage":     stage_display(r["stage_curr"]),
        "amount":    float(r["amount_curr"] or 0),
    } for _, r in df.iterrows()]
    amount = float(sum(x["amount"] for x in rows))
    _stage_lbl = _STAGE_BY_ID.get(stage, {}).get("label", stage) if stage not in ("", "early", "late") else stage
    return jsonify({
        "title": name or ("Funil" + (f" · {_stage_lbl}" if stage else "") + " — total"),
        "subtitle": _drill_money_subtitle(len(rows), amount) + " no funil (aberto)",
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal",     "wrap": True, "link": "deal_url"},
            {"key": "deal_type", "label": "Tipo"},
            {"key": "stage",     "label": "Estágio"},
            {"key": "amount",    "label": "Valor",    "type": "money"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


def _render_pipeline_wow(focus: str, grain="ae"):
    """Snapshot do pipeline em 2 domingos consecutivos (12h BRT) e o que aconteceu
    deal-a-deal entre eles. `focus` ∈ {'early','late'} — define o universo
    (deals nesse grupo em pelo menos um dos snapshots).

    Categorias por deal:
      + Novo (none/pre → focus)         → +amount_curr
      + Cresceu (focus → focus, ↑amt)   → +Δ
      − Encolheu (focus → focus, ↓amt)  → Δ negativo
      − Lost (focus → Lost)             → −amount_prev
      ▣ Won (focus → Won)               → −amount_prev (categoria à parte; fora do net)
      Transferiu (focus ↔ other)        → ignorada (fica pra visão dedicada)"""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # Janela FIXA nos 2 últimos domingos — sem filtro de período nesta tela, pra
    # Early e Late estarem SEMPRE no mesmo par (ver _wow_fixed_pair).
    last_sun, prev_sun = _wow_fixed_pair()
    t_curr_iso = f"{last_sun.isoformat()} 15:00:00 UTC"   # 12:00 BRT == 15:00 UTC
    t_prev_iso = f"{prev_sun.isoformat()} 15:00:00 UTC"

    def _scope(x):
        x = _apply_roster_current(x)         # roster Q3 + remove quem saiu (BQ traz líder cru)
        x = x[x["leader"].isin(ALLOWED_LEADERS)]
        # Gestor FICA no frame: os cards, a ponte do rodapé e a visão por líder
        # precisam enxergar o funil dele. Ele sai só do pivot por AE, abaixo.
        x = drop_departed_people(x)
        if f.get("region"):
            x = x[x["region"] == f["region"]]
        if f.get("leader"):
            x = x[x["leader"].isin(expand_leader(f["leader"]))]
        if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
            x = x[x["account_executive"] == f["ae"]]
        # Deal Type recorta as DUAS fontes (snapshot e eventos) pelo mesmo
        # `_scope`, senão as barras de estado e o movimento passariam a falar de
        # universos diferentes e a ponte do rodapé não fecharia.
        x = _pt_apply_dim_filter(x, "dtype", f.get("dtype", ""))
        return x

    # DUAS fontes, de propósito: o snapshot dos 2 domingos só responde "quanto
    # havia em cada ponta" (barras de ESTADO); todo o MOVIMENTO vem dos eventos
    # da janela, que enxergam o caminho intra-semana. Ver _wow_events_categorize.
    df = _scope(bq.pipeline_wow_snapshot(t_curr_iso, t_prev_iso, force_refresh=force, pipeline=_pl()))
    df["gp_prev"] = df["stage_prev"].apply(_hs_stage_group)
    df["gp_curr"] = df["stage_curr"].apply(_hs_stage_group)
    ev = _wow_events_categorize(
        _scope(bq.wow_events(t_curr_iso, t_prev_iso, force_refresh=force, pipeline=_pl())), focus)

    # 5ª categoria do relatório: no EARLY é o que AVANÇOU pra Late (o conceito de
    # won saiu da tela, a pedido); no LATE segue sendo won. O slot visual (cards,
    # chart, legenda) é o mesmo — muda a categoria que o alimenta.
    fifth = "late" if focus == "early" else "won"

    # Per-AE (ou por líder, explodindo p/ ancestrais). Totais sempre do frame CRU
    # pra não duplicar diretor+gerente.
    # Aqui a saída é uma barra POR PESSOA, e gestor não é vendedor: ele sai do
    # pivot. No grain de líder NÃO sai — `_explode_leaders` reescreve a linha dele
    # pro próprio nome como líder, que é justamente onde a venda direta deve
    # aparecer. Os cards e a ponte continuam vendo tudo (saem de `ev`, não daqui).
    ev_ent = (_explode_leaders(ev, f.get("leader")) if grain == "leader"
              else drop_non_ae_people(ev))
    relevant = ev_ent[ev_ent["category"].isin(_WOW_NET_CATS + (fifth,))]
    in_focus = df[(df["gp_prev"] == focus) | (df["gp_curr"] == focus)]

    # Pivot por AE × categoria. Valor soma os deltas dos EVENTOS; a contagem é de
    # deals DISTINTOS (nunique), senão um deal que se moveu 2× viraria "2 deals".
    if len(relevant) > 0:
        amt_pivot = relevant.pivot_table(index="account_executive", columns="category",
                                         values="delta", aggfunc="sum", fill_value=0)
        deals_pivot = relevant.pivot_table(index="account_executive", columns="category",
                                           values="deal_id", aggfunc="nunique", fill_value=0)
    else:
        amt_pivot = pd.DataFrame(index=pd.Index([], name="account_executive"))
        deals_pivot = pd.DataFrame(index=pd.Index([], name="account_executive"))

    for col in ("novo","cresceu","encolheu","lost", fifth):
        if col not in amt_pivot.columns:    amt_pivot[col]    = 0.0
        if col not in deals_pivot.columns:  deals_pivot[col]  = 0

    # Net = entradas - saídas-por-perda - encolhimento. A 5ª categoria fica fora.
    amt_pivot["net"] = (amt_pivot["novo"] + amt_pivot["cresceu"]
                       + amt_pivot["encolheu"] + amt_pivot["lost"])
    amt_pivot = amt_pivot.sort_values("net", ascending=False)

    rows = []
    for ae in amt_pivot.index:
        rows.append({
            "account_executive": ae,
            "novo":     float(amt_pivot.loc[ae, "novo"]),
            "cresceu":  float(amt_pivot.loc[ae, "cresceu"]),
            "encolheu": float(amt_pivot.loc[ae, "encolheu"]),
            "lost":     float(amt_pivot.loc[ae, "lost"]),
            # Slot "won" do template = a 5ª categoria do foco (won ou movido-pra-
            # Late) — o canal visual (CSS/JS) continua se chamando 'won'.
            "won":      float(amt_pivot.loc[ae, fifth]),
            "net":      float(amt_pivot.loc[ae, "net"]),
            "deals_novo":     int(deals_pivot.loc[ae, "novo"])     if ae in deals_pivot.index else 0,
            "deals_cresceu":  int(deals_pivot.loc[ae, "cresceu"])  if ae in deals_pivot.index else 0,
            "deals_encolheu": int(deals_pivot.loc[ae, "encolheu"]) if ae in deals_pivot.index else 0,
            "deals_lost":     int(deals_pivot.loc[ae, "lost"])     if ae in deals_pivot.index else 0,
            "deals_won":      int(deals_pivot.loc[ae, fifth])      if ae in deals_pivot.index else 0,
        })

    # Snapshots totais do focus
    total_prev = float(in_focus.loc[in_focus["gp_prev"] == focus, "amount_prev"].sum())
    total_curr = float(in_focus.loc[in_focus["gp_curr"] == focus, "amount_curr"].sum())
    deals_prev = int((in_focus["gp_prev"] == focus).sum())
    deals_curr = int((in_focus["gp_curr"] == focus).sum())

    # Totais por categoria — sempre do frame CRU de eventos, nunca do exploded por
    # líder (que duplicaria diretor + gerente). Agora TODAS as categorias saem da
    # mesma conta (delta do evento), então a ponte fecha sem sobra:
    #   total_prev + net + won + late + transferiu + regrediu + reabriu = total_curr
    # Antes, transferiu/regrediu eram remendados a partir das pontas do snapshot
    # justamente porque o diff não os produzia — não precisa mais.
    # 'won' entra SEMPRE (no Early não vira card, mas fecha a ponte auditável).
    _cats = _WOW_NET_CATS + (fifth, "won", "transferiu", "regrediu", "reabriu")
    cat_totals = {c: float(ev.loc[ev["category"] == c, "delta"].sum()) for c in _cats}
    cat_deals = {c: int(ev.loc[ev["category"] == c, "deal_id"].nunique()) for c in _cats}
    # Saldo do período = SÓ o que aumentou ou diminuiu de pipe no estágio. Won,
    # avanço p/ Late, transferência, volta de estágio e reabertura são movimento
    # de FRONTEIRA: saem do saldo e aparecem na ponte auditável do rodapé.
    cat_totals["net"] = sum(cat_totals[c] for c in _WOW_NET_CATS)

    # Y-scale: máx absoluto dos lados positivo (novo+cresceu) e negativo (encolheu+lost)
    y_max_pos = max((r["novo"] + r["cresceu"])    for r in rows) if rows else 0
    y_max_neg = max(-(r["encolheu"] + r["lost"]) for r in rows) if rows else 0
    y_max     = max(y_max_pos, y_max_neg, 1.0)

    return render_template(
        "pipeline_wow.html", ctx=ctx,
        focus=focus,
        focus_label=("Early · " + _STAGE_BY_ID["93878633"]["name"] if focus == "early"
                     else "Late · Negotiation / Decision / Procurement"),
        rows=rows,
        snap_curr_label=_fmt_date_short(last_sun) + " · 12h",
        snap_prev_label=_fmt_date_short(prev_sun) + " · 12h",
        total_prev=total_prev, total_curr=total_curr,
        deals_prev=deals_prev, deals_curr=deals_curr,
        delta_total=(total_curr - total_prev),
        cat_totals=cat_totals, cat_deals=cat_deals,
        fifth_cat=fifth,
        fifth_label=("Movido p/ Late" if fifth == "late" else "Won"),
        y_max=y_max, grain=grain,
    )


# ===========================================================================
# Histórico PoP — a mesma mecânica das telas PoP Early/Late, porém em SÉRIE:
# N semanas de movimento em vez de um único par de domingos.
#
# Duas fontes, como na PoP e pelo mesmo motivo de fundo (cada uma responde uma
# pergunta diferente), mas aqui com um motivo A MAIS, medido:
#   · MOVIMENTO  ← `wow_events`, bucketizado por semana. Enxerga o caminho
#     intra-semana (deal criado na quarta e ganho na sexta).
#   · NÍVEL      ← `bq.pipeline_level_series`, snapshot real de cada domingo.
#     NÃO é a soma acumulada dos eventos: ela erra até 0,9% do pipe no Late por
#     causa de amount apagado no HubSpot e de deals que entram e saem como won
#     na mesma janela. A conta completa está no docstring da query.
# ===========================================================================

WOW_HIST_WEEK_OPTS = (8, 12, 26)
WOW_HIST_WEEKS_DEFAULT = 12


def _wow_hist_weeks() -> int:
    """Nº de semanas da série, do chip da página (?weeks=). Valor fora da lista
    cai no padrão — a janela vira cache key no BQ, então aceitar número livre da
    URL seria criar uma entrada de cache nova a cada request."""
    try:
        w = int(request.args.get("weeks") or 0)
    except (TypeError, ValueError):
        w = 0
    return w if w in WOW_HIST_WEEK_OPTS else WOW_HIST_WEEKS_DEFAULT


WOW_HIST_QUARTER_KEY = "q"


def _wow_hist_window(f):
    """(nº de semanas, domingos) da série do Histórico PoP.

    Dois modos: os chips numéricos, que são janelas MÓVEIS terminando no último
    domingo, e o modo Quarter (`?weeks=q`), que ancora a série no início do
    trimestre do filtro. O Quarter existe porque "quanto o pipe cresceu no
    trimestre" não é respondível por janela móvel — 12 semanas atravessam a
    virada e misturam dois quarters. Reaproveita `_fc_hist_sundays`, que já é o
    calendário do Histórico do Forecast, pra as duas telas recortarem o
    trimestre exatamente do mesmo jeito."""
    if request.args.get("weeks") == WOW_HIST_QUARTER_KEY:
        q0, q1 = _fc_hist_quarter(f)
        suns = _fc_hist_sundays(q0, q1)
        return max(0, len(suns) - 1), suns
    w = _wow_hist_weeks()
    return w, _wow_hist_sundays(w)


def _wow_hist_focus() -> str:
    return "late" if request.args.get("focus") == "late" else "early"


def _wow_hist_sundays(weeks: int) -> list:
    """Os `weeks`+1 domingos que delimitam a série — N janelas precisam de N+1
    pontas. O índice 0 é a ÂNCORA (só nível inicial, nenhum movimento) e a
    semana k é o intervalo (domingo[k−1], domingo[k]]."""
    last_sun, _ = _last_two_sundays()
    return [last_sun - timedelta(weeks=weeks - i) for i in range(weeks + 1)]


def _wow_hist_frames(weeks: int, scope, force=False, suns=None):
    """(eventos com `week_idx`, níveis com `week_idx`+`gp`, domingos) no recorte
    da página. `scope` é o filtro de roster/allowlist/líder/região do caller."""
    suns = suns or _wow_hist_sundays(weeks)
    iso = [f"{s.isoformat()} 15:00:00 UTC" for s in suns]

    ev = scope(bq.wow_events(iso[-1], iso[0], force_refresh=force, pipeline=_pl())).copy()
    lv = scope(bq.pipeline_level_series(
        iso, sorted(_HS_EARLY_STAGES | _HS_LATE_STAGES), force_refresh=force, pipeline=_pl())).copy()
    lv["gp"] = lv["stage"].apply(_hs_stage_group)

    # Semana de cada evento: teto da distância até a âncora, em blocos de 7 dias.
    # TETO (e não piso) porque a janela é aberta embaixo e fechada em cima —
    # (domingo[k−1], domingo[k]] — então um evento exatamente às 12h do domingo k
    # pertence à semana k, não à k+1. O clip cobre só arredondamento de borda: o
    # próprio SQL já corta em (t_prev, t_curr].
    if len(ev):
        ts = pd.to_datetime(ev["ts"], utc=True)
        anchor = pd.Timestamp(suns[0], tz="UTC") + pd.Timedelta(hours=15)
        dist = (ts - anchor) / pd.Timedelta(days=7)
        ev["week_idx"] = dist.apply(
            lambda x: max(1, min(weeks, int(x) + (0 if x == int(x) else 1))))
    else:
        ev["week_idx"] = pd.Series(dtype="int64")
    return ev, lv, suns


def _wow_hist_week_rows(ev, lv, focus, weeks, suns):
    """Uma linha por semana: valor e nº de deals de cada categoria, saldo e o
    nível do pipe no domingo que FECHA a semana.

    Sempre do frame CRU de eventos — nunca do exploded por líder, que somaria
    diretor + gerente e dobraria o total do time."""
    fifth = "late" if focus == "early" else "won"
    e = _wow_events_categorize(ev, focus)
    lvf = lv[lv["gp"] == focus]
    return _hist_week_rows(e, lvf, _WOW_NET_CATS, fifth, weeks, suns)


def _hist_week_rows(e, lvf, net_cats, fifth, weeks, suns, level_col="amount"):
    """Núcleo compartilhado pelo Histórico PoP e pelo Histórico do Forecast.

    Recebe os eventos JÁ categorizados e o nível JÁ recortado no foco, porque as
    duas telas decidem essas duas coisas de formas diferentes (PoP: grupo de
    estágio; Forecast: estágio aberto + close date dentro do quarter) mas montam
    a linha da semana do mesmo jeito. Uma 2ª cópia divergiria na primeira coluna
    nova — o mesmo motivo que já fez `_wow_event_rows` ser compartilhado.

    `level_col` é a coluna do NÍVEL: a PoP mede em amount cheio, o Histórico do
    Forecast mede no amount PONDERADO pelo Modelo Analytics (`v_model`) — nível e
    movimento têm que sair da mesma coluna, senão a ponte não fecha."""
    cats = tuple(net_cats) + (fifth,)
    rows = []
    for k in range(1, weeks + 1):
        wk = e[e["week_idx"] == k]
        r = {"idx": k, "sunday": suns[k],
             "label": _fmt_date_short(suns[k]),
             "from_label": _fmt_date_short(suns[k - 1])}
        for c in cats:
            sub = wk[wk["category"] == c]
            r[c] = float(sub["delta"].sum())
            r["deals_" + c] = int(sub["deal_id"].nunique())
        r["net"] = sum(r[c] for c in net_cats)
        r["fifth"] = r[fifth]
        r["deals_fifth"] = r["deals_" + fifth]
        _lv = lvf[lvf["week_idx"] == k]
        r["level"] = float(_lv[level_col].sum())
        r["level_deals"] = int(len(_lv))
        rows.append(r)
    _lv0 = lvf[lvf["week_idx"] == 0]
    return rows, float(_lv0[level_col].sum()), int(len(_lv0))


def _wow_hist_bridge(ev, focus, level0, level_end):
    """Ponte da JANELA INTEIRA: do nível inicial ao final, parcela por parcela.

    Existe porque os dois números de destaque da tela parecem se contradizer —
    "saldo acumulado +R$ 8,55M" ao lado de "o pipe subiu R$ 1,33M". Não é
    contradição: o saldo só conta o que entrou/saiu DE VALOR no estágio, enquanto
    won, avanço pro Late, transferência, volta de estágio e reabertura movem o
    pipe sem passar pelo saldo. Sem a ponte, o leitor não tem como fechar a conta
    e desconfia do número certo. É a mesma peça que a tela PoP já tem no rodapé.

    Diferença importante em relação à ponte da PoP: lá ela fecha EXATO e uma
    sobra é bug. Aqui NÃO: nível e movimento vêm de fontes distintas (snapshot vs
    eventos), que divergem em centavos por causa de amount apagado no HubSpot e
    de deals que entram e saem como won na mesma janela — o docstring de
    `bq.pipeline_level_series` tem a medição. Por isso o resíduo é uma PARCELA
    exibida, não um erro escondido: some quando é zero, aparece nomeado quando
    não é."""
    e = _wow_events_categorize(ev, focus)
    fifth = "late" if focus == "early" else "won"
    _cats = _WOW_NET_CATS + (fifth, "won", "transferiu", "regrediu", "reabriu")
    tot = {c: float(e.loc[e["category"] == c, "delta"].sum()) for c in _cats}
    tot["net"] = sum(tot[c] for c in _WOW_NET_CATS)
    # Resíduo = o que o nível fez e os eventos não explicam. Sai da diferença
    # total (não da soma das parcelas exibidas) pra capturar QUALQUER divergência,
    # inclusive de categoria que nem apareça na ponte.
    tot["residuo"] = (level_end - level0) - float(e["delta"].sum())
    return tot


def _wow_hist_people(ev, lv, focus, weeks, grain, leader_filter):
    """Uma linha por PESSOA (AE ou líder) com o saldo de cada semana, o saldo
    acumulado e o nível de pipe nas duas pontas da janela.

    No grão de líder o frame é explodido por ancestral (`_explode_leaders`), de
    modo que o diretor engloba o gerente — é a mesma convenção das outras telas,
    e por isso a soma da coluna NÃO bate com o total do time (de propósito: o
    total vem do frame cru)."""
    e = _wow_events_categorize(ev, focus)
    e = e[e["category"].isin(_WOW_NET_CATS)]
    lvf = lv[lv["gp"] == focus]
    return _hist_people(e, lvf, weeks, grain, leader_filter)


def _hist_people(e, lvf, weeks, grain, leader_filter, level_col="amount",
                 explode=None):
    """Núcleo compartilhado do painel por pessoa (ver `_hist_week_rows`).
    `level_col` segue a mesma regra de lá: PoP em amount cheio, Histórico do
    Forecast no ponderado do Modelo Analytics. `explode` troca o
    `_explode_leaders` (o PoP Histórico Q usa a hierarquia de cada trimestre)."""
    if grain == "leader":
        e = (explode or _explode_leaders)(e, leader_filter)
        lvf = (explode or _explode_leaders)(lvf, leader_filter)
    else:
        # Painel por VENDEDOR: gestor não vira linha. Ele continua nos totais e
        # nas séries do time, que saem dos frames crus — só não é listado como se
        # fosse um AE. No grão de líder ele aparece, com o próprio nome.
        e, lvf = drop_non_ae_people(e), drop_non_ae_people(lvf)

    piv = (e.pivot_table(index="account_executive", columns="week_idx",
                         values="delta", aggfunc="sum", fill_value=0.0)
           if len(e) else pd.DataFrame())
    lv_piv = (lvf.pivot_table(index="account_executive", columns="week_idx",
                              values=level_col, aggfunc="sum", fill_value=0.0)
              if len(lvf) else pd.DataFrame())

    # Universo = quem teve movimento OU tem pipe em alguma ponta. Sem a 2ª
    # metade, quem ficou a janela inteira parado sumia da comparação — e "não
    # mexeu o pipe em 12 semanas" é exatamente o que a tela existe pra mostrar.
    names = sorted(set(piv.index) | set(lv_piv.index))
    out = []
    for nm in names:
        vals = [float(piv.loc[nm, k]) if (nm in piv.index and k in piv.columns) else 0.0
                for k in range(1, weeks + 1)]
        def _lv_at(k):
            return (float(lv_piv.loc[nm, k])
                    if (nm in lv_piv.index and k in lv_piv.columns) else 0.0)
        out.append({
            "name": nm, "short": short_name(nm), "weekly": vals,
            "total": sum(vals),
            "level_start": _lv_at(0), "level_end": _lv_at(weeks),
            "pos_weeks": sum(1 for v in vals if v > 0),
        })
    out.sort(key=lambda r: r["total"], reverse=True)
    return out


_HIST_POS_SEGS = (("novo", "#15803d"), ("cresceu", "#86efac"))
_HIST_NEG_SEGS = (("encolheu", "#fdba74"), ("lost", "#dc2626"))


def _build_hist_bars(wk_rows, weeks, drill_url, pos=_HIST_POS_SEGS,
                     neg=_HIST_NEG_SEGS, net_cats=_WOW_NET_CATS):
    """Geometria SVG do gráfico de movimento: uma coluna por semana, empilhada
    dos dois lados da linha do zero (novo/cresceu acima, encolheu/lost abaixo),
    com marcador de saldo. `drill_url(idx, cats)` monta a URL de cada segmento.

    A escala é ASSINADA e compartilhada: o mesmo R$ tem a mesma altura acima e
    abaixo do zero, senão uma semana de perdas pareceria menor que uma de ganhos
    do mesmo tamanho.

    `pos`/`neg` são os canais (chave, cor) de cada lado, na ordem do zero pra
    fora; o PoP Histórico por quarter acrescenta os dois de close date."""
    _TOP, _PLOT, _BOT = 26, 330, 86
    _LEFT = 92
    _STEP = 64 if weeks <= 13 else 40
    _BW = round(_STEP * 0.58)

    pos_max = max(sum(r[k] for k, _ in pos) for r in wk_rows) if wk_rows else 0.0
    neg_max = max(-sum(r[k] for k, _ in neg) for r in wk_rows) if wk_rows else 0.0
    if pos_max <= 0 and neg_max <= 0:
        # Recorte sem movimento nenhum (filtro que não casa com ninguém, ou um
        # time que não mexeu o Late na janela). Sem isto, pos_max/span = 0 grudava
        # a linha do zero no TOPO do gráfico e a tela vazia era lida como "tudo
        # negativo". Centralizado, o vazio parece o que é.
        pos_max = neg_max = 0.5
    span = (pos_max + neg_max) or 1.0
    zero_y = _TOP + _PLOT * (pos_max / span)

    def _y(v):
        """R$ (assinado) → coordenada y."""
        return zero_y - (v / span) * _PLOT

    # Passo ÚNICO nos dois lados do zero (ver _nice_money_step): a mesma régua
    # acima e abaixo é o que deixa comparar um ganho com uma perda de bater o
    # olho. Cada lado recebe só os ticks que cabem nele, então o lado menor não
    # fica com rótulos colados.
    step_v = _nice_money_step(span)
    yL = [{"label": "0", "y": round(zero_y, 1)}]
    k = 1
    while k * step_v <= pos_max:
        yL.append({"label": fmt_brl(k * step_v), "y": round(_y(k * step_v), 1)})
        k += 1
    k = 1
    while k * step_v <= neg_max:
        yL.append({"label": "−" + fmt_brl(k * step_v), "y": round(_y(-k * step_v), 1)})
        k += 1

    cols = []
    for i, r in enumerate(wk_rows):
        cx = _LEFT + i * _STEP + _STEP / 2
        segs, top_v, bot_v = [], 0.0, 0.0
        # Empilha do zero pra fora, na ordem em que a legenda lista.
        for key, cor in pos:
            v = r[key]
            if v <= 0:
                continue
            segs.append({"ch": key, "fill": cor,
                         "x": round(cx - _BW / 2, 1), "w": _BW,
                         "y": round(_y(top_v + v), 1),
                         "h": round((v / span) * _PLOT, 1),
                         "amt": fmt_brl(v), "deals": r["deals_" + key],
                         "url": drill_url(r["idx"], key)})
            top_v += v
        for key, cor in neg:
            v = -r[key]                        # guardados negativos
            if v <= 0:
                continue
            segs.append({"ch": key, "fill": cor,
                         "x": round(cx - _BW / 2, 1), "w": _BW,
                         "y": round(_y(-bot_v), 1),
                         "h": round((v / span) * _PLOT, 1),
                         "amt": fmt_brl(v), "deals": r["deals_" + key],
                         "url": drill_url(r["idx"], key)})
            bot_v += v
        cols.append({
            "idx": r["idx"], "label": r["label"], "segs": segs,
            "cx": round(cx, 1), "zx": round(_LEFT + i * _STEP, 1),
            "net": r["net"], "net_label": fmt_brl(abs(r["net"])),
            "net_y": round(_y(r["net"]), 1),
            "top_y": round(_y(top_v), 1), "bot_y": round(_y(-bot_v), 1),
            "url": drill_url(r["idx"], ",".join(net_cats)),
        })
    right_x = _LEFT + max(len(wk_rows), 1) * _STEP
    return {"w": right_x + 30, "h": _TOP + _PLOT + _BOT, "left": _LEFT,
            "right_x": right_x, "top": _TOP, "step": _STEP, "bw": _BW,
            "zero_y": round(zero_y, 1), "plot_h": _PLOT,
            "base_y": _TOP + _PLOT, "yL": yL, "cols": cols}


def _build_hist_level(wk_rows, level0, sun0, breaks=None):
    """Geometria da linha de NÍVEL do pipe. Eixo começa em 0 de propósito: numa
    série de nível, cortar a base exagera a variação — 5% de oscilação vira um
    tobogã se o eixo abre no mínimo da série.

    `breaks` (PoP Histórico por quarter): viradas de trimestre, cada uma com a
    semana `week` em que caiu, a fração `frac` da semana até ela, o nível que
    SAIU (`out`, régua do trimestre que acabou) e o que ENTROU (`in`, régua do
    novo). A linha é cortada ali — ligar os dois lados desenharia como
    movimento o que é só troca de régua."""
    _TOP, _PLOT, _BOT = 24, 200, 74
    _LEFT = 92
    _STEP = 64 if len(wk_rows) <= 13 else 40
    base_y = _TOP + _PLOT
    breaks = breaks or []
    vals = [level0] + [r["level"] for r in wk_rows]
    # Piso de R$ 1.000 (e não 1.0) no topo do eixo: num recorte sem pipe nenhum,
    # 4 ticks entre 0 e R$ 1,08 saíam todos como "R$ 0"/"R$ 1" depois do
    # arredondamento do fmt_brl — cinco rótulos repetidos sem informação.
    top = max(vals + [b[k] for b in breaks for k in ("out", "in")] + [1000.0]) * 1.08
    yL = [{"label": fmt_brl(top * k / 4), "y": round(base_y - k / 4 * _PLOT, 1)}
          for k in range(5)]
    pts, marks = [], []
    for i, v in enumerate(vals):
        cx = _LEFT + (i - 0.5) * _STEP + _STEP / 2   # âncora meia coluna à esquerda
        cy = base_y - (v / top) * _PLOT
        pts.append(f"{round(cx, 1)},{round(cy, 1)}")
        marks.append({"cx": round(cx, 1), "cy": round(cy, 1),
                      "label": fmt_brl(v),
                      "date": _fmt_date_short(sun0 if i == 0 else wk_rows[i - 1]["sunday"]),
                      # A âncora fica exatamente EM CIMA do eixo (x = left), então
                      # o rótulo centrado invadia os números do eixo Y. Ancorado
                      # pela esquerda, ele cresce pra dentro do gráfico.
                      "anchor": "start" if i == 0 else "middle",
                      "lx": round(cx + (5 if i == 0 else 0), 1),
                      # Rótulos INTERCALADOS acima/abaixo do ponto. "R$ 4,05M"
                      # ocupa ~50px e o passo é 64 (40 em 26 semanas), então numa
                      # única faixa eles se sobrepõem. Intercalar preserva os N
                      # números — esconder um a cada dois tiraria justamente a
                      # semana que o leitor foi conferir.
                      "ly": round(cy - 11 if i % 2 == 0 else cy + 18, 1),
                      "is_anchor": i == 0})

    # Com virada, a polyline vira trechos: até a virada no nível que saiu, e da
    # virada em diante a partir do nível que entrou.
    segs, brk, ini = [], [], 0
    for b in sorted(breaks, key=lambda b: (b["week"], b["frac"])):
        bx = round(_LEFT + (b["week"] - 1 + b["frac"]) * _STEP, 1)
        y_out = round(base_y - (b["out"] / top) * _PLOT, 1)
        y_in = round(base_y - (b["in"] / top) * _PLOT, 1)
        segs.append(" ".join(pts[ini:b["week"]] + [f"{bx},{y_out}"]))
        pts_in = f"{bx},{y_in}"
        ini = b["week"]
        brk.append({"x": bx, "y_out": y_out, "y_in": y_in, "label": b["label"],
                    "in_label": fmt_brl(b["in"]), "out_label": fmt_brl(b["out"]),
                    "start": pts_in})
    tail = pts[ini:]
    if brk:
        tail = [brk[-1]["start"]] + tail
        # Cada trecho do meio começa no "entrou" da virada anterior.
        for j in range(1, len(segs)):
            segs[j] = brk[j - 1]["start"] + " " + segs[j]
    segs.append(" ".join(tail))
    return {"w": _LEFT + len(wk_rows) * _STEP + 30, "h": _TOP + _PLOT + _BOT,
            "left": _LEFT, "top": _TOP, "base_y": base_y, "step": _STEP,
            "right_x": _LEFT + len(wk_rows) * _STEP,
            "yL": yL, "pts": " ".join(pts), "marks": marks,
            "segs": segs, "breaks": brk}


def _build_hist_spark(people, weeks):
    """Mini-gráficos por pessoa, todos na MESMA escala Y — é a escala comum que
    torna a comparação honesta: normalizar cada um pelo próprio máximo faria a
    semana de R$ 30 mil de um AE parecer igual à de R$ 900 mil de outro."""
    _W, _H = 26, 70                       # por semana / altura útil
    # Máximo REAL, não o topo robusto (_robust_top) que outros charts daqui usam:
    # lá o outlier é clampado e o RÓTULO mostra o valor verdadeiro, mas aqui não
    # há rótulo por barra — clampar seria esconder a semana excepcional que é
    # justamente o que se quer enxergar na comparação. O número exato de cada
    # semana está na matriz logo abaixo.
    span = max([abs(v) for p in people for v in p["weekly"]] + [1.0])
    out = []
    for p in people:
        bars = []
        for i, v in enumerate(p["weekly"]):
            h = abs(v) / span * (_H / 2)
            bars.append({"x": round(i * _W + 2, 1), "w": _W - 4,
                         "y": round(_H / 2 - (h if v > 0 else 0), 1),
                         "h": round(max(h, 0.8), 1),
                         "pos": v >= 0, "week": i + 1})
        out.append({**p, "bars": bars})
    return {"w": weeks * _W, "h": _H, "mid": _H / 2, "bw": _W, "rows": out,
            "span": span, "span_label": fmt_brl(span)}


def _render_wow_history(grain="ae"):
    """Histórico PoP: N semanas de movimento do pipe Early ou Late, no total do
    time e pessoa a pessoa."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    weeks, suns_win = _wow_hist_window(f)
    focus = _wow_hist_focus()
    # O drill precisa reconstruir a MESMA janela: no modo Quarter isso é a chave
    # "q", não o número de semanas — passar o número faria o drill montar uma
    # janela móvel do mesmo tamanho, ancorada no lugar errado.
    weeks_arg = (WOW_HIST_QUARTER_KEY
                 if request.args.get("weeks") == WOW_HIST_QUARTER_KEY else weeks)

    def _scope(x):
        x = _apply_roster_current(x)
        x = x[x["leader"].isin(ALLOWED_LEADERS)]
        x = drop_departed_people(x)          # gestor fica (ver _render_pipeline_wow)
        if f.get("region"):
            x = x[x["region"] == f["region"]]
        if f.get("leader"):
            x = x[x["leader"].isin(expand_leader(f["leader"]))]
        if f.get("ae"):   # filtro de AE da barra (antes ignorado aqui)
            x = x[x["account_executive"] == f["ae"]]
        # Vale pros dois frames da série (eventos e níveis) — ver nota no PoP.
        x = _pt_apply_dim_filter(x, "dtype", f.get("dtype", ""))
        return x

    ev, lv, suns = _wow_hist_frames(weeks, _scope, force=force, suns=suns_win)
    wk_rows, level0, _ = _wow_hist_week_rows(ev, lv, focus, weeks, suns)
    people = _wow_hist_people(ev, lv, focus, weeks, grain, f.get("leader"))
    level_end = wk_rows[-1]["level"] if wk_rows else level0
    bridge = _wow_hist_bridge(ev, focus, level0, level_end)

    def _drill(idx, cats, name=""):
        return url_for("pipeline_wow_history_deals", focus=focus, week=idx,
                       weeks=weeks_arg, cat=cats, name=name, grain=grain,
                       # O drill lê o Deal Type da PRÓPRIA URL; sem isto ele
                       # explodia aquisição + expansão com a barra só de uma.
                       dtype=(f.get("dtype") or None),
                       leader=(f.get("leader") if not name else None),
                       region=(f.get("region") if not name else None))

    fifth = "late" if focus == "early" else "won"
    # Chips de navegação da própria página (foco e nº de semanas). Preservam o
    # resto da querystring pra não zerar Líder/AE/moeda ao trocar de aba.
    def _chip_url(**kw):
        args = {k: v for k, v in request.args.items() if k not in kw and v}
        return request.path + "?" + urllib.parse.urlencode({**args, **kw})

    return render_template(
        "pipeline_wow_history.html", ctx=ctx,
        focus=focus, grain=grain, weeks=weeks,
        focus_label=("Early · " + _STAGE_BY_ID["93878633"]["name"] if focus == "early"
                     else "Late · Negotiation / Decision / Procurement"),
        fifth_cat=fifth, fifth_label=("Movido p/ Late" if fifth == "late" else "Won"),
        wk_rows=wk_rows, people=people,
        level0=level0, level_end=level_end, bridge=bridge,
        first_label=_fmt_date_short(suns[0]), last_label=_fmt_date_short(suns[-1]),
        bars=_build_hist_bars(wk_rows, weeks, _drill),
        lvl=_build_hist_level(wk_rows, level0, suns[0]),
        spark=_build_hist_spark(people, weeks),
        total_net=sum(r["net"] for r in wk_rows),
        total_fifth=sum(r["fifth"] for r in wk_rows),
        best=(max(wk_rows, key=lambda r: r["net"]) if wk_rows else None),
        worst=(min(wk_rows, key=lambda r: r["net"]) if wk_rows else None),
        week_opts=([(w, f"{w} sem") for w in WOW_HIST_WEEK_OPTS]
                   + [(WOW_HIST_QUARTER_KEY, "Quarter")]),
        week_sel=(WOW_HIST_QUARTER_KEY
                  if request.args.get("weeks") == WOW_HIST_QUARTER_KEY else weeks),
        chip_url=_chip_url, drill=_drill,
    )


@app.route("/pipeline/wow/history")
def pipeline_wow_history():
    return _render_wow_history("ae")


@app.route("/leaders/pipeline/wow/history")
def leaders_pipeline_wow_history():
    return _render_wow_history("leader")


@app.route("/pipeline/wow/history/deals")
def pipeline_wow_history_deals():
    """Drill de UMA semana da série. Mesma explosão da PoP (uma linha por
    evento), recortada pela semana clicada — o `week` vem do gráfico e a janela
    é reconstruída aqui a partir de (weeks, week), não da URL, pra não existir
    par de datas na querystring que possa divergir do que foi clicado."""
    focus = "late" if request.args.get("focus") == "late" else "early"
    weeks, suns_win = _wow_hist_window(get_filters())
    try:
        wk = int(request.args.get("week") or 0)
    except (TypeError, ValueError):
        wk = 0
    if not 1 <= wk <= weeks:
        return jsonify(_empty_drill(""))

    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))

    _all = list(_WOW_NET_CATS) + ["late" if focus == "early" else "won"]
    _pedido = (request.args.get("cat") or "").strip()
    if _pedido:
        pedidas = [c.strip().lower() for c in _pedido.split(",")]
        cats = [c for c in _all if c in pedidas]
    else:
        cats = list(_all)

    def _scope(x):
        x = _apply_roster_current(x)
        x = x[x["leader"].isin(ALLOWED_LEADERS)]
        x = drop_departed_people(x)
        # Mesmo recorte da série clicada (ver nota no _scope da página).
        return _pt_apply_dim_filter(x, "dtype", get_filters().get("dtype", ""))

    ev, _lv, suns = _wow_hist_frames(weeks, _scope, suns=suns_win)
    ev = ev[ev["week_idx"] == wk]
    ev = _entity_filter(ev, name, grain,
                        request.args.get("leader"), request.args.get("region"))
    ev = _wow_events_categorize(ev, focus)
    ev = ev[ev["category"].isin(cats)]
    rows = _wow_event_rows(ev)

    _janela = f"{_fmt_date_short(suns[wk - 1])} → {_fmt_date_short(suns[wk])}"
    n_deals = int(ev["deal_id"].nunique())
    _d = f"{n_deals} deal{'s' if n_deals != 1 else ''}"
    if len(rows) != n_deals:
        _d += f" · {len(rows)} movimentos"
    _v = sum(x["delta"] for x in rows)
    _sinal = "+" if _v >= 0 else "−"
    # `fmt_money` respeita o filtro mestre de moeda (e o MX, que é sempre ARR
    # US$). O R$ fixo daqui era o último lugar que escrevia "R$" numa tela do
    # México — a barra dizia US$ e o drill dela dizia R$.
    _valor = f"{_sinal}{fmt_money(abs(_v))}"
    cat = cats[0] if len(cats) == 1 else ""
    subtitle = (f"{_janela} · {_d} · "
                + (_valor if cat else f"saldo {_valor}"))
    _escopo = name or f"PoP {focus} — total"
    _sufixo = _WOW_CAT_LABEL.get(cat, "") if cat else "Saldo"
    return jsonify({
        "title": f"{_escopo} · {_sufixo} · semana {wk}",
        "subtitle": subtitle,
        "columns": _wow_drill_columns(grain, name),
        "rows": rows,
    })


# ===========================================================================
# PoP Histórico por QUARTER (só Sales Leaders) — o Histórico PoP com o pipe de
# cada semana restrito ao TRIMESTRE VIGENTE: deal no grupo de estágio (Early ou
# Late) E com close date dentro do trimestre daquela semana. Pedido do Pedro
# (13/10/2026): "o pipeline de Q3 tem que ter close date para 30/09, e todo
# trimestre da mesma forma". Trimestre = calendário normal (a extensão de
# fechamento do Q3 até 02/10 vale só pra receita realizada).
#
# Por que não reaproveita as fontes do Histórico PoP: `wow_events` e
# `pipeline_level_series` saem de `hubspot_deals_history`, que não versiona
# close date. Quem versiona são as cópias diárias do BI
# (`bq.pipeline_asof_changes`), gravadas às ~17h16 BRT.
#
# Movimento = diferença entre cópias CONSECUTIVAS, dia a dia — não domingo
# contra domingo: o deal que entra no Late na terça e é ganho na sexta aparece
# nas duas pontas do caminho, como no PoP. Nível e movimento saem da MESMA
# grade, então a ponte fecha em zero por construção (sem o resíduo do PoP).
#
# Duas categorias novas, que o pipe por trimestre cria e o PoP não tem:
#   puxou       o close date ENTROU no trimestre (deal já estava no estágio);
#   escorregou  o close date SAIU do trimestre (ou foi apagado).
# As duas entram no saldo: pro pipe do trimestre, escorregar é perda de verdade.
#
# Virada de trimestre: no 1º dia do trimestre novo o pipe troca de régua — sai
# o que sobrou do trimestre que acabou e entra o que tem close date no novo.
# Não é movimento de ninguém, então fica FORA do saldo e das barras: aparece na
# ponte e como corte na linha de nível. Por isso o modo trimestre ancora na
# VÉSPERA do início (30/09 pro Q4): o ponto de partida já é o pipe do Q4.
#
# O histórico é atribuído ao dono ATUAL do deal (o da cópia mais recente), a
# mesma convenção do PoP Histórico — sem isso, troca de AE entre times virava
# saída de um e entrada de outro.
# ===========================================================================

WQ_N_QUARTERS = 4      # chips de trimestre: os mais recentes que têm abertura
# Chip de ano fiscal (pedido do Pedro, 20/10/2026): o FY inteiro numa série só,
# trocando de régua a cada trimestre. Mesmas datas do preset "FY26" do filtro de
# período. O FY não pode começar antes das cópias diárias do BI (23/03/2026).
WQ_FISCAL_YEARS = {"FY26": (date(2026, 4, 1), date(2027, 1, 31))}

# Canais do gráfico, do zero pra fora. Os de close date ficam na ponta, pra a
# leitura do PoP original (novo/cresceu acima, encolheu/lost abaixo) continuar
# no mesmo lugar.
_WQ_POS = (("novo", "#15803d"), ("cresceu", "#86efac"), ("puxou", "#0284c7"))
_WQ_NEG = (("encolheu", "#fdba74"), ("lost", "#dc2626"), ("escorregou", "#7c3aed"))
_WQ_NET_CATS = tuple(k for k, _ in _WQ_POS + _WQ_NEG)
_WQ_CAT_LABEL = {**_WOW_CAT_LABEL,
                 "puxou": "Puxado pro trimestre",
                 "escorregou": "Escorregou do trimestre",
                 "saiu": "Saiu do funil",
                 "virada_saiu": "Virada · ficou no trimestre anterior",
                 "virada_entrou": "Virada · close date no trimestre novo"}
_WQ_AUS = "__ausente__"        # deal fora da cópia do dia (apagado, outro funil, sem AE)
_WQ_EV_COLS = ["deal_id", "week_idx", "step", "dia", "dia_ant", "q_escopo", "delta", "category",
               "stage_from", "stage_to", "cd_from", "cd_to", "amt_from", "amt_to",
               "deal_name", "account_executive", "leader", "region", "deal_type"]


def _wq_quarter_key(q0: date) -> str:
    return f"{q0.year}Q{(q0.month - 1) // 3 + 1}"


def _wq_quarter_opts(disp: list) -> list:
    """Trimestres que dá pra mostrar inteiros: precisam da cópia da VÉSPERA do
    início, que é a abertura. As cópias diárias começam em 23/03/2026, então o
    Q1/26 não entra."""
    if not disp:
        return []
    out, q = [], _quarter_start_of(_today())
    while len(out) < WQ_N_QUARTERS and q - timedelta(days=1) >= disp[0]:
        out.append(q)
        q = _prev_quarter(q)
    return sorted(out)


def _wq_window(force=False):
    """(alvos, pontas, dias, sel, trimestres) da série.

    `alvos` são as datas que a tela promete — domingos; no modo trimestre, a
    véspera do início e, se o trimestre já acabou, o último dia dele. `pontas`
    são as cópias que de fato as representam: a do próprio dia, ou a última
    cópia SAUDÁVEL antes dele quando a do dia falta ou veio quebrada (ver
    `_fcd_bi_saudaveis`). `dias` são todas as cópias saudáveis entre a 1ª e a
    última ponta — é entre elas, de uma pra seguinte, que o movimento é medido."""
    disp_df = bq.forecast_asof_snapshots(force_refresh=force)
    disp = (sorted(datetime.strptime(str(x), "%Y%m%d").date() for x in disp_df["sufixo"])
            if len(disp_df) else [])
    qopts = _wq_quarter_opts(disp)
    sel = (request.args.get("weeks") or "").strip()
    last_sun, _ = _last_two_sundays()
    # Domingo à tarde a cópia do dia só sai às ~17h16: até lá a série fecha no
    # domingo anterior, em vez de prometer um ponto que não existe.
    end_sun = max((d for d in disp if d.weekday() == 6 and d <= last_sun), default=None)
    if end_sun is None:
        return [], [], [], sel, qopts

    q0 = next((q for q in qopts if _wq_quarter_key(q) == sel), None)
    q1 = _quarter_end_of(q0) if q0 else None
    if not q0 and sel in WQ_FISCAL_YEARS and WQ_FISCAL_YEARS[sel][0] - timedelta(days=1) >= disp[0]:
        # FY: mesma mecânica do trimestre (véspera do início → domingos → último
        # dia, se já acabou), com as viradas de trimestre no meio da série.
        q0, q1 = WQ_FISCAL_YEARS[sel]
    if q0:
        alvos = [q0 - timedelta(days=1)]
        s = q0 + timedelta(days=(6 - q0.weekday()) % 7)
        while s <= min(q1, end_sun):
            alvos.append(s)
            s += timedelta(days=7)
        # Trimestre encerrado fecha no último dia dele, não no último domingo:
        # os dias finais são justamente os de mais won.
        if q1 < _today() and q1 <= disp[-1] and alvos[-1] != q1:
            alvos.append(q1)
    else:
        try:
            w = int(sel)
        except ValueError:
            w = 0
        if w not in WOW_HIST_WEEK_OPTS:
            w = WOW_HIST_WEEKS_DEFAULT
        sel = str(w)
        alvos = [end_sun - timedelta(weeks=w - i) for i in range(w + 1)]

    cands = [d for d in disp if alvos[0] - timedelta(days=7) <= d <= alvos[-1]]
    sane = _wq_saudaveis(cands, force=force)
    pares = []
    for t in alvos:
        p = max((d for d in sane if d <= t), default=None)
        if p is None or (pares and p <= pares[-1][1]):
            continue
        pares.append((t, p))
    alvos, pontas = [t for t, _ in pares], [p for _, p in pares]
    dias = [d for d in sane if pontas and pontas[0] <= d <= pontas[-1]]
    return alvos, pontas, dias, sel, qopts


def _wq_explode(df, leader_filter):
    """`_explode_leaders` com a hierarquia do trimestre de cada linha
    (`q_escopo`): no Q2/26 a linha do Leonardo inclui o time do Gustavo."""
    if not len(df):
        return df
    qs = list(pd.unique(df["q_escopo"]))
    if leader_filter:
        scope = set().union(*(_expand_leader_em(leader_filter, q) for q in qs))
    else:
        scope = set(LEADER_ORDER) - {"Pedro Mendes"}
    parts = []
    for L in [x for x in LEADER_ORDER if x != "Pedro Mendes" and x in scope]:
        times = {q: _expand_leader_em(L, q) for q in qs}
        m = [ld in times[q] for ld, q in zip(df["leader"], df["q_escopo"])]
        part = df[m].copy()
        if len(part):
            part["account_executive"] = L
            parts.append(part)
    return pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0]


def _wq_saudaveis(dias: list, force=False) -> list:
    """As cópias de `dias` que não vieram quebradas (ver `bq.forecast_asof_ae_counts`).

    Régua LOCAL — a mediana das cópias a até 7 dias — e não a da janela inteira
    como em `_fcd_bi_saudaveis`, que é de 14 dias: em 26 semanas a base de deals
    com AE cresce ~26% (15,4 mil em abril → 19,5 mil em outubro/2026), e a
    mediana global marcava como quebradas as cópias de 05 a 16/04, que estavam
    inteiras. A de 01/10/2026 (sem AE) e a de 02/10 (−26%) continuam caindo.
    Se a checagem falhar, segue sem ela."""
    sufs = [d.strftime("%Y%m%d") for d in dias]
    try:
        cnt = bq.forecast_asof_ae_counts(sorted(sufs), pipeline=_pl(), force_refresh=force)
    except Exception as e:  # noqa: BLE001 — sanidade é best-effort
        print(f"pepeuhacker · checagem das cópias do BI falhou ({e})", flush=True)
        return list(dias)
    n = dict(zip(cnt["sufixo"].astype(str), cnt["n_ae"].astype(float))) if len(cnt) else {}
    if not n:
        return list(dias)
    out = []
    for d, sfx in zip(dias, sufs):
        viz = [n.get(o.strftime("%Y%m%d"), 0.0) for o in dias if abs((o - d).days) <= 7]
        ref = float(pd.Series(viz).median()) if viz else 0.0
        if n.get(sfx, 0.0) >= _FCD_BI_MIN_FRAC * ref:
            out.append(d)
    return out


def _wq_scope(f, entity=True):
    """Roster/allowlist/Deal Type — e, na página, líder/região. O drill recebe
    líder/região pela URL e filtra depois, como no PoP.

    `cut_from` = início do trimestre que está sendo medido: quem saiu da empresa
    em DEPARTED_FROM continua inteiro nos trimestres ANTERIORES ao corte dele.
    Sem isso os 6 AEs que saíram em 01/10/2026 sumiam também das semanas do Q3,
    quando ainda eram do time (R$ 170–410 mil de pipe Late por domingo) — e a
    quota do Q3, que conta com eles, ficava contra um pipe sem eles."""
    def _scope(x, cut_from=None):
        x = _apply_roster_current(x, cut_from=cut_from)
        if cut_from is not None and "leader" in x.columns:
            # Líder do AE NO TRIMESTRE MEDIDO — a mesma régua do realizado
            # (`_attr_deal_leader`) e da meta. No Q3/Q4 é o roster (igual ao de
            # hoje); no Q2 os times eram outros (Caio e Lucas com o Gustavo, o
            # Arthur era AE dele), e o pipe com líder filtrado contava o time de
            # hoje contra a meta do time de então.
            q = _quarter_start_of(cut_from)
            lq = {a: ld for (a, qq), ld in _period_leader_map().items() if qq == q and ld}
            lq.update({a: v["leader"] for a, v in _roster_of(q).items() if a not in lq})
            # Gestor continua dono do próprio deal (venda direta do time dele).
            lq = {a: ld for a, ld in lq.items() if a not in NON_AE_USERS}
            # Quem saiu e nunca teve linha na curada (Gabriel Faria e Rayssa
            # Holanda, Q3/26) só aparece no roster — por isso o roster entra
            # junto: sem líder, o deal caía no filtro de ALLOWED_LEADERS.
            tem = x["account_executive"].isin(lq)
            if tem.any():
                x = x.copy()
                x.loc[tem, "leader"] = x.loc[tem, "account_executive"].map(lq)
                if "region" in x.columns:
                    x.loc[tem, "region"] = x.loc[tem, "region"].fillna(_roster_region())
        x = x[x["leader"].isin(ALLOWED_LEADERS)]
        x = drop_departed_people(x)          # gestor fica (ver _render_pipeline_wow)
        if entity and f.get("region"):
            x = x[x["region"] == f["region"]]
        if entity and f.get("leader"):
            qh = _quarter_start_of(cut_from or _today())
            x = x[x["leader"].isin(_expand_leader_em(f["leader"], qh))]
        # Filtro de AE: desde 21/10/2026 vale nas duas telas (AE e Leaders) —
        # antes a de Leaders limpava o `ae` e mostrava o time inteiro com um AE
        # escolhido na barra.
        if entity and f.get("ae"):
            x = x[x["account_executive"] == f["ae"]]
        return _pt_apply_dim_filter(x, "dtype", f.get("dtype", ""))
    return _scope


def _wq_frames(focus, scope, force=False):
    """Eventos, níveis e viradas da série, já no recorte de `scope`.

    Devolve um dict com:
      ev       uma linha por (deal, passo) em que o valor no pipe mudou — passo =
               par de cópias consecutivas, `step` = índice da cópia de chegada;
      lv       valor de cada deal em cada ponta (`week_idx`, coluna `v`);
      viradas  uma por troca de trimestre dentro da janela (o que saiu/entrou);
      alvos, pontas, dias, sel, qopts — ver `_wq_window`.

    A régua de cada passo é o trimestre do dia de CHEGADA, e a âncora é medida
    na régua do 1º passo. Assim cada passo compara as duas pontas com a mesma
    régua, e a troca de régua vira um evento à parte (`virada_*`) no 1º passo do
    trimestre novo. Soma de todos os deltas = nível final − nível inicial."""
    alvos, pontas, dias, sel, qopts = _wq_window(force=force)
    out = {"ev": pd.DataFrame(columns=_WQ_EV_COLS),
           "lv": pd.DataFrame(columns=["deal_id", "week_idx", "v", "stage", "cd",
                                       "deal_name", "account_executive", "leader",
                                       "region"]),
           "viradas": [], "reguas": [], "alvos": alvos, "pontas": pontas, "dias": dias,
           "sel": sel, "qopts": qopts}
    if len(pontas) < 2:
        return out
    sufs = [d.strftime("%Y%m%d") for d in dias]
    raw = bq.pipeline_asof_changes(sufs, sorted(_HS_EARLY_STAGES | _HS_LATE_STAGES),
                                   pipeline=_pl(), force_refresh=force)
    if not len(raw):
        return out

    meta = raw.drop_duplicates("deal_id")[["deal_id", "deal_name", "account_executive",
                                           "deal_type"]].copy()
    meta["deal_id"] = meta["deal_id"].astype(str)
    meta = meta.merge(_fc_asof_leader_meta(force=force), on="account_executive", how="left")
    # Venda direta de gestor: mesma atribuição do Histórico do Forecast.
    mgr = meta["account_executive"].isin(NON_AE_USERS)
    meta.loc[mgr, "leader"] = meta.loc[mgr, "account_executive"]
    meta.loc[mgr, "region"] = meta.loc[mgr, "region"].fillna("BR")
    # Escopo POR TRIMESTRE (ver `_wq_scope`): cada trimestre da janela tem o
    # seu conjunto de deals dentro. A grade leva a UNIÃO deles; dono e líder
    # do deal ficam os do trimestre mais recente em que ele está dentro.
    trims = sorted({_quarter_start_of(d) for d in dias})
    por_trim = {q: scope(meta.copy(), cut_from=q) for q in trims}
    no_trim = {q: set(v["deal_id"]) for q, v in por_trim.items()}
    meta = (pd.concat([por_trim[q] for q in reversed(trims)], ignore_index=True)
              .drop_duplicates("deal_id").reset_index(drop=True))
    if not len(meta):
        return out
    # Líder de cada deal EM CADA TRIMESTRE: eventos e níveis levam o do
    # trimestre deles (`q_escopo`), pra painel por líder e drill recortarem
    # igual à página — no Q2/26 o deal do Caio é do Gustavo, não do Leonardo.
    lider_q = {q: dict(zip(v["deal_id"], v["leader"])) for q, v in por_trim.items()}

    def _lider_no_trim(df):
        return [lider_q.get(q, {}).get(d, ld)
                for d, q, ld in zip(df["deal_id"], df["q_escopo"], df["leader"])]

    # ---- Grade deal × dia, reconstruída das linhas de mudança com ffill ----
    deals = meta["deal_id"].tolist()
    D = len(dias)
    pos = {s: i for i, s in enumerate(sufs)}
    r = raw.assign(deal_id=raw["deal_id"].astype(str))
    r = r[r["deal_id"].isin(set(deals))]
    r = r.assign(i=r["dia"].astype(str).map(pos))
    r = r[r["i"].notna()].astype({"i": int})
    r["stg"] = r["stage"].astype(object).where(r["stage"].notna(), _WQ_AUS)
    _cdt = pd.to_datetime(r["close_date"].astype(object), errors="coerce")
    # Close date como nº de dias desde 1970; −1 = sem close date. Sentinela (e
    # não NaN) porque o ffill abaixo preencheria o NaN legítimo de quem apagou
    # o close date com a data antiga.
    r["cd"] = ((_cdt - pd.Timestamp("1970-01-01")).dt.days).fillna(-1)
    r["amt"] = pd.to_numeric(r["amount"], errors="coerce").fillna(0.0)

    def _grade(col, vazio):
        g = (r.pivot(index="deal_id", columns="i", values=col)
              .reindex(index=deals, columns=range(D)).ffill(axis=1))
        return g.fillna(vazio).to_numpy()

    stg = _grade("stg", _WQ_AUS).astype(object)
    amt = _grade("amt", 0.0).astype(float)
    cd = _grade("cd", -1).astype(float)
    gmap = {u: ("aus" if u == _WQ_AUS else _hs_stage_group(u)) for u in pd.unique(stg.ravel())}
    grp = pd.Series(stg.ravel()).map(gmap).to_numpy().reshape(stg.shape)

    ep = date(1970, 1, 1)
    qs = np.array([(_quarter_start_of(d) - ep).days for d in dias], dtype=float)
    qe = np.array([(_quarter_end_of(d) - ep).days for d in dias], dtype=float)
    # Escopo de cada coluna = o do trimestre do dia (mesma régua do close date).
    _dentro_q = {q: np.isin(np.array(deals, dtype=object), list(v)) for q, v in no_trim.items()}
    sc = np.stack([_dentro_q[_quarter_start_of(d)] for d in dias], axis=1)

    def _dentro(c, j):
        """Deal no foco com close date no trimestre do dia `j` e no escopo
        desse trimestre, na cópia `c`."""
        return ((grp[:, c] == focus) & (cd[:, c] >= qs[j]) & (cd[:, c] <= qe[j])
                & sc[:, j])

    # ---- Passos: todas as cópias consecutivas de uma vez ----
    gp, gc = grp[:, :-1], grp[:, 1:]
    cp, cc = cd[:, :-1], cd[:, 1:]
    ap, ac = amt[:, :-1], amt[:, 1:]
    q0, q1 = qs[1:], qe[1:]                     # régua = dia de chegada
    sq = sc[:, 1:]                              # escopo idem
    in_p = (gp == focus) & (cp >= q0) & (cp <= q1) & sq
    in_c = (gc == focus) & (cc >= q0) & (cc <= q1) & sq
    mud = (in_p != in_c) | (in_p & in_c & (ap != ac))
    other = "late" if focus == "early" else "early"
    ent, sai, fica = ~in_p & in_c, in_p & ~in_c, in_p & in_c
    cat = np.select(
        [ent & np.isin(gp, ["aus", "pre", "none"]),
         ent & (gp == other),
         ent & np.isin(gp, ["won", "lost"]),
         ent & (gp == focus),
         sai & (gc == "aus"),
         sai & (gc == "won"),
         sai & (gc == "lost"),
         sai & (gc == other),
         sai & np.isin(gc, ["pre", "none"]),
         sai & (gc == focus),
         fica & (ac > ap),
         fica & (ac < ap)],
        # Late: chegar do Early é "novo" (decisão do Pedro de 03/08/2026, a
        # mesma do PoP); no Early, voltar do Late é "transferiu".
        ["novo", "novo" if focus == "late" else "transferiu", "reabriu", "puxou",
         "saiu", "won", "lost", "late" if focus == "early" else "transferiu",
         "regrediu", "escorregou", "cresceu", "encolheu"],
        default="")
    n_i, s_i = np.nonzero(mud)
    _deals = np.array(deals, dtype=object)
    partes = [pd.DataFrame({
        "deal_id": _deals[n_i], "step": s_i + 1,
        "delta": np.where(in_c, ac, 0.0)[n_i, s_i] - np.where(in_p, ap, 0.0)[n_i, s_i],
        "category": cat[n_i, s_i],
        "stage_from": stg[:, :-1][n_i, s_i], "stage_to": stg[:, 1:][n_i, s_i],
        "cd_from": cp[n_i, s_i], "cd_to": cc[n_i, s_i],
        "amt_from": ap[n_i, s_i], "amt_to": ac[n_i, s_i],
    })]

    # ---- Viradas de trimestre (a partir do 2º passo; o 1º já nasce na régua) ----
    jks = [dias.index(p) for p in pontas]
    viradas = []
    for i in range(2, D):
        if qs[i] == qs[i - 1]:
            continue
        c = i - 1
        velho, novo = _dentro(c, c), _dentro(c, i)
        for m, sinal, nome in ((velho, -1.0, "virada_saiu"), (novo, 1.0, "virada_entrou")):
            partes.append(pd.DataFrame({
                "deal_id": _deals[m], "step": i, "delta": sinal * amt[m, c],
                "category": nome, "stage_from": stg[m, c], "stage_to": stg[m, c],
                "cd_from": cd[m, c], "cd_to": cd[m, c],
                "amt_from": amt[m, c], "amt_to": amt[m, c]}))
        k = int(np.searchsorted(jks, i, side="left"))
        ini, fim = alvos[k - 1], alvos[k]
        q_ini = _quarter_start_of(dias[i])
        viradas.append({
            "week": k, "dia": dias[c],
            "frac": min(1.0, max(0.0, (q_ini - ini).days / max(1, (fim - ini).days))),
            "de": _quarter_label(dias[c]), "para": _quarter_label(dias[i]),
            "label": f"{_quarter_label(dias[c])} → {_quarter_label(dias[i])}",
            "out": float(amt[velho, c].sum()), "in": float(amt[novo, c].sum()),
            "n_out": int(velho.sum()), "n_in": int(novo.sum())})

    ev = pd.concat([p for p in partes if len(p)], ignore_index=True) if any(
        len(p) for p in partes) else pd.DataFrame(columns=_WQ_EV_COLS)
    if len(ev):
        ev["week_idx"] = np.searchsorted(jks, ev["step"].to_numpy(), side="left")
        ev["dia"] = [dias[i] for i in ev["step"]]
        ev["dia_ant"] = [dias[i - 1] for i in ev["step"]]
        # Trimestre cujo escopo (e hierarquia) vale pra linha: o do dia de
        # chegada; a saída da virada pertence ao trimestre que acabou.
        ev["q_escopo"] = [_quarter_start_of(a if c == "virada_saiu" else d)
                          for a, d, c in zip(ev["dia_ant"], ev["dia"], ev["category"])]
        ev = ev.merge(meta[["deal_id", "deal_name", "account_executive", "leader",
                            "region", "deal_type"]], on="deal_id", how="left")
        ev["leader"] = _lider_no_trim(ev)
    out["ev"] = ev.reindex(columns=_WQ_EV_COLS)

    # ---- Nível em cada ponta (a âncora na régua do 1º passo) ----
    lv, reguas = [], []
    for k, j in enumerate(jks):
        jr = 1 if k == 0 else j
        m = _dentro(j, jr)
        reguas.append(_quarter_start_of(dias[jr]))
        lv.append(pd.DataFrame({"deal_id": _deals[m], "week_idx": k, "v": amt[m, j],
                                "stage": stg[m, j], "cd": cd[m, j]}))
    lv = pd.concat(lv, ignore_index=True)
    lv["q_escopo"] = lv["week_idx"].map(dict(enumerate(reguas)))
    lv = lv.merge(meta[["deal_id", "deal_name", "account_executive", "leader",
                        "region"]], on="deal_id", how="left")
    lv["leader"] = _lider_no_trim(lv)
    out["lv"] = lv
    out["viradas"] = viradas
    out["reguas"] = reguas
    return out


def _wq_cd_txt(v) -> str:
    """Close date da grade (dias desde 1970, −1 = vazio) → '30/09/26'."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    if v < 0 or v != v:
        return "sem data"
    return (date(1970, 1, 1) + timedelta(days=int(v))).strftime("%d/%m/%y")


def _wq_bridge(ev, fifth, viradas, level0, level_end):
    """Ponte da janela inteira: do nível inicial ao final, parcela por parcela.
    Aqui ela fecha EXATO (nível e movimento são a mesma grade); o resíduo só
    existe como teste — se aparecer, é bug, e a tela mostra."""
    _cats = _WQ_NET_CATS + (fifth, "won", "transferiu", "regrediu", "reabriu", "saiu")
    tot = {c: float(ev.loc[ev["category"] == c, "delta"].sum()) for c in _cats}
    tot["net"] = sum(tot[c] for c in _WQ_NET_CATS)
    tot["viradas"] = [{**v, "saldo": v["in"] - v["out"]} for v in viradas]
    _virada = float(ev.loc[ev["category"].isin(["virada_saiu", "virada_entrou"]),
                           "delta"].sum())
    tot["residuo"] = (level_end - level0) - float(ev["delta"].sum())
    tot["virada_total"] = _virada
    return tot


# ---------------------------------------------------------------------------
# Cobertura histórica do PoP Histórico Q — pedido do Pedro (13/10/2026): "o
# quanto tínhamos de pipeline em relação à remaining quota", domingo a domingo.
#
#   cobertura(s)          = pipe do foco no trimestre-régua de s ÷ remanescente(s)
#   remanescente(s)       = max(meta do trimestre − realizado até s, 0)
#
# É a conta da aba Cobertura (`_pipeline_page`) andando pra trás no tempo, com
# UMA diferença: aqui o deal do próprio gestor entra no pipe e no realizado (a
# regra do resto do Leaders, `drop_departed_people`); a aba Cobertura ainda o
# tira (HIDDEN_FROM_AE_VIEWS no pipe; realizado só nas linhas de AE). Medido em
# 11/10/2026: R$ 60 mil a mais no Late do Leonardo (LDC Unidades Portuárias).
#   · META: time inteiro = TEAM_REVENUE_META (= soma dos Comp. Target dos 4
#     diretores; bate no centavo no Q3 e no Q4/26); líder filtrado = Comp.
#     Target dele, o mesmo número da barra dele na Cobertura de Leaders.
#     Trimestre sem meta declarada (Q2/26) = soma das quotas — o fallback que a
#     própria Cobertura usa.
#   · REALIZADO: won com PO no trimestre até s, lido da cópia do BI DO PRÓPRIO
#     domingo (`bq.won_asof`), a mesma base do pipe — assim cada deal está num
#     lado só da conta. Validado e hold juntos (os dois abatem a quota).
#   · META DE COBERTURA: a da semana de s. Late = 2x→4x (`_late_coverage_goal`).
#     Early = rampa do ciclo (`_early_coverage_goal`), mas SÓ quando o ciclo
#     aponta pro trimestre-régua: no M3 a meta Early da Cobertura é do pipe do
#     trimestre SEGUINTE contra a quota cheia dele, que não é o pipe desta tela.
# ---------------------------------------------------------------------------

def _ae_quarter_quota(ae, q, f=None, force=False) -> float:
    """Quota de UM AE no trimestre `q` — roster quando o trimestre tem um
    (Q3/Q4 de 2026), linha curada antes disso. Mesma conta da aba Cobertura."""
    f_ae = {**(f or {}), "date_from": "", "date_to": "", "leader": "", "region": "",
            "role": "", "ae": ae}
    for k in ("dtype", "source", "prod", "fcat", "cdate", "cown", "env", "coh", "money"):
        f_ae.setdefault(k, "")
    qdf = _quota_by_ae_for_window(_entity_only_filters(_get_clean_ae_perf(force=force), f_ae),
                                  q, _quarter_end_of(q), f_ae)
    return float(pd.to_numeric(qdf["quota_brl"], errors="coerce").fillna(0).sum())


def _wq_team_meta(q, f, force=False):
    """(meta R$, fonte) do trimestre `q` no recorte de líder da página. Meta
    None = não há meta comparável.

    Líder filtrado num trimestre SEM Comp. Target (Q2/26) fica sem meta, de
    propósito: a soma das quotas sairia do time que ele tinha NAQUELE
    trimestre (líder da linha curada), e o pipe é do time de hoje — no Q2 o
    Arthur não liderava ninguém (meta 0) e o Leonardo só tinha o Raphael
    (R$ 35 mil contra R$ 1,5 mi de pipe = 43x)."""
    A = (f.get("ae") or "").strip()
    if A:
        quota = _ae_quarter_quota(A, q, f, force=force)
        return (quota, "quota do AE") if quota > 0 else (None, "sem quota no trimestre")
    L = (f.get("leader") or "").strip()
    if L and L != "Pedro Mendes":
        targets = _leader_targets(q)
        if L in targets:
            return float(targets[L]), "Comp. Target"
        return None, "sem meta declarada"
    if q in TEAM_REVENUE_META:
        return float(TEAM_REVENUE_META[q]), "meta do time"
    f_ = {**f, "date_from": "", "date_to": "", "ae": "", "role": ""}
    qdf = _quota_by_ae_for_window(_entity_only_filters(_get_clean_ae_perf(force=force), f_),
                                  q, _quarter_end_of(q), f_)
    return (float(pd.to_numeric(qdf["quota_brl"], errors="coerce").fillna(0).sum()),
            "soma das quotas")


def _wq_realizado(fr, f, force=False) -> pd.DataFrame:
    """Realizado do trimestre-régua de cada ponta, COMO ESTAVA na cópia dela.

    Mesmas regras do `_validated_hold_by_ae` da Cobertura: data da receita com a
    extensão de fechamento (`_rev_date` — PO de 01–02/10/2026 é Q3, então não
    abate o Q4), fora quem está em DEPARTED_AES, líder pelo trimestre da receita
    (`_attr_deal_leader`), só ALLOWED_LEADERS e o filtro de líder da página."""
    cols = ["week_idx", "deal_id", "deal_name", "account_executive", "leader",
            "amount", "po"]
    pontas, reguas = fr["pontas"], fr["reguas"]
    if not pontas or len(reguas) != len(pontas):
        return pd.DataFrame(columns=cols)
    sufs = [p.strftime("%Y%m%d") for p in pontas]
    w = bq.won_asof(sufs, pipeline=_pl(), force_refresh=force)
    if not len(w):
        return pd.DataFrame(columns=cols)
    w = w.assign(week_idx=w["dia"].astype(str).map({x: k for k, x in enumerate(sufs)}))
    w = w[w["week_idx"].notna()].astype({"week_idx": int})
    w["po"] = pd.to_datetime(w["po_emission_date"].astype(object), errors="coerce")
    w["rev_po"] = _rev_date(w["po"])
    w = w[w["rev_po"].notna()]
    rq = w["rev_po"].map(lambda t: _quarter_start_of(t.date()))
    w = w[rq == w["week_idx"].map(dict(enumerate(reguas)))]
    w = w[~w["account_executive"].isin(DEPARTED_AES)]
    # Quem saiu: o won dele só conta antes do corte (regra de `_get_forecast_deals`).
    for ae, corte in DEPARTED_FROM.items():
        w = w[~((w["account_executive"] == ae) & (w["rev_po"] >= pd.Timestamp(corte)))]
    w = w.merge(_fc_asof_leader_meta(force=force), on="account_executive", how="left")
    w["leader"] = _attr_deal_leader(w, "rev_po", _period_leader_map(force=force))
    w = w[w["leader"].isin(ALLOWED_LEADERS)]
    if f.get("ae"):
        w = w[w["account_executive"] == f["ae"]]
    elif f.get("leader"):
        reg = w["week_idx"].map(dict(enumerate(reguas)))
        times = {q: _expand_leader_em(f["leader"], q) for q in set(reguas)}
        w = w[[ld in times[q] for ld, q in zip(w["leader"], reg)]]
    return w.reindex(columns=cols)


def _wq_fmt_meta(v) -> str:
    """Meta de cobertura com a casa que ela tem: 2,25x (e não "2,2x", que é o
    que o `_fmt_mult` devolve e esconde o limiar de verdade do teste)."""
    txt = f"{float(v):.2f}".rstrip("0").rstrip(".")
    return txt.replace(".", ",") + "x"


def _wq_fmt_cov(v) -> str:
    """Cobertura TRUNCADA em 1 casa: 2,249x vira "2,2x", nunca "2,25x" — um
    valor abaixo da meta não pode aparecer igual a ela."""
    t = math.floor(float(v) * 10 + 1e-9) / 10
    return (f"{t:.0f}" if t == int(t) else f"{t:.1f}").replace(".", ",") + "x"


def _wq_cobertura(fr, focus, f, force=False) -> list:
    """Uma linha por ponta da série: pipe, meta, realizado, remanescente,
    cobertura e a meta de cobertura da semana."""
    alvos, reguas = fr["alvos"], fr["reguas"]
    if not reguas or len(reguas) != len(alvos):
        return []
    lvl = fr["lv"].groupby("week_idx")["v"].sum()
    rz = _wq_realizado(fr, f, force=force)
    rz_v = rz.groupby("week_idx")["amount"].sum()
    rz_n = rz.groupby("week_idx")["deal_id"].nunique()
    metas = {q: _wq_team_meta(q, f, force=force) for q in set(reguas)}
    rows = []
    for k, (a, q) in enumerate(zip(alvos, reguas)):
        meta, fonte = metas[q]
        real = float(rz_v.get(k, 0.0))
        tem_meta = meta is not None and meta > 0
        rem = max(meta - real, 0.0) if tem_meta else None
        pipe = float(lvl.get(k, 0.0))
        # Semana da meta: a do domingo; na âncora do modo trimestre (véspera do
        # início, já na régua do trimestre novo) é a 1ª semana dele. Presa no
        # trimestre-régua: se a ponta cair numa cópia do trimestre anterior, a
        # meta tem que ser a dele.
        gd = min(max(a, q), _quarter_end_of(q))
        if focus == "late":
            wk, goal = _late_coverage_goal(gd)
        else:
            e = _early_coverage_goal(gd)
            wk, goal = e["week"], (e["goal"] if e["target_q"] == q else None)
        cov = (pipe / rem) if (tem_meta and rem > 0) else None
        if not tem_meta:
            goal = None
        rows.append({
            "k": k, "date": a, "label": _fmt_date_short(a), "curta": _fmt_date_curta(a),
            "regua": q, "regua_label": _quarter_label(q),
            "pipe": pipe, "meta": meta, "meta_fonte": fonte,
            "real": real, "n_real": int(rz_n.get(k, 0)), "rem": rem, "cov": cov,
            "goal": goal, "goal_label": (f"S{wk} · {_wq_fmt_meta(goal)}" if goal is not None
                                         else "—"),
            "tem_meta": tem_meta, "batida": tem_meta and rem == 0,
            # Meta batida (remanescente 0) conta como coberta.
            "hit": tem_meta and (rem == 0 or (goal is not None and cov >= goal)),
            "cov_txt": (_wq_fmt_cov(cov) if cov is not None
                        else ("meta batida" if tem_meta else "sem meta")),
        })
    return rows


def _wq_build_cov(rows, viradas, drill):
    """Geometria do gráfico de cobertura: por domingo, barra verde (pipe) e
    cinza (remanescente) no eixo de R$ à esquerda; linha azul da cobertura e
    degraus tracejados da meta de cobertura no eixo Nx à direita. Mesmas cores
    da aba Cobertura (`_rank_chart.html`), pra ler como a mesma família.

    O topo do eixo Nx é robusto (p90 das coberturas): no fim do trimestre o
    remanescente encolhe e uma semana com R$ 10 mil faltando vira 200x —
    escalar por ela achataria todas as outras. O ponto acima do topo fica
    preso nele, com o valor real no rótulo."""
    _TOP, _PLOT, _BOT = 34, 250, 74
    _LEFT, _RAX = 92, 58
    n = len(rows)
    _STEP = 64 if n <= 14 else 40
    _BW = max(7, round(_STEP * 0.3))
    base_y = _TOP + _PLOT
    right_x = _LEFT + n * _STEP

    mtop = max([r["pipe"] for r in rows] + [r["rem"] or 0.0 for r in rows] + [1000.0]) * 1.12
    yL = [{"label": fmt_brl(mtop * i / 4), "y": round(base_y - i / 4 * _PLOT, 1)}
          for i in range(5)]
    covs = sorted(r["cov"] for r in rows if r["cov"] is not None)
    goals = [r["goal"] for r in rows if r["goal"] is not None]
    ref = covs[min(len(covs) - 1, int(0.9 * len(covs)))] if covs else 0.0
    raw_top = max([ref] + goals + [1.0]) * 1.15
    xstep = 1 if raw_top <= 6 else (2 if raw_top <= 12 else 5)
    xtop = int(math.ceil(raw_top / xstep) * xstep)
    yR = [{"label": _fmt_mult(v), "y": round(base_y - v / xtop * _PLOT, 1)}
          for v in range(0, xtop + 1, xstep)]

    def _ym(v):
        return round(base_y - min(v, xtop) / xtop * _PLOT, 1)

    cols, segs, cur, goal_d = [], [], [], []
    for i, r in enumerate(rows):
        x0 = _LEFT + i * _STEP
        cx = x0 + _STEP / 2
        hp, hr = r["pipe"] / mtop * _PLOT, (r["rem"] or 0.0) / mtop * _PLOT
        cy = _ym(r["cov"]) if r["cov"] is not None else None
        gy = _ym(r["goal"]) if r["goal"] is not None else None
        cols.append({
            **r, "x0": round(x0, 1), "cx": round(cx, 1),
            "pipe_x": round(cx - _BW - 1, 1), "rem_x": round(cx + 1, 1),
            "pipe_y": round(base_y - hp, 1), "pipe_h": round(max(hp, 0.5), 1),
            "rem_y": round(base_y - hr, 1), "rem_h": round(max(hr, 0.5), 1),
            "cov_y": cy, "clamped": r["cov"] is not None and r["cov"] > xtop,
            "url_pipe": drill(r["k"], "pipe"), "url_real": drill(r["k"], "realizado"),
            "is_anchor": i == 0,
        })
        # Linha da cobertura: corta na troca de régua (virada) e em semana sem
        # remanescente, igual à linha de nível.
        nova_regua = i > 0 and rows[i - 1]["regua"] != r["regua"]
        if cy is None or nova_regua:
            if len(cur) > 1:
                segs.append(" ".join(cur))
            cur = []
        if cy is not None:
            cur.append(f"{round(cx, 1)},{cy}")
        if gy is not None:
            goal_d.append(f"M{round(x0 + 3, 1)},{gy}H{round(x0 + _STEP - 3, 1)}")
    if len(cur) > 1:
        segs.append(" ".join(cur))
    brk = [{"x": round(_LEFT + (v["week"] - 0.5 + v["frac"]) * _STEP, 1), "label": v["para"]}
           for v in viradas]
    return {"w": right_x + _RAX, "h": base_y + _BOT, "left": _LEFT, "right_x": right_x,
            "top": _TOP, "base_y": base_y, "step": _STEP, "bw": _BW,
            "yL": yL, "yR": yR, "cols": cols, "segs": segs,
            "goal_d": " ".join(goal_d), "breaks": brk}


def _render_wow_quarter(grain="leader"):
    """PoP Histórico por quarter: N semanas (ou um trimestre) de movimento do
    pipe Early ou Late com close date no trimestre vigente de cada semana.

    grain="ae" é a cópia do Sales AE BR (pedido do Pedro, 20/10/2026): o painel
    por pessoa lista vendedores (gestor fica no total, fora das linhas) e o filtro
    de AE recorta pipe, realizado e meta — com um AE escolhido, a meta é a quota
    dele no trimestre. Desde 21/10/2026 o filtro de AE vale também na tela de
    Leaders (antes era limpo aqui e a tela mostrava o time inteiro)."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    focus = _wow_hist_focus()
    fr = _wq_frames(focus, _wq_scope(f), force=force)
    ev, lv, alvos, viradas = fr["ev"], fr["lv"], fr["alvos"], fr["viradas"]
    weeks = max(0, len(alvos) - 1)
    sel = fr["sel"]
    fifth = "late" if focus == "early" else "won"

    if weeks:
        wk_rows, level0, _ = _hist_week_rows(ev, lv, _WQ_NET_CATS, fifth, weeks, alvos,
                                             level_col="v")
    else:
        wk_rows, level0 = [], 0.0
    # Ponta que caiu numa cópia de outro dia (a do dia faltou ou veio quebrada):
    # o rótulo avisa com asterisco em vez de fingir que é o domingo.
    trocadas = {k for k, (a, p) in enumerate(zip(alvos, fr["pontas"])) if a != p}
    for r in wk_rows:
        r["virada"] = next((v for v in viradas if v["week"] == r["idx"]), None)
        if r["idx"] in trocadas:
            r["label"] += "*"
    level_end = wk_rows[-1]["level"] if wk_rows else level0
    people = (_hist_people(ev[ev["category"].isin(_WQ_NET_CATS)], lv, weeks, grain,
                           f.get("leader"), level_col="v", explode=_wq_explode)
              if weeks else [])
    bridge = _wq_bridge(ev, fifth, viradas, level0, level_end)

    def _drill(idx, cats, name=""):
        # Na tela de AE com um AE escolhido, o total É o AE: o drill sai no nome dele.
        if not name and grain == "ae" and f.get("ae"):
            name = f["ae"]
        return url_for("pipeline_wow_quarter_deals" if grain == "ae"
                       else "leaders_pipeline_wow_quarter_deals", focus=focus, week=idx,
                       weeks=sel, cat=cats, name=name, grain=grain,
                       dtype=(f.get("dtype") or None),
                       leader=(f.get("leader") if not name else None),
                       region=(f.get("region") if not name else None))

    def _chip_url(**kw):
        args = {k: v for k, v in request.args.items() if k not in kw and v}
        return request.path + "?" + urllib.parse.urlencode({**args, **kw})

    bars = _build_hist_bars(wk_rows, weeks, _drill, pos=_WQ_POS, neg=_WQ_NEG,
                            net_cats=_WQ_NET_CATS)
    # Marcador da virada no gráfico de barras: na fração da semana em que o
    # trimestre novo começou (quinta, 01/10 → ~57% da semana 27/09→04/10).
    bar_breaks = [{"x": round(bars["left"] + (v["week"] - 1 + v["frac"]) * bars["step"], 1),
                   "label": v["para"]} for v in viradas]
    q_sel = next((q for q in fr["qopts"] if _wq_quarter_key(q) == sel), None)
    fy_sel = sel if (not q_sel and sel in WQ_FISCAL_YEARS) else None
    janela_b = (_quarter_label(q_sel) if q_sel else fy_sel if fy_sel else f"{weeks} semanas")

    # Cobertura histórica. Com Deal Type ela não existe: a quota não é aberta
    # por tipo, e pipe só de aquisição contra a quota inteira mentiria.
    def _cov_drill(k, kind):
        return url_for("pipeline_wow_quarter_cobertura" if grain == "ae"
                       else "leaders_pipeline_wow_quarter_cobertura", focus=focus, k=k,
                       kind=kind, weeks=sel, leader=(f.get("leader") or None),
                       region=(f.get("region") or None), ae=(f.get("ae") or None))

    cov_rows = (_wq_cobertura(fr, focus, f, force=force)
                if (weeks and not f.get("dtype")) else [])
    for r in cov_rows:
        if r["k"] in trocadas:
            r["label"] += "*"
    cov = _wq_build_cov(cov_rows, viradas, _cov_drill) if cov_rows else None
    return render_template(
        "pipeline_wow_quarter.html", ctx=ctx,
        focus=focus, grain=grain, weeks=weeks, janela_b=janela_b, modo_q=bool(q_sel),
        modo_fy=bool(fy_sel),
        focus_label=("Early · " + _STAGE_BY_ID["93878633"]["name"] if focus == "early"
                     else "Late · Negotiation / Decision / Procurement"),
        fifth_cat=fifth, fifth_label=("Movido p/ Late" if fifth == "late" else "Won"),
        wk_rows=wk_rows, people=people, viradas=viradas,
        level0=level0, level_end=level_end, bridge=bridge,
        first_label=(_fmt_date_short(alvos[0]) if alvos else "—"),
        last_label=(_fmt_date_short(alvos[-1]) if alvos else "—"),
        tem_troca=bool(trocadas),
        bars=bars, bar_breaks=bar_breaks,
        lvl=_build_hist_level(wk_rows, level0, alvos[0] if alvos else _today(),
                              breaks=viradas),
        spark=_build_hist_spark(people, weeks),
        total_net=sum(r["net"] for r in wk_rows),
        total_fifth=sum(r["fifth"] for r in wk_rows),
        total_puxou=sum(r["puxou"] for r in wk_rows),
        total_escorregou=sum(r["escorregou"] for r in wk_rows),
        best=(max(wk_rows, key=lambda r: r["net"]) if wk_rows else None),
        worst=(min(wk_rows, key=lambda r: r["net"]) if wk_rows else None),
        week_opts=([(str(w), f"{w} sem") for w in WOW_HIST_WEEK_OPTS]
                   + [(_wq_quarter_key(q), _quarter_label(q)) for q in fr["qopts"]]
                   + [(k, k) for k, (a, _) in WQ_FISCAL_YEARS.items() if a <= _today()]),
        week_sel=sel, net_cats=",".join(_WQ_NET_CATS),
        hist_channels=list(_WQ_NET_CATS),
        chip_url=_chip_url, drill=_drill,
        cov=cov, cov_now=(cov_rows[-1] if cov_rows else None),
        cov_fontes=sorted({(r["regua_label"], r["meta_fonte"], r["meta"] or 0.0) for r in cov_rows}),
        cov_early_m3=(focus == "early" and any(r["goal"] is None and r["tem_meta"]
                                               for r in cov_rows)),
    )


@app.route("/leaders/pipeline/wow/quarter/cobertura")
def leaders_pipeline_wow_quarter_cobertura():
    return _wq_cobertura_drill("leader")


@app.route("/pipeline/wow/quarter/cobertura")
def pipeline_wow_quarter_cobertura():
    return _wq_cobertura_drill("ae")


def _wq_cobertura_drill(grain):
    """Drill da cobertura de UM domingo: `kind=pipe` lista o pipe que estava no
    numerador; `kind=realizado` lista o won que abateu a quota até ali. Na tela
    de AE o `ae` da URL recorta os dois lados e a meta (quota do AE)."""
    focus = "late" if request.args.get("focus") == "late" else "early"
    f = get_filters()
    try:
        k = int(request.args.get("k") or -1)
    except (TypeError, ValueError):
        k = -1
    fr = _wq_frames(focus, _wq_scope(f))
    if not 0 <= k < len(fr["alvos"]) or len(fr["reguas"]) != len(fr["alvos"]):
        return jsonify(_empty_drill(""))
    q, quando = fr["reguas"][k], _fmt_date_short(fr["alvos"][k])
    qlbl = _quarter_label(q)
    if request.args.get("kind") == "realizado":
        rz = _wq_realizado(fr, f)
        rz = rz[rz["week_idx"] == k]
        meta, fonte = _wq_team_meta(q, f)
        real = float(rz["amount"].sum())
        rows = [{"deal_name": r.deal_name or "—", "deal_url": _hs_deal_url(r.deal_id),
                 "ae": r.account_executive,
                 "po": (r.po.strftime("%d/%m/%y") + (f" · {_rev_tag(r.po)}" if _rev_tag(r.po) else "")
                        if pd.notna(r.po) else "—"),
                 "amount": float(r.amount)} for r in rz.itertuples(index=False)]
        rows.sort(key=lambda x: x["amount"], reverse=True)
        conta = (f"{fonte} {fmt_money(meta)} − realizado {fmt_money(real)} "
                 f"({len(rows)} deals) = remanescente {fmt_money(max(meta - real, 0))}"
                 if meta else f"{fonte} · realizado {fmt_money(real)} ({len(rows)} deals)")
        return jsonify({
            "title": f"Realizado do {qlbl} até {quando}",
            "subtitle": conta,
            "columns": [
                {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
                {"key": "ae", "label": "AE"},
                {"key": "po", "label": "PO"},
                {"key": "amount", "label": "Amount", "type": "money"},
            ],
            "rows": rows,
        })
    lv = fr["lv"][fr["lv"]["week_idx"] == k]
    rows = [{"deal_name": r.deal_name or "—", "deal_url": _hs_deal_url(r.deal_id),
             "ae": r.account_executive, "estagio": stage_display(r.stage),
             "close": _wq_cd_txt(r.cd), "amount": float(r.v)}
            for r in lv.itertuples(index=False)]
    rows.sort(key=lambda x: x["amount"], reverse=True)
    return jsonify({
        "title": f"Pipe {focus.capitalize()} do {qlbl} em {quando}",
        "subtitle": f"{len(rows)} deals · {fmt_money(sum(x['amount'] for x in rows))}",
        "columns": [
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "ae", "label": "AE"},
            {"key": "estagio", "label": "Estágio"},
            {"key": "close", "label": "Close date"},
            {"key": "amount", "label": "Amount", "type": "money"},
        ],
        "rows": rows,
    })


@app.route("/leaders/pipeline/wow/quarter")
def leaders_pipeline_wow_quarter():
    return _render_wow_quarter("leader")


@app.route("/pipeline/wow/quarter")
def pipeline_wow_quarter():
    return _render_wow_quarter("ae")


@app.route("/leaders/pipeline/wow/quarter/deals")
def leaders_pipeline_wow_quarter_deals():
    return _wq_deals_drill()


@app.route("/pipeline/wow/quarter/deals")
def pipeline_wow_quarter_deals():
    return _wq_deals_drill()


def _wq_deals_drill():
    """Drill de UMA semana da série. Uma linha por EVENTO (o mesmo deal aparece
    2× se entrou e saiu na semana), com o estágio e o close date de → para. A
    janela é reconstruída a partir de (weeks, week), como no Histórico PoP."""
    focus = "late" if request.args.get("focus") == "late" else "early"
    f = get_filters()
    try:
        wk = int(request.args.get("week") or 0)
    except (TypeError, ValueError):
        wk = 0
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    fr = _wq_frames(focus, _wq_scope(f, entity=False))
    alvos = fr["alvos"]
    if not 1 <= wk <= len(alvos) - 1:
        return jsonify(_empty_drill(""))

    _all = list(_WQ_NET_CATS) + ["late" if focus == "early" else "won",
                                 "virada_saiu", "virada_entrou"]
    _pedido = (request.args.get("cat") or "").strip()
    cats = ([c for c in _all if c in {x.strip().lower() for x in _pedido.split(",")}]
            if _pedido else list(_WQ_NET_CATS))
    ev = fr["ev"]
    ev = ev[(ev["week_idx"] == wk) & ev["category"].isin(cats)]
    # Recorte de pessoa com a hierarquia do trimestre de cada linha (no Q2/26 o
    # Leonardo inclui o Gustavo) — a mesma régua do escopo da página.
    _L = name if (name and grain == "leader") else ("" if name else
                                                    (request.args.get("leader") or ""))
    if name and grain == "ae":
        ev = ev[ev["account_executive"] == name]
    elif _L:
        times = {q: _expand_leader_em(_L, q) for q in pd.unique(ev["q_escopo"])}
        ev = ev[[ld in times[q] for ld, q in zip(ev["leader"], ev["q_escopo"])]]
    if not name and request.args.get("region"):
        ev = ev[ev["region"] == request.args.get("region")]
    # Filtro de AE da tela por cima do clique (líder/região acima seguem a
    # hierarquia do trimestre de cada linha).
    if request.args.get("ae"):
        ev = ev[ev["account_executive"] == request.args.get("ae")]

    def _lbl(sid):
        return "—" if sid == _WQ_AUS else stage_display(sid)

    rows = []
    for r in ev.itertuples(index=False):
        est = (_lbl(r.stage_from) if r.stage_from == r.stage_to
               else f"{_lbl(r.stage_from)} → {_lbl(r.stage_to)}")
        cdf, cdt = _wq_cd_txt(r.cd_from), _wq_cd_txt(r.cd_to)
        if r.stage_from == _WQ_AUS:
            cdf = "—"
        if r.stage_to == _WQ_AUS:
            cdt = "—"
        quando = (r.dia.strftime("%d/%m") if (r.dia - r.dia_ant).days <= 1
                  else f"{r.dia_ant.strftime('%d/%m')}–{r.dia.strftime('%d/%m')}")
        if r.category.startswith("virada"):
            quando = f"virada {r.dia_ant.strftime('%d/%m')}"
        rows.append({
            "deal_name": r.deal_name or "—", "deal_url": _hs_deal_url(r.deal_id),
            "ae": r.account_executive,
            "categoria": _WQ_CAT_LABEL.get(r.category, r.category),
            "movimento": est,
            "close": cdf if cdf == cdt else f"{cdf} → {cdt}",
            "quando": quando, "delta": float(r.delta),
            "amount": float(r.amt_to if r.stage_to != _WQ_AUS else r.amt_from),
        })
    rows.sort(key=lambda x: abs(x["delta"]), reverse=True)

    _janela = f"{_fmt_date_short(alvos[wk - 1])} → {_fmt_date_short(alvos[wk])}"
    n_deals = int(ev["deal_id"].nunique())
    _d = f"{n_deals} deal{'s' if n_deals != 1 else ''}"
    if len(rows) != n_deals:
        _d += f" · {len(rows)} movimentos"
    _v = sum(x["delta"] for x in rows)
    _valor = f"{'+' if _v >= 0 else '−'}{fmt_money(abs(_v))}"
    cat = cats[0] if len(cats) == 1 else ""
    if set(cats) == {"virada_saiu", "virada_entrou"}:
        _sufixo = "Virada de trimestre"
    else:
        _sufixo = _WQ_CAT_LABEL.get(cat, "") if cat else "Saldo"
    _escopo = name or f"PoP {focus} por quarter — total"
    return jsonify({
        "title": f"{_escopo} · {_sufixo} · semana {wk}",
        "subtitle": f"{_janela} · {_d} · " + (_valor if cat else f"saldo {_valor}"),
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "categoria", "label": "Movimento"},
            {"key": "movimento", "label": "Estágio (de → para)"},
            {"key": "close",     "label": "Close date (de → para)"},
            {"key": "quando",    "label": "Cópia do BI", "align": "right"},
            {"key": "delta",     "label": "Δ (R$)", "type": "money_signed"},
            {"key": "amount",    "label": "Amount", "type": "money"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


@app.route("/pipeline/visits")
def pipeline_visits():
    return _pipeline_visits_page("ae")


@app.route("/leaders/pipeline/visits")
def leaders_pipeline_visits():
    return _pipeline_visits_page("leader")


def _pipeline_visits_page(grain="ae"):
    """Sub-aba Visitas do Pipeline (AE e Leaders). 2 charts de contagem de visitas
    (objeto HubSpot) por vendedor/líder:
      1) Campo — vendedor no cliente (visitas fora do IA Center)
      2) IA Center — cliente na Tractian (pipeline 890777614)
    grain='leader' → soma do time via _leader_rollup."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    today = _today()
    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else _quarter_start_of(today)
    to_d   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else _quarter_end_of(today)
    lo, hi = pd.Timestamp(from_d), pd.Timestamp(to_d)

    v = _get_visits(force=force)
    v = v[v["leader"].isin(ALLOWED_LEADERS)]
    v = v[~v["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    v = v[v["start_date"].notna() & (v["start_date"] >= lo) & (v["start_date"] <= hi)]
    if f["region"]:
        v = v[v["region"] == f["region"]]
    if f["leader"]:
        v = v[v["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        v = v[v["account_executive"] == f["ae"]]

    # Totais reais do time (deal-level — nunca da soma de linhas de líder).
    tot_field = int((v["category"] == "field").sum())
    tot_ia    = int((v["category"] == "ia_center").sum())

    field_by_ae = v[v["category"] == "field"].groupby("account_executive").size()
    ia_by_ae    = v[v["category"] == "ia_center"].groupby("account_executive").size()
    meta = (v.drop_duplicates("account_executive").set_index("account_executive")[["leader", "region"]]
            if len(v) else pd.DataFrame(columns=["leader", "region"]))
    aes = sorted(set(field_by_ae.index) | set(ia_by_ae.index))
    by_ae = [{
        "account_executive": a,
        "leader": (meta.loc[a, "leader"] if a in meta.index else None),
        "region": (meta.loc[a, "region"] if a in meta.index else None),
        "field": int(field_by_ae.get(a, 0)), "ia": int(ia_by_ae.get(a, 0)),
    } for a in aes]

    if grain == "leader":
        by_ae = _leader_rollup(by_ae, f["leader"], ["field", "ia"])
        vpath, dkey = "/leaders/pipeline/visits", "leader"
    else:
        vpath, dkey = "/pipeline/visits", "ae"

    _drill = {k: val for k, val in (("date_from", f["date_from"]), ("date_to", f["date_to"]),
                                    ("leader", f["leader"]), ("region", f["region"])) if val}

    def _items(metric):
        rows = sorted([r for r in by_ae if (r.get(metric) or 0) > 0],
                      key=lambda r: r[metric], reverse=True)
        out = []
        for r in rows:
            nm = r.get("name") or r["account_executive"]
            out.append({"name": nm, "short": r.get("short") or short_name(nm),
                        "leader": r.get("leader"), "realized": int(r[metric]),
                        "quota": 0, "attainment": None})
        return out

    chart_field = (_build_rank_chart(_items("field"), _drill, axis_mode="count",
                                     drill_path=vpath, drill_key=dkey)
                   if tot_field else None)
    chart_ia = (_build_rank_chart(_items("ia"), _drill, axis_mode="count",
                                  drill_path=vpath, drill_key=dkey)
                if tot_ia else None)

    range_label = f"{from_d.strftime('%d/%m/%Y')} → {to_d.strftime('%d/%m/%Y')}"
    return render_template("pipeline_visits.html", ctx=ctx, grain=grain,
                           chart_field=chart_field, chart_ia=chart_ia,
                           tot_field=tot_field, tot_ia=tot_ia, range_label=range_label)


# ===========================================================================
# Histórico do Forecast — a série semanal do forecast BRUTO do quarter.
#
# Mesma mecânica do Histórico PoP (nível · movimento · pessoa a pessoa), com
# duas diferenças de fundo, as duas ditadas pelo dado e não por estilo:
#
#   1) O QUE DEFINE "ESTAR DENTRO". No PoP é o GRUPO DE ESTÁGIO. Aqui são duas
#      condições: estágio aberto (1-6) E close date dentro do quarter. Um deal
#      sai do forecast tanto por mudar de estágio quanto por ESCORREGAR de data
#      — e o 2º caminho é o mais comum. Por isso o close date desta tela é o
#      vigente em cada domingo, não o de hoje: usar o de hoje apagaria
#      retroativamente quem escorregou, encolhendo o passado até ele virar o
#      presente. E é a FOTO do dia que dá essa data (ver
#      `bq.forecast_asof_series`) — a reconstrução anterior, por
#      `close_date_history_df`, errava nos dois sentidos: inflava R$ 3,2 mi o
#      domingo de 30/08/2026 por congelamento e outros R$ 4,2 mi o de 05/07 por
#      inventar close date de deal que ainda não tinha nenhum.
#
#   2) MOVIMENTO E NÍVEL SAEM DA MESMA FONTE. O PoP lê movimento de `wow_events`
#      e nível de `pipeline_level_series` — duas fontes que divergem até 0,9%,
#      e é por isso que a ponte de lá exibe um resíduo nomeado. Aqui o movimento
#      é DERIVADO do próprio nível, domingo contra domingo, então
#          Σ delta ≡ nível_fim − nível_início
#      por construção, e a ponte fecha em zero sempre. Não é capricho: não existe
#      stream de eventos de forecast pra cruzar — o que existe é a FOTO de cada
#      domingo (ver `bq.forecast_asof_series`), e o movimento sai da diferença
#      entre duas fotos.
#
# O preço de (2) é perder o caminho intra-semana: um deal que entrou na quarta e
# saiu na sexta não aparece como dois movimentos, só como saldo zero. No PoP isso
# importava (22 deals numa janela de 1 semana); aqui é aceitável porque a
# pergunta da tela é de NÍVEL — "como o forecast do quarter evoluiu" — e não de
# fluxo intra-semanal.
# ===========================================================================



def _fc_open_stage_ids() -> list:
    """Os 6 estágios abertos (Scheduled→Procurement) direto do STAGE_REGISTRY,
    que já é a fonte única da definição de funil. Uma 2ª lista aqui divergiria
    dele na próxima mudança de estágio — o mesmo motivo pelo qual
    `bq.forecast_asof_series` recebe a lista em vez de embutir a sua."""
    return [s["id"] for s in STAGE_REGISTRY]


def _fc_hist_sundays(q_start: date, q_end: date) -> list:
    """Domingos que delimitam a série: âncora no domingo <= início do quarter,
    depois de 7 em 7 até o último domingo fechado (sem passar do fim do quarter).

    O índice 0 é a ÂNCORA — só nível, nenhum movimento medido — e a semana k é o
    intervalo (domingo[k−1], domingo[k]]. Mesma convenção do Histórico PoP, pra
    que as duas telas leiam "semana que fechou em DD/MM" com o mesmo sentido."""
    last_sun, _ = _last_two_sundays()
    end = min(last_sun, _sunday_on_or_before(q_end))
    s = _sunday_on_or_before(q_start)
    out = []
    while s <= end:
        out.append(s)
        s += timedelta(days=7)
    return out


def _fc_asof_leader_meta(force=False) -> pd.DataFrame:
    """(account_executive → leader, region) pro frame as-of. A cópia diária do BI
    não tem líder: ela é da tabela de DEALS, e a hierarquia mora na curated de
    performance. Usa o snapshot mais recente de cada AE, igual a
    `_get_forecast_deals` — o roster atual é reaplicado depois, no scope."""
    ae = canon_ae_names(bq.ae_performance(force_refresh=force))
    ae = apply_leader_overrides(ae)
    ae = ae[(ae["region"].notna()) & (ae["role"] == "AE")]
    ae["quarter"] = pd.to_datetime(ae["quarter"]).dt.date
    return (ae.sort_values("quarter").drop_duplicates("account_executive", keep="last")
              [["account_executive", "leader", "region"]])


def _fc_hist_frames(q_start: date, q_end: date, scope, force=False):
    """(eventos derivados, níveis, domingos) do Histórico do Forecast.

    A fonte é a CÓPIA DIÁRIA do BI (`bq.forecast_asof_series`): o estado que a
    curated tinha naquele domingo — estágio, amount, close date, tipo e forecast
    category, todos as-of. Ver o bloco de comentário sobre `FC_ASOF_DATASET` em
    bq.py para o porquê de isto ter substituído a reconstrução por
    `hubspot_deals_history` + `close_date_history_df`.

    Colunas que a tela consome:
      · `in_fc`   — o deal estava no forecast do quarter NAQUELE domingo
                    (estágio aberto 1-6 E close date dentro do trimestre);
      · `v`       — amount cheio quando `in_fc`, 0 quando não (bruto);
      · `v_model` — amount × probabilidade do MODELO ANALYTICS (segmento ×
                    estágio × tipo, já com o desconto de segurança);
      · `v_dir`   — amount × peso da FORECAST CATEGORY dos diretores
                    (100/75/50/25%; OMIT/vazio = 0);
      · `forecast_category` — a categoria as-of, preservada crua pra tela poder
                    medir a COBERTURA (ver `_fc_cat_coverage`): em 28/06/2026 só
                    5% do pipeline (por valor) estava numa das 4 categorias
                    previstas, contra 74% em 30/08 — sem esse número ao lado, a
                    curva dos diretores lê como forecast crescendo o que é
                    sobretudo CRM sendo preenchido.

    Só entram domingos que TÊM snapshot. Os que não têm não viram zero nem
    interpolação: viram buraco na série, e a tela diz quais faltam."""
    suns = _fc_hist_sundays(q_start, q_end)
    disp = bq.forecast_asof_snapshots(force_refresh=force)
    mapa = dict(zip(disp["dia"], disp["sufixo"])) if len(disp) else {}
    idx = {s.strftime("%Y%m%d"): k for k, s in enumerate(suns)}
    sufixos = [mapa[d] for d in idx if d in mapa]
    # Semanas com FOTO — vem da existência da tabela, não do frame filtrado: um
    # líder sem deal numa semana não é "semana sem dado", é semana com zero.
    com_foto = {k for d, k in idx.items() if d in mapa}

    raw = bq.forecast_asof_series(sufixos, _fc_open_stage_ids(),
                                  force_refresh=force, pipeline=_pl())
    weeks = max(0, len(suns) - 1)
    if not len(raw):
        cols = ["deal_id", "deal_name", "account_executive", "leader", "region",
                "week_idx", "stage", "amount", "close_date", "sunday",
                "in_fc", "v", "v_model", "v_dir", "forecast_category"]
        vazio = pd.DataFrame(columns=cols)
        return _fc_hist_events(vazio, 0), vazio, suns, set()

    lv = canon_ae_names(raw).copy()
    lv["week_idx"] = lv["dia"].map(idx)
    lv = lv[lv["week_idx"].notna()]
    lv["week_idx"] = lv["week_idx"].astype(int)
    lv["sunday"] = lv["week_idx"].map(dict(enumerate(suns)))
    lv["amount"] = pd.to_numeric(lv["amount"], errors="coerce").fillna(0.0)
    lv["close_date"] = pd.to_datetime(lv["close_date"], errors="coerce")

    # Hierarquia + roster/allowlist/líder/região (o `scope` é o mesmo das outras
    # telas de histórico).
    lv = lv.merge(_fc_asof_leader_meta(force=force), on="account_executive", how="left")
    # Venda direta de gestor: o "AE" do deal é o próprio líder, então ele não
    # casa no meta de role='AE'. Mesma atribuição que `_get_forecast_deals` faz.
    mgr = lv["account_executive"].isin(NON_AE_USERS)
    lv.loc[mgr, "leader"] = lv.loc[mgr, "account_executive"]
    lv.loc[mgr, "region"] = lv.loc[mgr, "region"].fillna("BR")
    lv = scope(lv)

    lo, hi = pd.Timestamp(q_start), pd.Timestamp(q_end)
    lv["in_fc"] = (lv["stage"].isin(_fc_open_stage_ids())
                   & lv["close_date"].notna()
                   & (lv["close_date"] >= lo) & (lv["close_date"] <= hi))
    lv["v"] = lv["amount"].where(lv["in_fc"], 0.0)

    # Peso do Modelo Analytics, vetorizado por (segmento, estágio, tipo): o frame
    # tem ~15 mil linhas e é remontado a cada request.
    seg = (lv["account_executive"].map(_mx_ae_segment) if _is_mx()
           else lv["leader"].map(LEADER_SEGMENT))
    st = lv["stage"].map(MODEL_STAGE)
    kind = lv["deal_type"].map({"Aquisition": "acq", "Up & cross sell": "exp"})
    prob = pd.Series(0.0, index=lv.index)
    ok = seg.notna() & st.notna() & kind.notna()
    if ok.any():
        prob.loc[ok] = [FORECAST_MODEL_PROB[a][b][c] * MODEL_SAFETY_FACTOR
                        for a, b, c in zip(seg[ok], st[ok], kind[ok])]
    lv["v_model"] = lv["v"] * prob

    # Peso dos diretores, com a MESMA tabela da aba Forecast
    # (FORECAST_CATEGORY_PCT_EFF, já com o fator de correção — a série histórica
    # inteira é recalculada aqui, então ela nasce corrigida junto com o atual):
    # OMIT vale 0 e "sem categoria" também cai em 0 pelo
    # fillna — é o que a aba faz com os "não previstos". A distinção entre os dois
    # zeros não se perde: `forecast_category` fica crua no frame e
    # `_fc_cat_coverage` a usa pra medir cobertura.
    pct = lv["forecast_category"].map(FORECAST_CATEGORY_PCT_EFF)
    lv["v_dir"] = lv["v"] * pct.fillna(0.0)

    ev = _fc_hist_events(lv, weeks, value_col="v_model", presence_col="in_fc",
                         com_foto=com_foto)
    return ev, lv, suns, com_foto


def _fc_hist_events(lv, weeks: int, value_col="v", presence_col=None,
                    com_foto=None):
    """Movimento derivado: uma linha por (deal, semana) em que o valor mudou.

    `value_col` escolhe a BASE do movimento: "v" é o amount cheio (bruto) e
    "v_model" é o amount ponderado pelo Modelo Analytics. A tela usa o ponderado
    — é o número que o líder compara com a meta — e o drill precisa usar a MESMA
    coluna, senão a soma dos deals do drill não fecha com a barra clicada.

    `presence_col` é o que decide ENTRADA e SAÍDA (tipicamente `in_fc`), e existe
    porque no ponderado o valor zero é ambíguo: um deal em Scheduled/Qualification/
    Project Scoping está DENTRO do forecast do quarter e vale 0 no modelo (só
    Negotiation/Decision/Procurement têm probabilidade). Sem esta coluna, o deal
    que só avançou de estágio entrava como "novo" — media-se 40% do valor da barra
    verde do Q3/2026 assim (R$ 1,42 mi de R$ 3,53 mi; na semana 5, 87% da barra).
    Avanço de estágio é "ganhou peso", não entrada.

    `com_foto` limita o movimento às semanas MEDÍVEIS: só existe delta quando os
    DOIS domingos do par têm foto. Sem isso, uma semana sem snapshot virava barra
    de saldo zero (indistinguível de "nada aconteceu") e a primeira semana com
    foto virava uma coluna fantasma de "entrou tudo" — o Q1/2026 mostrava
    R$ 2,03 mi de entrada em 15/02 que era a série começando.

    Categorias — de propósito as MESMAS do PoP (`_WOW_NET_CATS` + um 5º), pra
    que os construtores de linha, barra e heatmap sejam os mesmos e as duas
    telas se leiam igual:
      novo     entrou no forecast do quarter (deal criado, estágio abriu, ou o
               close date entrou no trimestre);
      cresceu  / encolheu — mexeu de valor/peso sem entrar nem sair;
      lost     saiu SEM ser ganho: escorregou pra fora do quarter, foi perdido
               ou voltou pra um estágio fora de 1-6;
      won      saiu por ter sido GANHO — o 5º, separado justamente pra que won e
               escorregada não virem a mesma barra vermelha."""
    cols = ["deal_id", "week_idx", "delta", "category",
            "v_prev", "v_curr", "deal_name", "account_executive", "leader", "region"]
    if not len(lv) or weeks < 1:
        return pd.DataFrame(columns=cols)

    def _piv(col, dtype=None):
        d = lv.assign(_x=lv[col].astype(dtype) if dtype else lv[col])
        piv = d.pivot_table(index="deal_id", columns="week_idx", values="_x",
                            aggfunc="sum", fill_value=0)
        for k in range(weeks + 1):      # semana sem nenhum deal ainda é coluna
            if k not in piv.columns:
                piv[k] = 0
        return piv[sorted(piv.columns)]

    piv = _piv(value_col, float)
    pres = _piv(presence_col, float) if presence_col else (piv != 0).astype(float)

    # Estágio de cada (deal, domingo) — só pra distinguir won de escorregada.
    stg = (lv.drop_duplicates(["deal_id", "week_idx"])
             .set_index(["deal_id", "week_idx"])["stage"])
    # Dono de cada (deal, semana): quem era o AE NAQUELE domingo. Congelar no dono
    # da 1ª foto jogava o movimento no time errado quando o deal troca de mão —
    # 101 deals mudaram de AE dentro do Q3/2026, R$ 360 mil de eventos.
    dono = (lv.drop_duplicates(["deal_id", "week_idx"])
              .set_index(["deal_id", "week_idx"])[["deal_name", "account_executive",
                                                   "leader", "region"]])

    medivel = (None if com_foto is None
               else {k for k in range(1, weeks + 1)
                     if k in com_foto and (k - 1) in com_foto})

    partes = []
    for k in range(1, weeks + 1):
        if medivel is not None and k not in medivel:
            continue
        prev, curr = piv[k - 1], piv[k]
        p_prev, p_curr = pres[k - 1] > 0, pres[k] > 0
        d = curr - prev
        m = (d != 0) | (p_prev != p_curr)
        if not m.any():
            continue
        partes.append(pd.DataFrame({
            "deal_id": piv.index[m], "week_idx": k,
            "v_prev": prev[m].to_numpy(), "v_curr": curr[m].to_numpy(),
            "delta": d[m].to_numpy(),
            "dentro_antes": p_prev[m].to_numpy(),
            "dentro_agora": p_curr[m].to_numpy(),
        }))
    if not partes:
        return pd.DataFrame(columns=cols)
    ev = pd.concat(partes, ignore_index=True)

    def _cat(r):
        # Sem underscore no nome de propósito: itertuples() renomeia coluna que
        # começa com "_" pra posicional (_1, _2) e o acesso por nome quebra.
        if not r.dentro_antes and r.dentro_agora:
            return "novo"
        if r.dentro_antes and not r.dentro_agora:
            return ("won" if stg.get((r.deal_id, r.week_idx)) == _HS_WON_STAGE
                    else "lost")
        return "cresceu" if r.delta > 0 else "encolheu"

    ev["category"] = [_cat(r) for r in ev.itertuples()]
    # Entrada/saída com delta 0 (deal que entrou num estágio sem peso no modelo,
    # ou saiu valendo 0) não é movimento nenhum na série ponderada: some, senão a
    # barra ganha deals que não mexeram o número.
    ev = ev[(ev["delta"] != 0)]
    if not len(ev):
        return pd.DataFrame(columns=cols)
    ev = ev.join(dono, on=["deal_id", "week_idx"])
    return ev.reindex(columns=cols)


# ---------------------------------------------------------------------------
# As DUAS séries semanais da tela. Uma por modelo de forecast, porque são duas
# perguntas diferentes e os números não se substituem:
#
#   · MODELO ANALYTICS — peso = probabilidade histórica de conversão do
#     segmento × estágio × tipo. Reconstruível pro quarter INTEIRO, porque a
#     cópia diária do BI tem estágio, amount e tipo as-of.
#   · MODELO DOS DIRETORES (100/75/50/25%) — peso = `hs_manual_forecast_category`,
#     o julgamento do dono do deal. Também reconstruível pro quarter inteiro,
#     porque a cópia diária do BI guarda a categoria as-of. O que NÃO se pode
#     fazer é aplicar a categoria de HOJE ao passado: um deal que hoje é Commit
#     era Pipeline em julho, o passado sairia alto e a curva viria "caindo" por
#     artefato de método. Ler a foto do dia não tem esse problema; o viés dela é
#     outro, e está medido em `_fc_cat_coverage` (a categoria vai sendo
#     PREENCHIDA durante o trimestre).
#
# Os pontos que não existem entram como None e a linha PULA — não como zero, que
# seria afirmar que o forecast era nulo naquela semana.
# ---------------------------------------------------------------------------

def _fc_cat_coverage(lv, suns, com_foto):
    """Como o pipeline estava CLASSIFICADO em cada domingo, POR VALOR:
      · `prev` — % numa das 4 categorias previstas (100/75/50/25%);
      · `omit` — % marcado como Not forecasted (peso 0 de propósito);
      · `sem`  — % sem categoria nenhuma (ninguém preencheu).

    Isto vai pra tela junto da série dos diretores porque sem isso o gráfico
    mente por omissão. Medido no Q3/2026: em 28/06 só 5% do pipeline (por valor)
    estava numa categoria prevista — 39% sem categoria e 56% em Not forecasted —
    contra 74% em 30/08. A curva do modelo dos diretores sobe 18x nesse período, e
    a maior parte disso é o CRM sendo preenchido, não confiança nova do time.

    `omit` fica separado de `sem` de propósito: "o AE disse que não entra" é uma
    informação; "ninguém preencheu" é a falta dela."""
    out = []
    for k in range(len(suns)):
        if k not in com_foto:
            out.append(None)
            continue
        wk = lv[(lv["week_idx"] == k) & lv["in_fc"]] if len(lv) else lv
        tot = float(wk["v"].sum()) if len(wk) else 0.0
        if not tot:
            out.append(None)
            continue
        cat = wk["forecast_category"]
        prev = float(wk.loc[cat.isin(FORECAST_CATEGORY_ORDER), "v"].sum())
        vazio = cat.isna() | (cat.astype(str).str.strip() == "")
        out.append({"prev": 100.0 * prev / tot,
                    "sem": 100.0 * float(wk.loc[vazio, "v"].sum()) / tot,
                    "omit": 100.0 * float(wk.loc[cat.eq("OMIT"), "v"].sum()) / tot})
    return out


def _fc_nota_cobertura(cob, suns) -> str:
    """A nota que acompanha a série dos diretores — ver `_fc_cat_coverage`."""
    vis = [(k, c) for k, c in enumerate(cob) if c is not None]
    if not vis:
        return ""
    (k0, c0), (k1, c1) = vis[0], vis[-1]
    detalhe = " · ".join(f"{_fmt_date_short(suns[k])} <b>{c['prev']:.0f}%</b>" for k, c in vis)
    return (
        "<b>Leia junto com a cobertura.</b> Este peso é o julgamento do dono do deal, "
        "então a curva só é comparável entre semanas se a categoria estiver preenchida. "
        f"Em {_fmt_date_short(suns[k0])}, <b>{c0['prev']:.0f}%</b> do pipeline (por valor) "
        f"estava numa das 4 categorias previstas — {c0['sem']:.0f}% sem categoria nenhuma e "
        f"{c0['omit']:.0f}% em <i>Not forecasted</i>. Em {_fmt_date_short(suns[k1])} são "
        f"<b>{c1['prev']:.0f}%</b> ({c1['sem']:.0f}% e {c1['omit']:.0f}%). "
        "Ou seja: boa parte da subida desta curva é CRM sendo preenchido, não confiança nova — "
        "o Modelo Analytics ao lado não tem esse viés. Categoria prevista, semana a semana: "
        f"{detalhe}.")


def _fc_series_team(lv, suns, col, com_foto):
    """Série do time (uma soma por domingo) de uma das colunas ponderadas.
    Domingo sem foto devolve None — buraco na linha, não zero: zero afirmaria que
    o forecast era nulo naquela semana. `com_foto` é o conjunto de semanas que
    TÊM snapshot, e vem da existência da tabela — não do frame, que já está
    filtrado por líder/região e pode estar legitimamente vazio numa semana."""
    out = []
    for k in range(len(suns)):
        if k not in com_foto:
            out.append(None)
            continue
        wk = lv[lv["week_idx"] == k] if len(lv) else lv
        out.append(float(wk[col].fillna(0.0).sum()) if len(wk) else 0.0)
    return out


def _fc_series_by_leader(lv, suns, col, leader_filter, com_foto):
    """Uma série por LÍDER, na convenção das telas de Leaders: o diretor engloba
    o gerente (via `_explode_leaders`), então a soma das linhas não fecha com o
    time — de propósito, igual à matriz semana × líder."""
    ex = _explode_leaders(lv, leader_filter)
    if not len(ex):
        return []
    out = []
    for name, grp in ex.groupby("account_executive"):
        vals = []
        for k in range(len(suns)):
            if k not in com_foto:
                vals.append(None)
                continue
            wk = grp[grp["week_idx"] == k]
            vals.append(float(wk[col].fillna(0.0).sum()) if len(wk) else 0.0)
        out.append({"name": name, "short": short_name(name), "values": vals})
    # Ordem = LEADER_ORDER (hierarquia), não alfabética nem por valor: a mesma
    # sequência do dropdown e do organograma, pra leitura ficar previsível.
    ordem = {L: i for i, L in enumerate(LEADER_ORDER)}
    out.sort(key=lambda r: ordem.get(r["name"], 99))
    return out


def _fc_done_cumulative(f, suns, q0, q1, force=False):
    """Done (PO Won) ACUMULADO até cada domingo, no time e por líder.

    Fato imutável: um deal ganho não deixa de ter sido ganho, e a data de
    validação não muda depois. Por isso o acumulado sai da base de HOJE em vez do
    histórico semanal — não há nada a reconstruir. O recorte de quarter é o mesmo
    da aba Forecast (data de validação, não close date), com a extensão de
    fechamento (`rev_date`): validado em 01–02/10/2026 é Done do Q3, não do Q4."""
    deals = _get_forecast_deals(force=force)
    lo, hi = pd.Timestamp(q0), pd.Timestamp(q1)
    won = deals[deals["is_won"] & deals["rev_date"].notna()
                & (deals["rev_date"] >= lo) & (deals["rev_date"] <= hi)].copy()
    won = _fc_hist_scope(f, cut_from=q0)(won)
    team = [float(won[won["rev_date"] <= pd.Timestamp(s)]["amount"].sum())
            for s in suns]
    ex = _explode_leaders(won, f.get("leader"))
    by = {}
    if len(ex):
        for name, grp in ex.groupby("account_executive"):
            by[name] = [float(grp[grp["rev_date"] <= pd.Timestamp(s)]["amount"].sum())
                        for s in suns]
    return team, by


def _fc_sum_series(a, b):
    """Soma duas séries ponto a ponto preservando os buracos: None + x = None.
    (Projeção = Done + Forecast; onde o forecast não existe, a projeção também
    não — somar só o Done desenharia uma curva que não é de projeção nenhuma.)"""
    out = []
    for i, va in enumerate(a):
        vb = b[i] if i < len(b) else None
        out.append(None if (va is None or vb is None) else float(va) + float(vb))
    return out


def _build_fc_line(suns, series, goal=None, y_max=None):
    """Geometria SVG da linha de uma das séries de forecast, em R$.

    Difere do `_build_line_chart` (que é em %) em três coisas que esta tela pede:
      · eixo em moeda, com o filtro mestre MRR/ARR aplicado pelo fmt_brl;
      · LINHA DE META horizontal — sem ela a série não responde "estamos acima ou
        abaixo do alvo?", que é a pergunta da tela;
      · buraco na série: valor None não desenha ponto E parte a linha em dois
        segmentos, em vez de interpolar por cima de uma semana sem dado.

    Eixo Y começa em 0: numa série de nível, cortar a base transforma 5% de
    oscilação em tobogã (mesma decisão do `_build_hist_level`)."""
    _TOP, _PLOT, _BOT = 34, 250, 76
    # _RIGHT = espaço do rótulo da meta na margem. 78 e não 70 porque em ARR US$
    # o rótulo é maior ("US$ 5,95M" = 62px contra "R$ 2,50M" = 54px) e sobravam
    # 2,4px de folga — um valor de 2 dígitos passaria por cima da borda.
    _LEFT, _RIGHT = 96, 78
    n = max(len(suns), 1)
    # Passo dimensionado pra largura TOTAL caber no painel (~1.100px no desktop):
    # com 96 o gráfico de 10 pontos dava 1.134 e o container abria scroll
    # horizontal — o rótulo da meta, que fica na margem direita, ficava cortado.
    _STEP = 88 if n <= 11 else (60 if n <= 16 else 44)
    base_y = _TOP + _PLOT
    right_x = _LEFT + n * _STEP

    vals = [v for s in series for v in s["values"] if v is not None]
    top = y_max or (max(vals + ([goal] if goal else []) + [1000.0]) * 1.10)
    yL = [{"label": fmt_brl(top * k / 4), "y": round(base_y - k / 4 * _PLOT, 1)}
          for k in range(5)]
    # Rótulo de data: "28 jun 2026" (11 chars) só cabe em toda coluna com passo
    # largo. Com passo médio o ANO sai (ele já está no h1 e no subtítulo), o que
    # corta a largura quase pela metade; com passo estreito, além disso, rotula
    # uma coluna a cada duas — sempre incluindo a última. Medido antes: 26 de 26
    # pares de datas se sobrepondo numa janela de 27 pontos.
    _curto = _STEP < 70
    _pula = _STEP < 56
    def _rot(i, sd):
        if _pula and i % 2 != (len(suns) - 1) % 2:
            return ""
        return _fmt_date_curta(sd) if _curto else _fmt_date_short(sd)
    xs = [{"label": _rot(i, sd),
           "cx": round(_LEFT + i * _STEP + _STEP / 2, 1),
           "zx": round(_LEFT + i * _STEP, 1)} for i, sd in enumerate(suns)]

    def _y(v):
        return round(base_y - min(v / top, 1.0) * _PLOT, 1)

    # ---- 1ª passada: geometria dos pontos e das linhas ----
    out = []
    for s in series:
        segs, cur, pontos = [], [], []
        for i, v in enumerate(s["values"]):
            if v is None:                      # buraco: parte a linha em dois
                if len(cur) > 1:
                    segs.append(" ".join(cur))
                cur = []
                continue
            cx, cy = xs[i]["cx"], _y(float(v))
            cur.append(f"{cx},{cy}")
            pontos.append({"i": i, "cx": cx, "cy": cy, "v": float(v)})
        if len(cur) > 1:
            segs.append(" ".join(cur))
        out.append({"name": s["name"], "color": s["color"], "dash": s.get("dash", ""),
                    "segs": segs, "points": pontos})

    # ---- 2ª passada: rótulos, com registro do que já está ocupado na coluna ----
    #
    # Rotular ponto a ponto sem olhar o vizinho produzia duas colisões reais:
    # rótulo da série de cima em cima da série de baixo (quando as duas terminam
    # próximas, o caso de todo trimestre FECHADO, medido com 85% de sobreposição),
    # e rótulo em cima da tracejada da meta. Aqui cada rótulo escolhe entre acima
    # e abaixo do ponto o lado que ainda está livre.
    goal_y = _y(float(goal)) if goal else None
    ocupado = {}                     # índice da coluna → lista de y já usados
    for si, s in enumerate(out):
        n_pts = len(s["points"])
        for pi, p in enumerate(s["points"]):
            # Todos os pontos da 1ª série levam rótulo; das outras, só a ponta —
            # 2 séries × 10 pontos com rótulo em tudo viram mancha. Com eixo
            # apertado (passo < 56, janela de 20+ semanas) a 1ª série também
            # rotula uma coluna a cada duas: as MESMAS que levam data, senão
            # rótulos vizinhos de ~54px se sobrepõem num passo de 44.
            ultimo = pi == n_pts - 1
            if not (si == 0 or ultimo):
                p["label"], p["ly"], p["big"] = "", p["cy"], False
                continue
            if _pula and not ultimo and p["i"] % 2 != (len(suns) - 1) % 2:
                p["label"], p["ly"], p["big"] = "", p["cy"], False
                continue
            # Passo estreito precisa alternar acima/abaixo pra caber; passo largo
            # prefere sempre acima (mais fácil de ler).
            prefere_acima = (si > 0 or _STEP >= 70 or p["i"] % 2 == 0)
            cands = ([p["cy"] - 11, p["cy"] + 18] if prefere_acima
                     else [p["cy"] + 18, p["cy"] - 11])
            # Com passo menor que a largura de um rótulo (~54px), o vizinho de
            # coluna também disputa espaço — o registro tem que olhar os três.
            _viz = ([p["i"] - 1, p["i"], p["i"] + 1] if _STEP < 58 else [p["i"]])
            usados = [y for c in _viz for y in ocupado.get(c, [])]
            escolhido = None
            for c in cands:
                if any(abs(c - u) < 13 for u in usados):
                    continue
                if goal_y is not None and abs(c - goal_y) < 11:
                    continue
                escolhido = c
                break
            if escolhido is None:           # os dois lados ocupados: fica no 1º
                escolhido = cands[0]
            ocupado.setdefault(p["i"], []).append(escolhido)
            p["label"] = fmt_brl(p["v"])
            p["ly"] = round(escolhido, 1)
            p["big"] = si == 0

    g = None
    if goal:
        # Rótulo DENTRO do gráfico é só o valor: "Meta · R$ 2,50M" não cabe na
        # margem direita (78px) e saía cortado em "Me…". Quem nomeia a tracejada
        # é a legenda, que tem o texto inteiro.
        g = {"y": _y(float(goal)), "label": fmt_brl(goal), "value": fmt_brl(goal)}
    return {"w": right_x + _RIGHT, "h": _TOP + _PLOT + _BOT, "left": _LEFT, "top": _TOP,
            "base_y": base_y, "right_x": right_x, "step": _STEP, "plot_h": _PLOT,
            "yL": yL, "xs": xs, "series": out, "goal": g}


def _build_fc_small(rows, goal_by, n_pts, span=None):
    """Small multiples: um mini-gráfico de linha por líder, TODOS na mesma escala.

    A escala comum é o que torna a comparação honesta — normalizar cada card pelo
    próprio máximo faria o forecast de R$ 130 mil do João parecer do tamanho do de
    R$ 900 mil do Fernando (mesma razão do `_build_hist_spark`). A meta de cada um
    entra como tracejada DENTRO do card: é ela que diz se a linha está no lugar.
    """
    _W, _H, _PAD = 30, 78, 9
    # Escala: só o que ESTÁ na tela. Usar LEADER_BOARD_TARGET inteiro fazia o
    # card do João Tavares (meta R$ 184 mil) ser escalado pela meta do Fernando
    # (R$ 899 mil) quando o filtro era só o João — a linha dele ficava nos 40%
    # de baixo do card e o cabeçalho anunciava um topo de ninguém na tela.
    span = span or (max([v for r in rows for v in r["values"] if v is not None]
                        + [float(goal_by.get(r["name"]) or 0.0) for r in rows]
                        + [1000.0]) * 1.10)
    w = max(n_pts, 1) * _W
    out = []
    for r in rows:
        meta = float(goal_by.get(r["name"]) or 0.0)
        segs, cur, pts = [], [], []
        for i, v in enumerate(r["values"]):
            if v is None:
                if len(cur) > 1:
                    segs.append(" ".join(cur))
                cur = []
                continue
            cx = round(i * _W + _W / 2, 1)
            cy = round(_H - _PAD - (float(v) / span) * (_H - 2 * _PAD), 1)
            cur.append(f"{cx},{cy}")
            pts.append({"cx": cx, "cy": cy})
        if len(cur) > 1:
            segs.append(" ".join(cur))
        vis = [v for v in r["values"] if v is not None]
        ini, fim = (vis[0], vis[-1]) if vis else (0.0, 0.0)
        out.append({**r, "segs": segs, "points": pts,
                    "goal_y": (round(_H - _PAD - (meta / span) * (_H - 2 * _PAD), 1)
                               if meta else None),
                    "goal": meta, "start": ini, "end": fim, "delta": fim - ini,
                    "att": (fim / meta) if meta else None})
    return {"w": w, "h": _H, "rows": out, "span": span, "span_label": fmt_brl(span)}


def _fc_hist_bridge(wk_rows, level0, level_end):
    """Ponte da janela. Aqui ela fecha EXATO por construção (nível e movimento
    saem do mesmo `v`), ao contrário da ponte do Histórico PoP, onde o resíduo é
    uma parcela real. O resíduo continua sendo calculado e exibido quando não é
    zero — não porque se espere que aconteça, mas porque, se um dia acontecer,
    é sinal de bug nesta tela e esconder isso seria pior."""
    net = sum(r["net"] for r in wk_rows)
    won = sum(r["fifth"] for r in wk_rows)
    return {"net": net, "won": won,
            "residuo": (level_end - level0) - (net + won)}


def _fc_hist_label(q0: date, q1: date) -> str:
    """Título da janela. "Q3 2026" quando é UM trimestre; "Q1→Q2 2026" quando o
    filtro de datas cruza a virada — antes o h1 dizia "Q1 2026" pra uma série de
    seis meses, porque o rótulo saía só de q0."""
    a = f"Q{(q0.month - 1) // 3 + 1} {q0.year}"
    b = f"Q{(q1.month - 1) // 3 + 1} {q1.year}"
    return a if a == b else f"{a} → {b}"


def _fc_hist_quarter(f) -> tuple:
    """(início, fim) do quarter da série. Sai do MESMO recorte de data da barra
    de filtros que a aba Forecast usa, e não de `today` fixo: assim escolher um
    trimestre passado no filtro leva a série junto, em vez de a tela contradizer
    o filtro que está desenhado logo acima dela. Sem filtro = quarter atual."""
    today = _today()
    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else _quarter_start_of(today)
    to_d = pd.to_datetime(f["date_to"]).date() if f["date_to"] else _quarter_end_of(today)
    return _quarter_start_of(from_d), _quarter_end_of(to_d)


def _fc_hist_scope(f, cut_from=None):
    """Filtro de roster/allowlist/líder/região da tela.

    `drop_departed_people` e NÃO `HIDDEN_FROM_AE_VIEWS`: a lista antiga levava
    embora também os GESTORES, e com eles a venda direta do líder — que a aba
    Forecast conta. Media R$ 73 mil de diferença no domingo de 30/08/2026 (2,1%
    do forecast do modelo), o suficiente pra as duas telas nunca fecharem. Quem é
    gestor continua fora das listas POR AE, mas por `drop_non_ae_people`, no
    ponto certo (ver o bloco "Duas exclusões DIFERENTES").

    `cut_from` (o início do trimestre da série) manda no corte de quem saiu da
    empresa: numa série histórica, quem está em DEPARTED_FROM tem que continuar
    inteiro nos trimestres ANTERIORES ao cutoff dele — é o contrato da lista."""
    def _scope(x):
        x = _apply_roster_current(x, cut_from=cut_from)
        x = x[x["leader"].isin(ALLOWED_LEADERS)]
        x = drop_departed_people(x)
        if f.get("region"):
            x = x[x["region"] == f["region"]]
        if f.get("leader"):
            x = x[x["leader"].isin(expand_leader(f["leader"]))]
        # Filtro de AE da barra (21/10/2026): antes ficava de fora, e as telas que
        # usam este escopo (Histórico/WoW/DoD do Forecast, Pipeline DoD/WoW)
        # mostravam o time inteiro com um AE escolhido — tela e explosão.
        if f.get("ae"):
            x = x[x["account_executive"] == f["ae"]]
        return x
    return _scope


def _fc_hist_missing(suns, com_foto) -> list:
    """Domingos da janela SEM snapshot as-of — a série tem buraco ali, e a tela
    diz quais são em vez de emendar a linha por cima. Substituiu a caixa "origem
    do close date": com a cópia diária do BI não há mais mistura de fontes, o
    close date é o que valia no dia."""
    return [_fmt_date_short(sd) for k, sd in enumerate(suns) if k not in com_foto]


def _render_forecast_history(grain="ae"):
    """Histórico do Forecast: como o forecast do quarter se comportou semana a
    semana, nos DOIS modelos, no total do time e depois líder por líder.

    Duas séries porque são dois julgamentos diferentes do mesmo pipeline, e
    nenhum substitui o outro:
      · Modelo Analytics — probabilidade histórica de conversão (segmento ×
        estágio × tipo). Não depende de ninguém preencher nada;
      · Modelo dos diretores — a forecast category (100/75/50/25%), que é o
        julgamento do dono do deal. Depende de estar preenchida, e é por isso que
        a COBERTURA aparece junto: em 28/06/2026 só 60% do pipeline tinha
        categoria, contra 95% em 30/08 — boa parte da subida da curva é
        preenchimento de CRM, não confiança nova.

    Cada série é comparada com a MESMA meta das outras telas (Comp. Target, via
    LEADER_BOARD_TARGET) — é a pergunta da tela: o forecast está andando na
    direção do alvo?"""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    q0, q1 = _fc_hist_quarter(f)

    ev, lv, suns, com_foto = _fc_hist_frames(q0, q1, _fc_hist_scope(f, cut_from=q0),
                                             force=force)
    weeks = max(0, len(suns) - 1)
    lvf = lv[lv["in_fc"]] if len(lv) else lv
    # Nível e movimento na base do Modelo Analytics: é o número que o líder
    # compara com a meta, e nível e movimento têm que sair da MESMA coluna pra
    # ponte fechar (ver _fc_hist_bridge).
    wk_rows, _lv0, _n0 = _hist_week_rows(ev, lvf, _WOW_NET_CATS, "won", weeks, suns,
                                         level_col="v_model")
    people = _hist_people(ev[ev["category"].isin(_WOW_NET_CATS)], lvf,
                          weeks, grain, f.get("leader"), level_col="v_model")
    # Semanas MEDÍVEIS: só as que têm foto nas duas pontas. Sem este corte, semana
    # sem snapshot virava barra de saldo zero — que se lê como "nada aconteceu" —
    # e a 1ª semana com foto virava coluna fantasma de "entrou tudo".
    medivel = {k for k in range(1, weeks + 1) if k in com_foto and (k - 1) in com_foto}
    wk_rows = [r for r in wk_rows if r["idx"] in medivel]
    # A ponte parte do nível do domingo ANTERIOR à 1ª semana medível (e não do
    # domingo 0, que pode não ter foto: level0 = 0 dizia "o forecast era zero").
    level0 = (float(lvf[lvf["week_idx"] == (wk_rows[0]["idx"] - 1)]["v_model"].sum())
              if wk_rows and len(lvf) else 0.0)
    level_end = wk_rows[-1]["level"] if wk_rows else level0
    bridge = _fc_hist_bridge(wk_rows, level0, level_end)
    # A ponte só fecha se as semanas medíveis forem CONTÍNUAS. Com buraco no meio,
    # o resíduo seria o que aconteceu na semana não medida — informação legítima,
    # mas aí a ponte não é uma conta fechada, e sai da tela.
    ponte_ok = bool(wk_rows) and (len(wk_rows) == wk_rows[-1]["idx"] - wk_rows[0]["idx"] + 1)
    # A matriz semana × pessoa segue as MESMAS colunas do gráfico de movimento:
    # `_hist_people` devolve uma célula por semana da janela, então as semanas não
    # medíveis são recortadas aqui — senão a tabela tinha 13 colunas de dado pra
    # 6 cabeçalhos (e estourava o índice na renderização).
    _idxs = [r["idx"] for r in wk_rows]
    for _p in people:
        _p["weekly"] = [_p["weekly"][i - 1] for i in _idxs]
        _p["total"] = sum(_p["weekly"])
        _p["pos_weeks"] = sum(1 for v in _p["weekly"] if v > 0)
    people = [_p for _p in people if _p["total"] or any(_p["weekly"])
              or _p.get("level_end")]
    people.sort(key=lambda r: r["total"], reverse=True)

    # ---- As duas séries do time + o Done acumulado ----
    fc_model = _fc_series_team(lv, suns, "v_model", com_foto)
    fc_dir = _fc_series_team(lv, suns, "v_dir", com_foto)
    done, done_by = _fc_done_cumulative(f, suns, q0, q1, force=force)
    # KPI "Realizado": acumulado no quarter ATÉ HOJE (ou até o fim dele), não
    # só até o último domingo do eixo — o eixo do Q3/26 para em 27/09 e o card
    # perderia 28–30/09 e os validados na extensão de fechamento (até 02/10).
    _done_fim, _ = _fc_done_cumulative(f, [min(_today(), q1)], q0, q1, force=force)
    proj_model = _fc_sum_series(done, fc_model)
    proj_dir = _fc_sum_series(done, fc_dir)
    cobertura = _fc_cat_coverage(lv, suns, com_foto)

    # ---- Meta: a MESMA das outras telas (Comp. Target dos líderes no recorte).
    # SÓ no trimestre corrente: LEADER_BOARD_TARGET é a planilha do quarter em
    # curso (ver o comentário do dict), então desenhá-la sobre a série de um
    # trimestre passado seria comparar o forecast de abril com a meta de julho.
    # Fora do quarter corrente a tela mostra as séries SEM linha de meta e diz
    # por quê — melhor faltar a referência do que ela estar errada.
    # "Corrente" pela data da RECEITA: durante a extensão de fechamento o
    # trimestre que ainda acumula realizado é o anterior (02/10/2026 → Q3).
    meta_ok = (q0 == _quarter_start_of(_rev_date(_today()))
               and q1 == _quarter_end_of(q0))       # meta é de UM trimestre
    lideres = sorted({L for L in lv["leader"].dropna().unique()}) if len(lv) else []
    meta_team = _leader_meta_total(lideres) if meta_ok else 0.0
    metas_lider = LEADER_BOARD_TARGET if meta_ok else {}
    # Com um AE escolhido, a série é só dele: a meta é a quota dele no trimestre,
    # e o alvo do líder sai dos cartões por líder (seria o time inteiro contra
    # um vendedor).
    if f.get("ae") and meta_ok:
        meta_team = _ae_quarter_quota(f["ae"], q0, f, force=force)
        metas_lider = {}

    # ---- Recorte por líder (o "depois cada um dos diretores") ----
    por_lider_model = _fc_series_by_leader(lv, suns, "v_model", f.get("leader"), com_foto)
    por_lider_dir = _fc_series_by_leader(lv, suns, "v_dir", f.get("leader"), com_foto)
    for r in por_lider_model:
        r["values"] = _fc_sum_series(done_by.get(r["name"], [0.0] * len(suns)), r["values"])
    for r in por_lider_dir:
        r["values"] = _fc_sum_series(done_by.get(r["name"], [0.0] * len(suns)), r["values"])

    def _drill(idx, cats, name=""):
        return url_for("forecast_history_deals", week=idx, cat=cats, name=name,
                       grain=grain,
                       leader=(f.get("leader") if not name else None),
                       region=(f.get("region") if not name else None),
                       date_from=f.get("date_from"), date_to=f.get("date_to"))

    # Topo de eixo COMUM aos dois gráficos. Com cada um usando o próprio máximo,
    # duas séries de tamanhos diferentes desenhavam a mesma altura e comparar os
    # modelos de bater o olho enganava.
    _todos = [v for serie in (proj_model, proj_dir, done) for v in serie if v is not None]
    y_max_comum = max(_todos + [meta_team, 1000.0]) * 1.10
    # Mesma ideia nos small multiples: um span pros DOIS grids, senão o card do
    # mesmo diretor tem a meta em alturas diferentes nos dois modelos — e o
    # cabeçalho manda justamente comparar card a card.
    _cards = [v for r in (por_lider_model + por_lider_dir) for v in r["values"]
              if v is not None]
    span_cards = max(_cards + [float(v) for v in metas_lider.values()]
                     + [1000.0]) * 1.10

    def _fim(serie):
        vis = [v for v in serie if v is not None]
        return vis[-1] if vis else 0.0

    def _ini(serie):
        vis = [v for v in serie if v is not None]
        return vis[0] if vis else 0.0

    # Datas do 1º e do último ponto COM DADO — e não as pontas da janela. Num
    # trimestre parcialmente coberto (o Q1/2026 tem 7 domingos sem foto) o card
    # dizia "de R$ 2,61M em 28 dez 2025", uma data que não tem ponto nenhum.
    _com = sorted(k for k in com_foto if k < len(suns))
    pt_ini_label = _fmt_date_short(suns[_com[0]]) if _com else "—"
    pt_fim_label = _fmt_date_short(suns[_com[-1]]) if _com else "—"

    def _delta_semana(serie):
        """Δ do último ponto contra o ponto COM DADO anterior (e não contra o
        índice −2, que pode ser um domingo sem foto)."""
        vis = [v for v in serie if v is not None]
        return (vis[-1] - vis[-2]) if len(vis) >= 2 else None

    return render_template(
        "forecast_history.html", ctx=ctx, grain=grain, weeks=weeks,
        q_label=_fc_hist_label(q0, q1),
        q_range=f"{q0.strftime('%d/%m/%Y')} → {q1.strftime('%d/%m/%Y')}",
        wk_rows=wk_rows, people=people,
        level0=level0, level_end=level_end, bridge=bridge, ponte_ok=ponte_ok,
        # O subtítulo fala da SÉRIE (1º e último ponto com foto), não da janela:
        # no Q1/2026 ele dizia "13 semanas · 28 dez 2025 → 29 mar 2026" pra uma
        # série que começa em 15/02.
        first_label=pt_ini_label, last_label=pt_fim_label,
        pt_ini_label=pt_ini_label, pt_fim_label=pt_fim_label,
        n_pontos=len(_com),
        wow_model=_delta_semana(proj_model), wow_dir=_delta_semana(proj_dir),
        bars=_build_hist_bars(wk_rows, weeks, _drill),
        # A matriz só precisa da ESCALA (maior |saldo| de célula) pra pintar a
        # intensidade. Antes vinha de `_build_hist_spark`, que montava também as
        # barrinhas do painel de saldo por pessoa — painel que saiu da tela
        # quando os small multiples passaram a ser de NÍVEL, por modelo.
        hm_span=max([abs(v) for p in people for v in p["weekly"]] + [1.0]),
        total_net=sum(r["net"] for r in wk_rows),
        total_won=sum(r["fifth"] for r in wk_rows),
        best=(max(wk_rows, key=lambda r: r["net"]) if wk_rows else None),
        worst=(min(wk_rows, key=lambda r: r["net"]) if wk_rows else None),
        faltando=_fc_hist_missing(suns, com_foto),
        drill=_drill,
        # --- Modelo Analytics
        meta_team=meta_team,
        chart_model=_build_fc_line(
            suns,
            [{"name": "Projeção (Done + Forecast)", "color": "#2563EB",
              "values": proj_model},
             {"name": "Done acumulado (PO Won)", "color": "#0F6E56",
              "values": done, "dash": "5 4"}],
            goal=meta_team, y_max=y_max_comum),
        small_model=_build_fc_small(por_lider_model, metas_lider, len(suns),
                                    span=span_cards),
        proj_model_now=_fim(proj_model), proj_model_ini=_ini(proj_model),
        att_model=(_fim(proj_model) / meta_team) if meta_team else None,
        # --- Modelo dos diretores (forecast category)
        chart_dir=_build_fc_line(
            suns,
            [{"name": "Projeção (Done + Forecast ponderado)", "color": "#7C3AED",
              "values": proj_dir},
             {"name": "Done acumulado (PO Won)", "color": "#0F6E56",
              "values": done, "dash": "5 4"}],
            goal=meta_team, y_max=y_max_comum),
        small_dir=_build_fc_small(por_lider_dir, metas_lider, len(suns),
                                  span=span_cards),
        proj_dir_now=_fim(proj_dir), proj_dir_ini=_ini(proj_dir),
        att_dir=(_fim(proj_dir) / meta_team) if meta_team else None,
        cobertura=cobertura,
        meta_ok=meta_ok,
        sem_serie=not any(v is not None for v in proj_model),
        done_now=(_done_fim[0] if _done_fim else (done[-1] if done else 0.0)),
        rev_note=rev_ext_note(q0.isoformat(), q1.isoformat()),
        nota_dir=_fc_nota_cobertura(cobertura, suns),
        suns_labels=[_fmt_date_short(sd) for sd in suns],
    )


@app.route("/forecast/history")
def forecast_history():
    return _render_forecast_history("ae")


@app.route("/leaders/forecast/history")
def leaders_forecast_history():
    return _render_forecast_history("leader")


@app.route("/forecast/history/deals")
def forecast_history_deals():
    """Drill de UMA semana da série: os deals que entraram, cresceram, encolheram,
    saíram ou foram ganhos naquela semana. A janela é reconstruída aqui a partir
    do quarter + índice da semana — não vem par de datas na querystring que possa
    divergir do que foi clicado (mesma decisão do drill do Histórico PoP)."""
    f = get_filters()
    q0, q1 = _fc_hist_quarter(f)
    suns = _fc_hist_sundays(q0, q1)
    weeks = max(0, len(suns) - 1)
    try:
        wk = int(request.args.get("week") or 0)
    except (TypeError, ValueError):
        wk = 0
    if not 1 <= wk <= weeks:
        return jsonify(_empty_drill(""))
    # Semana sem foto nas duas pontas não é movimento medido: o gráfico não
    # desenha barra ali, e o drill por URL direta também não inventa uma.
    _disp = bq.forecast_asof_snapshots()
    _dias = set(_disp["dia"]) if len(_disp) else set()
    if not all(suns[i].strftime("%Y%m%d") in _dias for i in (wk - 1, wk)):
        return jsonify(_empty_drill(""))

    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))

    _all = list(_WOW_NET_CATS) + ["won"]
    _pedido = (request.args.get("cat") or "").strip()
    if _pedido:
        pedidas = [c.strip().lower() for c in _pedido.split(",")]
        cats = [c for c in _all if c in pedidas]
    else:
        cats = list(_all)

    ev, _lv, _suns, _cf = _fc_hist_frames(q0, q1, _fc_hist_scope(f, cut_from=q0))
    ev = ev[(ev["week_idx"] == wk) & ev["category"].isin(cats)]
    ev = _entity_filter(ev, name, grain,
                        request.args.get("leader"), request.args.get("region"))

    rows = []
    for r in ev.sort_values("delta").itertuples():
        rows.append({
            "deal_name": r.deal_name,
            "deal_url": _hs_deal_url(r.deal_id),   # era o portal 6482678 (errado)
            "account_executive": r.account_executive,
            "leader": r.leader,
            "categoria": _WOW_CAT_LABEL.get(r.category, r.category),
            "de_para": f"{fmt_brl(r.v_prev)} → {fmt_brl(r.v_curr)}",
            "delta": float(r.delta),
        })

    _janela = f"{_fmt_date_short(suns[wk - 1])} → {_fmt_date_short(suns[wk])}"
    _v = sum(x["delta"] for x in rows)
    _sinal = "+" if _v >= 0 else "−"
    # `fmt_money` respeita o filtro mestre de moeda (e o MX, que é sempre ARR
    # US$). O R$ fixo daqui era o último lugar que escrevia "R$" numa tela do
    # México — a barra dizia US$ e o drill dela dizia R$.
    _valor = f"{_sinal}{fmt_money(abs(_v))}"
    cat = cats[0] if len(cats) == 1 else ""
    _n = len(rows)
    subtitle = f"{_janela} · {_n} deal{'s' if _n != 1 else ''} · " + (
        _valor if cat else f"saldo {_valor}")
    _escopo = name or "Forecast do quarter — total"
    _sufixo = _WOW_CAT_LABEL.get(cat, "") if cat else "Saldo"
    return jsonify({
        "title": f"{_escopo} · {_sufixo} · semana {wk}",
        "subtitle": subtitle,
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "categoria", "label": "Movimento"},
            {"key": "de_para", "label": "No forecast (de → para)"},
            {"key": "delta", "label": "Δ (R$)", "type": "money_signed"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


# ===========================================================================
# WoW do Forecast — a MIGRAÇÃO do pipeline entre as faixas de probabilidade,
# domingo contra domingo.
#
# A pergunta desta tela não é "quanto é o forecast" (é a aba Forecast) nem "como
# ele andou no trimestre" (é o Histórico): é O QUE MUDOU DE UMA SEMANA PRA OUTRA
# e, sobretudo, SE A MUDANÇA FOI DE CONFIANÇA. Duas semanas podem fechar com o
# MESMO ponderado querendo dizer coisas opostas — numa, R$ 2 mi subiram de 25%
# pra 50%; na outra, R$ 2 mi entraram como Not forecasted e outros tantos
# escorregaram de trimestre. O nível não distingue as duas; a migração sim.
#
# Por isso o objeto central é a MATRIZ faixa_anterior × faixa_atual, e não o
# nível de cada faixa: o nível é a soma das linhas e das colunas dela.
#
# FONTE: as mesmas cópias diárias do BI do Histórico do Forecast
# (`bq.forecast_asof_series`), duas fotos em vez de treze — mais o fechado
# PERDIDO, que o Histórico não pede e que aqui é uma das faixas (ver a seção
# ">>> LOST É OPCIONAL" no docstring da função em bq.py).
#
# TODO movimento é DERIVADO da diferença entre as duas fotos, porque não existe
# stream de eventos de forecast category em lugar nenhum do BigQuery
# (`hubspot_deals_history` versiona 7 campos e nenhum é ela). O preço é o mesmo
# do Histórico: um deal que subiu na quarta e voltou na sexta aparece como
# "ficou". A vantagem também é a mesma — a ponte fecha em ZERO por construção,
# porque nível e movimento saem da mesma fonte:
#     Σ Δprojeção(deal) ≡ projeção_atual − projeção_anterior
# ===========================================================================

# As 5 faixas de probabilidade, da MENOR pra MAIOR. Esta ordem é o que define
# "subiu" e "desceu" — mora aqui, não no template.
_FCW_BANDS = ("nf", "pipeline", "probable", "bestcase", "commit")
# Desfecho: sai do jogo da probabilidade. Won é o 100% já realizado, Lost é o
# zero definitivo. Nenhum dos dois é FAIXA, e é por isso que mover pra eles não
# conta como subir nem descer de faixa (ver `_fcw_mv`): virar Won não é ganhar
# confiança, é acabar a aposta.
_FCW_OUTCOMES = ("won", "lost")
_FCW_KEYS = _FCW_BANDS + _FCW_OUTCOMES
_FCW_RANK = {k: i for i, k in enumerate(_FCW_BANDS)}
# Forecast category crua do HubSpot → faixa. O que NÃO está aqui (OMIT, CLOSED,
# vazio) cai em `nf`, e o PESO sai da faixa, nunca da categoria crua: senão um
# deal aberto marcado CLOSED — que vale 1,0 em FORECAST_CATEGORY_PCT — entraria
# na faixa de peso 0 valendo 100% no ponderado.
_FCW_BAND_BY_RAW = {"PIPELINE": "pipeline", "PROBABLE": "probable",
                    "BEST_CASE": "bestcase", "COMMIT": "commit"}
# Âmbar do Not forecasted e cinza do "fora": saem do registro do Pipeline Total
# pra as duas telas não pintarem a mesma categoria de cores diferentes.
_FCW_NF_COLOR, _FCW_NF_INK = next((c, i) for _k, raw, _l, c, i in _PT_DIMS["fcat"]["values"]
                                  if raw == "OMIT")
_FCW_OUT_COLOR, _FCW_OUT_INK = next((c, i) for k, _r, _l, c, i in _PT_DIMS["fcat"]["values"]
                                    if k == _PT_DIM_NONE)

_FCW_META = {
    # `w` = peso do modelo dos diretores, a MESMA tabela da aba Forecast, já com
    # o DIR_FORECAST_FACTOR. Won vale 1,0 CHEIO porque já é receita, não previsão
    # — e é por isso que virar Won passa a render um Δ de +10% do amount no
    # `ganho` da ponte: o fator de correção se desfaz quando o deal se realiza.
    # Lost e Not forecasted valem 0 — o primeiro por desfecho, o segundo por
    # decisão de quem preencheu.
    "nf": {"label": "Not forecasted", "hint": "0% · OMIT ou sem categoria",
           "color": _FCW_NF_COLOR, "ink": _FCW_NF_INK, "w": 0.0},
    **{k: {"label": FORECAST_CATEGORY_LABEL[raw],
           "hint": {"PIPELINE": "Funnel", "PROBABLE": "Probable",
                    "BEST_CASE": "Best Case", "COMMIT": "Commit"}[raw],
           "color": FORECAST_CATEGORY_COLOR[raw], "ink": FORECAST_CATEGORY_INK[raw],
           "w": FORECAST_CATEGORY_PCT_EFF[raw]}
       for raw, k in _FCW_BAND_BY_RAW.items()},
    # Verde-escuro do Realizado da aba Forecast (o #047857 já é o Commit) e
    # vermelho do `.kpi.danger`.
    "won":  {"label": "Won", "hint": "PO Won no trimestre",
             "color": "#065F46", "ink": "#065F46", "w": 1.0},
    "lost": {"label": "Lost", "hint": "perdido no trimestre",
             "color": "#B91C1C", "ink": "#B91C1C", "w": 0.0},
    "out":  {"label": "Fora do forecast", "hint": "close date fora do trimestre, "
                                                  "estágio fora dos 6 abertos ou deal inexistente",
             "color": _FCW_OUT_COLOR, "ink": _FCW_OUT_INK, "w": 0.0},
}

# Os movimentos, na ordem em que a tela conta a história: primeiro o que somou,
# depois o que subtraiu. `sinal` é a EXPECTATIVA de direção (só pinta o rótulo);
# o valor de cada linha é medido, não presumido.
_FCW_MV_META = {
    "entrou":  {"label": "Entrou no forecast", "color": "#2563EB", "sinal": 1,
                "hint": "não estava no forecast do trimestre no domingo anterior"},
    "subiu":   {"label": "Subiu de faixa", "color": "#047857", "sinal": 1,
                "hint": "mesma etapa do jogo, mais confiança do dono do deal"},
    "ganho":   {"label": "Ganho (Won)", "color": "#065F46", "sinal": 1,
                "hint": "virou PO Won — o Δ é só a diferença entre o peso que ele "
                        "tinha e os 100% do realizado, e é pequeno de propósito"},
    "repreco": {"label": "Reprecificação", "color": "#6E56CF", "sinal": 0,
                "hint": "ficou na mesma faixa e o amount mudou"},
    "desceu":  {"label": "Desceu de faixa", "color": "#DB2777", "sinal": -1,
                "hint": "o dono do deal tirou confiança"},
    "saiu":    {"label": "Saiu do forecast", "color": "#B45309", "sinal": -1,
                "hint": "escorregou de trimestre ou saiu dos 6 estágios abertos"},
    "perdido": {"label": "Perdido (Lost)", "color": "#B91C1C", "sinal": -1,
                "hint": "closed lost na semana — o Δ é o peso que ele tinha, agora zerado"},
    "recl":    {"label": "Won/Lost reclassificado", "color": "#64748B", "sinal": 0,
                "hint": "deal que era Won ou Lost do trimestre e deixou de ser "
                        "(reabertura, ou data de validação/fechamento fora da janela)"},
}
_FCW_MV_ORDER = ("entrou", "subiu", "ganho", "repreco", "desceu", "saiu",
                 "perdido", "recl")


def _fcw_mv(bp: str, bc: str) -> str:
    """Movimento de um deal entre as duas fotos. A ordem dos testes é o contrato
    da tela, e não é arbitrária:

      · desfecho vence faixa — um Commit que virou Won é "ganho", não "subiu";
      · entrada/saída do trimestre vence faixa — um deal que escorregou pra
        outubro é "saiu", e não "desceu pra fora";
      · sobrar as duas pontas em faixa é o único caso de subiu/desceu, que é
        justamente a pergunta "houve evolução dentro da probabilidade".
    """
    if bp == bc:
        return "repreco"
    if bc == "won":
        return "ganho"
    if bc == "lost":
        return "perdido"
    if bp in _FCW_OUTCOMES:
        return "recl"
    if bp == "out":
        return "entrou"
    if bc == "out":
        return "saiu"
    return "subiu" if _FCW_RANK[bc] > _FCW_RANK[bp] else "desceu"


def _fcw_scope(f, cut_from=None):
    """Escopo desta tela: o MESMO do Histórico do Forecast (roster, allowlist de
    líder, região, venda direta de gestor) mais o corte de Deal Type — que existe
    aqui pelo mesmo motivo das telas PoP: aquisição e expansão migram de faixa em
    ritmos diferentes, e a soma das duas esconde isso."""
    base = _fc_hist_scope(f, cut_from=cut_from)

    def _scope(x):
        return _pt_apply_dim_filter(base(x), "dtype", f.get("dtype", ""))
    return _scope


def _fcw_pairs(q0: date, q1: date, force=False):
    """(pares, curr, prev, faltando) — os pares de domingos COMPARÁVEIS do
    trimestre, o escolhido, e os domingos da janela que não têm foto.

    Par comparável = dois domingos CONSECUTIVOS com foto nas duas pontas. Domingo
    sem cópia as-of não vira zero nem interpolação: ele derruba os DOIS pares que
    tocam nele, porque comparar 30/08 com 16/08 chamando de "semana" seria mentir
    no rótulo. A tela diz quais domingos faltam.

    O par vem do chip da própria página (`?par=YYYYMMDD`, o domingo ATUAL do par)
    e não do date-picker: `_wow_window` ancora cada ponta de forma independente,
    e com date_from/date_to soltos "01/06 → 30/06" viraria a comparação
    "31/mai vs 28/jun" — 4 semanas travestidas de WoW. Este é o "seletor de PAR
    de domingos consecutivos" que o docstring de `_wow_fixed_pair` pede."""
    suns = _fc_hist_sundays(q0, q1)
    disp = bq.forecast_asof_snapshots(force_refresh=force)
    dias = set(disp["dia"]) if len(disp) else set()
    com = {k for k, s in enumerate(suns) if s.strftime("%Y%m%d") in dias}
    faltando = _fc_hist_missing(suns, com)
    pares = [(suns[i], suns[i - 1]) for i in range(1, len(suns))
             if i in com and (i - 1) in com]
    if not pares:
        return [], None, None, faltando
    sel = (request.args.get("par") or "").strip()
    curr, prev = next(((c, p) for c, p in pares if c.strftime("%Y%m%d") == sel),
                      pares[-1])
    return pares, curr, prev, faltando


def _fcw_frame(q0: date, q1: date, curr_sun: date, prev_sun: date, f,
               force=False, raw=None) -> pd.DataFrame:
    """UMA linha por deal, com o estado nas duas pontas e o movimento entre elas.

    Colunas que a tela consome:
      · `b_prev` / `b_curr` — a faixa em cada domingo (uma de `_FCW_KEYS`, ou
        `out` = fora do forecast do trimestre naquele domingo);
      · `v_prev` / `v_curr` — o amount as-of. Deal ausente de uma foto vale 0
        ali, e por isso `out` sempre pesa 0 na projeção;
      · `p_prev` / `p_curr` / `dp` — o amount × peso da FAIXA, e o delta. É a
        projeção dos diretores (Won conta 1,0), a mesma conta da série `v_dir`
        do Histórico;
      · `mv` — o movimento (ver `_fcw_mv`);
      · `motivo` — só pra `saiu`/`entrou`: por onde o deal entrou ou saiu.

    `raw` = as duas fotos já carregadas, com `dia` = YYYYMMDD de cada ponta. É
    como o DoD entra: as pontas dele não são cópias do BI (ver `_fcd_raw`), mas
    todo o resto — faixa, peso, movimento — é esta mesma conta.

    Deal fora do forecast nas DUAS pontas é descartado: são ~2,8 mil linhas de
    ruído (o funil inteiro do Sales BR) que não têm o que dizer sobre a semana."""
    vazio = pd.DataFrame(columns=[
        "deal_id", "deal_name", "account_executive", "leader", "region",
        "deal_type", "b_prev", "b_curr", "v_prev", "v_curr", "p_prev", "p_curr",
        "dp", "mv", "motivo", "fc_prev", "fc_curr", "cd_prev", "cd_curr"])
    if not (curr_sun and prev_sun):
        return vazio

    sufixos = [d.strftime("%Y%m%d") for d in (prev_sun, curr_sun)]
    raw = raw if raw is not None else bq.forecast_asof_series(
        sufixos, _fc_open_stage_ids(),
        # A faixa Lost é o motivo desta tela pedir o perdido, e a janela do
        # trimestre é o que mantém isso barato (ver bq.forecast_asof_series).
        lost_stage_id=_HS_LOST_STAGE,
        lost_from=q0.isoformat(), lost_to=q1.isoformat(),
        force_refresh=force, pipeline=_pl())
    if not len(raw):
        return vazio

    lv = canon_ae_names(raw).copy()
    lv["amount"] = pd.to_numeric(lv["amount"], errors="coerce").fillna(0.0)
    for c in ("close_date", "po_emission_date"):
        lv[c] = pd.to_datetime(lv[c], errors="coerce")
    # Won conta pela data de VALIDAÇÃO (emissão do PO, fallback close date) — o
    # mesmo critério da aba Forecast, que é o que joga o deal fechado em 1-3/abr
    # e validado em março no Q1, não no Q2.
    lv["valid_date"] = lv["po_emission_date"].fillna(lv["close_date"])
    # O Won do trimestre é RECEITA: decide pela data com a extensão de
    # fechamento (validado em 01–02/10/2026 é Won do Q3, e sai do Q4).
    lv["rev_date"] = _rev_date(lv["valid_date"])

    # Hierarquia + roster/allowlist/líder/região/deal type, igual `_fc_hist_frames`.
    lv = lv.merge(_fc_asof_leader_meta(force=force), on="account_executive", how="left")
    mgr = lv["account_executive"].isin(NON_AE_USERS)
    lv.loc[mgr, "leader"] = lv.loc[mgr, "account_executive"]
    lv.loc[mgr, "region"] = lv.loc[mgr, "region"].fillna("BR")
    lv = _fcw_scope(f, cut_from=q0)(lv)
    if not len(lv):
        return vazio

    lo, hi = pd.Timestamp(q0), pd.Timestamp(q1)
    abertos = _fc_open_stage_ids()
    no_q = lv["close_date"].notna() & (lv["close_date"] >= lo) & (lv["close_date"] <= hi)
    won_q = lv["rev_date"].notna() & (lv["rev_date"] >= lo) & (lv["rev_date"] <= hi)
    banda = lv["forecast_category"].map(_FCW_BAND_BY_RAW).fillna("nf")
    b = pd.Series("out", index=lv.index)
    _ab = lv["stage"].isin(abertos) & no_q
    b[_ab] = banda[_ab]
    b[lv["stage"].eq(_HS_WON_STAGE) & won_q] = "won"
    b[lv["stage"].eq(_HS_LOST_STAGE) & no_q] = "lost"
    lv["b"] = b

    # Duas fotos → dois lados. `drop_duplicates` por (dia, deal) é cinto de
    # segurança: a cópia do BI é uma linha por deal, mas se um dia ela deixar de
    # ser, o pivot silenciosamente somaria o mesmo deal duas vezes.
    lv = lv.drop_duplicates(subset=["dia", "deal_id"])
    ant = lv[lv["dia"] == sufixos[0]].set_index("deal_id")
    atu = lv[lv["dia"] == sufixos[1]].set_index("deal_id")
    ids = ant.index.union(atu.index)

    w = pd.DataFrame(index=ids)
    w["b_prev"] = ant["b"].reindex(ids).fillna("out")
    w["b_curr"] = atu["b"].reindex(ids).fillna("out")
    # Fora nas duas pontas = ruído do funil inteiro, não notícia da semana.
    w = w[~(w["b_prev"].eq("out") & w["b_curr"].eq("out"))]
    ids = w.index
    ant, atu = ant.reindex(ids), atu.reindex(ids)

    w["v_prev"] = ant["amount"].fillna(0.0)
    w["v_curr"] = atu["amount"].fillna(0.0)
    w["fc_prev"] = ant["forecast_category"]
    w["fc_curr"] = atu["forecast_category"]
    w["cd_prev"] = ant["close_date"]
    w["cd_curr"] = atu["close_date"]
    # Identidade e hierarquia: a foto ATUAL manda; quem só existe na anterior
    # (deal apagado do CRM, AE que saiu do escopo) fica com o que ela tinha.
    for col in ("deal_name", "account_executive", "leader", "region", "deal_type"):
        w[col] = atu[col].fillna(ant[col])

    peso = {k: m["w"] for k, m in _FCW_META.items()}
    w["p_prev"] = w["b_prev"].map(peso) * w["v_prev"]
    w["p_curr"] = w["b_curr"].map(peso) * w["v_curr"]
    w["dp"] = w["p_curr"] - w["p_prev"]
    w["mv"] = [_fcw_mv(p, c) for p, c in zip(w["b_prev"], w["b_curr"])]

    # Motivo da entrada e da saída. Só o que o dado sustenta:
    #   · saída "data"  — o deal ESTÁ na foto de agora, em estágio aberto, mas o
    #     close date dele já não cai no trimestre: escorregou;
    #   · saída "funil" — não está na foto medida (mudou pra um estágio fora dos
    #     6 abertos, saiu do pipeline Sales BR ou o registro sumiu). Aqui não dá
    #     pra separar as três: a query só traz os 6 abertos + Won + Lost, então
    #     No Show e deal apagado chegam do mesmo jeito — como ausência;
    #   · saída "ext"   — o deal foi GANHO, mas a receita dele conta no
    #     trimestre anterior pela extensão de fechamento (REALIZED_QUARTER_
    #     EXTENSIONS): não escorregou, foi realizado antes do trimestre medido;
    #   · entrada "novo" — deal criado DEPOIS do domingo anterior (created_date
    #     da base de hoje, que é imutável). O resto entrou por data/estágio.
    pipe = _get_pipeline_deals(force=force)
    criado = dict(zip(pipe["deal_id"].astype(str),
                      pd.to_datetime(pipe["created_date"], errors="coerce")))
    _cr = pd.to_datetime(pd.Series(w.index.map(criado), index=w.index), errors="coerce")
    _novo = _cr.notna() & (_cr > pd.Timestamp(prev_sun))
    _foto = atu["stage"].notna()
    _saiu, _ent = w["mv"].eq("saiu"), w["mv"].eq("entrou")
    _ext = (atu["stage"].eq(_HS_WON_STAGE) & atu["rev_date"].notna()
            & (atu["rev_date"] < lo) & atu["rev_date"].ne(atu["valid_date"])
            ).reindex(w.index).fillna(False).astype(bool)
    w["motivo"] = (pd.Series("", index=w.index)
                   .mask(_saiu & _foto, "data").mask(_saiu & _foto & _ext, "ext")
                   .mask(_saiu & ~_foto, "funil")
                   .mask(_ent & _novo, "novo").mask(_ent & ~_novo, "veio"))

    return w.reset_index().rename(columns={"index": "deal_id"})


def _fcw_levels(w) -> list:
    """Uma linha por faixa: nível nas duas pontas MAIS a decomposição do Δ.

    A conta fecha por construção, faixa por faixa:
        nível_atual − nível_anterior = entrou − saiu + reprecificação
    Sem as três colunas, uma faixa que ficou parada no total mas trocou metade
    dos deals leria como "semana sem novidade" — e é o contrário disso."""
    rows = []
    for k in _FCW_KEYS:
        ip, ic = w["b_prev"].eq(k), w["b_curr"].eq(k)
        fica = ip & ic
        vp = float(w.loc[ip, "v_prev"].sum())
        vc = float(w.loc[ic, "v_curr"].sum())
        rows.append({
            "key": k, **{x: _FCW_META[k][x] for x in ("label", "hint", "color", "ink")},
            "w": _FCW_META[k]["w"],
            "n_prev": int(ip.sum()), "n_curr": int(ic.sum()),
            "v_prev": vp, "v_curr": vc, "delta": vc - vp,
            "pct": ((vc - vp) / vp) if vp else None,
            "p_prev": vp * _FCW_META[k]["w"], "p_curr": vc * _FCW_META[k]["w"],
            "entrou": float(w.loc[ic & ~ip, "v_curr"].sum()),
            "saiu": float(w.loc[ip & ~ic, "v_prev"].sum()),
            "repreco": float((w.loc[fica, "v_curr"] - w.loc[fica, "v_prev"]).sum()),
            "n_entrou": int((ic & ~ip).sum()), "n_saiu": int((ip & ~ic).sum()),
        })
    return rows


def _fcw_bridge(w) -> list:
    """A ponte da projeção: uma linha por movimento, com o quanto ele explica do
    Δ da projeção dos diretores. Fecha em zero por construção — os movimentos
    PARTICIONAM os deals, e Δprojeção é a soma de `dp`.

    `bruto` é o valor que MOVEU, com uma convenção: o amount atual, exceto quando
    o deal saiu do forecast (aí é o anterior, que é o que deixou de contar) e na
    reprecificação (aí é a própria variação). Sem a convenção escrita, "bruto" de
    um deal que mudou de faixa E de valor na mesma semana seria ambíguo."""
    rows = []
    for k in _FCW_MV_ORDER:
        sub = w[w["mv"].eq(k)]
        if k == "repreco":
            sub = sub[sub["v_curr"] != sub["v_prev"]]
            bruto = float((sub["v_curr"] - sub["v_prev"]).sum())
        elif k in ("saiu", "recl"):
            bruto = float(sub["v_prev"].sum())
        else:
            bruto = float(sub["v_curr"].sum())
        if not len(sub):
            continue
        # Entrada e saída ganham o POR QUÊ na própria linha: "saiu R$ 1,09 mi" é
        # uma notícia bem diferente de "escorregou R$ 1,09 mi de trimestre", e a
        # segunda é a que aparece toda virada de quarter. Ver `motivo` em
        # `_fcw_frame` pro que o dado sustenta (e o que não).
        det = []
        if k in ("saiu", "entrou"):
            _lbl = {"data": "escorregaram de trimestre",
                    "ext": "ganhos no fechamento estendido do trimestre anterior",
                    "funil": "saíram dos 6 estágios abertos",
                    "novo": "criados nesta semana",
                    "veio": "vieram de fora do trimestre"}
            for mot, txt in _lbl.items():
                g = sub[sub["motivo"].eq(mot)]
                if len(g):
                    det.append({"txt": txt, "n": int(len(g)),
                                "v": float(g["v_prev" if k == "saiu" else "v_curr"].sum())})
        rows.append({"key": k, **_FCW_MV_META[k], "n": int(len(sub)),
                     # A reprecificação é uma VARIAÇÃO de amount, não um valor
                     # movido: sem sinal a tela escreveria "R$ -19k".
                     "bruto": bruto, "assinado": k == "repreco",
                     "det": det, "dp": float(sub["dp"].sum())})
    return rows


def _fcw_matrix(w) -> dict:
    """Matriz faixa_anterior × faixa_atual, em valor BRUTO.

    O eixo inclui `out` (fora do forecast do trimestre) nas duas pontas, e é ele
    que fecha a matriz: sem a coluna, o deal que escorregou pra outubro
    simplesmente desapareceria da linha da faixa dele; sem a linha, o que entrou
    apareceria do nada. A célula out×out não existe (foi descartada no frame).

    A célula é medida no valor de ORIGEM (o amount do domingo anterior), e a
    linha `out` — que não tem origem — no valor de agora. Essa escolha é o que faz
    a linha somar EXATO: "dos R$ 2,38 mi que estavam em Not forecasted, R$ 1,16
    mi ficaram, R$ 284 mil subiram pra 25% e R$ 827 mil saíram do trimestre". Se a
    célula fosse o valor atual, a linha não fecharia com o nível anterior sempre
    que o amount mudasse na mesma semana.

    O rodapé, por isso, NÃO é a soma das colunas: é o nível de cada faixa AGORA
    (de `_fcw_levels`). A diferença entre uma coisa e outra é exatamente a
    reprecificação, que o painel de faixas já mostra em coluna própria.

    A intensidade de cor ignora a DIAGONAL no cálculo da escala: "ficou na mesma
    faixa" é quase sempre a maior célula da matriz (no Q3/2026, R$ 9,6 mi de Lost
    acumulado contra R$ 614 mil da maior migração real), e incluí-la deixava todo
    movimento de verdade na mesma tinta pálida."""
    eixo = list(_FCW_KEYS) + ["out"]
    span, grid, tot_col = 0.0, [], {k: 0.0 for k in eixo}
    for bp in eixo:
        linha, tot_row = [], 0.0
        for bc in eixo:
            sub = w[w["b_prev"].eq(bp) & w["b_curr"].eq(bc)]
            v = float((sub["v_curr"] if bp == "out" else sub["v_prev"]).sum())
            mv = _fcw_mv(bp, bc)
            linha.append({"de": bp, "para": bc, "n": int(len(sub)), "v": v,
                          "mv": mv, "color": _FCW_MV_META[mv]["color"],
                          "label": _FCW_MV_META[mv]["label"],
                          "diag": bp == bc, "vazio": bp == "out" and bc == "out"})
            tot_row += v
            tot_col[bc] += v
            if bp != bc:
                span = max(span, abs(v))
        grid.append({"key": bp, "label": _FCW_META[bp]["label"],
                     "ink": _FCW_META[bp]["ink"], "cells": linha, "total": tot_row,
                     # A linha `out` não tem nível anterior (o deal estava fora):
                     # o total dela é o que ENTROU, medido agora.
                     "e_nivel": bp != "out"})
    niveis = {r["key"]: r for r in _fcw_levels(w)}
    return {
        "eixo": [{"key": k, **{x: _FCW_META[k][x] for x in ("label", "color", "ink")}}
                 for k in eixo],
        "rows": grid, "span": span or 1.0,
        # Rodapé = nível de agora nas faixas; na coluna "Fora" o nível não existe,
        # então ali vai a soma da coluna mesmo (o que saiu do trimestre).
        "rodape": [(niveis[k]["v_curr"] if k in niveis else tot_col[k]) for k in eixo],
        "saiu_total": tot_col["out"],
    }


# Ordem do espectro, da pior notícia pra melhor. É ela que empilha a barra da
# esquerda pra direita: quem lê de fora entende "piorou → ficou → melhorou" sem
# consultar legenda, do mesmo jeito que o PoP se lê pelo "moveu pra Late".
_FCW_DEST_ORDER = ("perdido", "saiu", "desceu", "repreco", "subiu", "ganho")
# Como cada destino entra no resumo de 4 colunas. Won é coluna PRÓPRIA e não
# entra em "subiu": virar receita não é ganhar confiança, é acabar a aposta — e
# é o número que a liderança pergunta primeiro. Lost é piora, junto de quem
# desceu de faixa e de quem escorregou do trimestre.
_FCW_DEST_GROUP = {"perdido": "piorou", "saiu": "piorou", "desceu": "piorou",
                   "repreco": "ficou", "subiu": "subiu", "ganho": "won"}
_FCW_DEST_RESUMO = ("subiu", "won", "ficou", "piorou")
# `repreco` significa outra coisa AQUI: na ponte é a variação de valor de quem
# não mudou de faixa; nesta barra é o volume que PERMANECEU na faixa (mudando de
# tamanho ou não). Rótulo próprio, senão a legenda diz "Reprecificação" pro
# bloco que a coluna de resumo chama de "Ficou".
_FCW_DEST_LABEL = {"repreco": "Ficou na faixa"}


def _fcw_destino(w) -> dict:
    """Destino do volume de CADA faixa: das cinco faixas do parâmetro, quanto
    ficou, quanto evoluiu, quanto piorou e por qual caminho.

    É a matriz de migração relida como pergunta de negócio. A matriz tem a mesma
    verdade, mas espalhada em 64 células onde "melhorou" é "acima da diagonal" —
    ninguém descobre ali que o 50% está sangrando pra baixo. Aqui cada faixa é
    uma barra, e as colunas de destino viram os seis movimentos possíveis.

    A barra é medida no valor de ORIGEM (o amount do domingo anterior), a mesma
    escolha da matriz e pelo mesmo motivo: é o que faz a barra somar EXATO o
    nível que a faixa tinha. Se cada pedaço fosse medido agora, a barra não
    fecharia sempre que o amount mudasse na mesma semana — e a reprecificação
    fica registrada à parte, em `repreco_dp`.

    Won e Lost NÃO são barras: como faixa de origem, os dois são acumulado do
    trimestre (no Q3/2026, R$ 11,2 mi de Lost contra R$ 1,9 mi na maior faixa) e
    esmagariam as cinco na mesma escala. Eles aparecem onde significam alguma
    coisa nesta pergunta — como DESTINO, dentro da barra da faixa de onde o deal
    saiu: é assim que "o Commit converteu R$ 123 mil em Won esta semana" vira
    legível. `out` (entrou no forecast) também fica fora das barras: esses deals
    não tinham faixa anterior pra decompor, e enfiá-los numa das cinco quebraria
    a invariante que faz a barra fechar. Vão numa barra própria."""
    bands, span = [], 0.0
    for b in _FCW_BANDS:
        sub_b = w[w["b_prev"].eq(b)]
        tot = float(sub_b["v_prev"].sum())
        span = max(span, tot)
        segs, resumo = [], {k: {"v": 0.0, "n": 0} for k in _FCW_DEST_RESUMO}
        for mv in _FCW_DEST_ORDER:
            s2 = sub_b[sub_b["mv"].eq(mv)]
            v, n = float(s2["v_prev"].sum()), int(len(s2))
            g = resumo[_FCW_DEST_GROUP[mv]]
            g["v"] += v
            g["n"] += n
            if not v:
                continue
            segs.append({"key": mv, "label": _FCW_DEST_LABEL.get(
                             mv, _FCW_MV_META[mv]["label"]),
                         "color": _FCW_MV_META[mv]["color"], "v": v, "n": n,
                         "pct": v / tot if tot else 0.0,
                         # `ficou` é o único pedaço que NÃO ganha cor de
                         # movimento: ele não se moveu. Fica na cor da própria
                         # faixa, em transparência — o bloco que permaneceu
                         # mantém a identidade dela, e verde/vermelho ficam
                         # reservados pro que de fato andou.
                         "e_ficou": mv == "repreco"})
        for g in resumo.values():
            g["pct"] = (g["v"] / tot) if tot else 0.0
        fic = sub_b[sub_b["mv"].eq("repreco")]
        bands.append({
            "key": b, "total": tot, "n": int(len(sub_b)),
            **{x: _FCW_META[b][x] for x in ("label", "hint", "color", "ink")},
            "segs": segs, "resumo": resumo,
            # Reprecificação DENTRO do que ficou: o deal não mudou de faixa, mas
            # mudou de tamanho. Sem esta linha o "ficou" leria como "nada
            # aconteceu", e num trimestre é justamente onde o valor escorrega.
            "repreco_dp": float((fic["v_curr"] - fic["v_prev"]).sum()),
        })
    # Quem veio de FORA do forecast do trimestre. Medido AGORA (v_curr): estes
    # deals não têm valor anterior — mesma regra da linha `out` da matriz.
    #
    # E é decomposto, não um número só: na semana de 06/09/2026, dos 160 deals
    # que vieram de fora, 130 (R$ 815 mil) foram marcados Lost na MESMA semana e
    # só 29 (R$ 435 mil) entraram numa faixa de verdade. Somar os dois como
    # "entrou no forecast" chamaria de entrada o que foi enterro.
    ent = w[w["b_prev"].eq("out")]
    ent_segs = []
    for mv in ("perdido", "entrou", "ganho"):
        s2 = ent[ent["mv"].eq(mv)]
        if not len(s2):
            continue
        ent_segs.append({"key": mv, "n": int(len(s2)),
                         "v": float(s2["v_curr"].sum()),
                         "label": {"entrou": "Entrou numa faixa",
                                   "ganho": "Direto pra Won",
                                   "perdido": "Direto pra Lost"}[mv],
                         "color": _FCW_MV_META[mv]["color"]})
    return {
        "bands": bands, "span": span or 1.0,
        "entrou": {"v": float(ent["v_curr"].sum()), "n": int(len(ent)),
                   "segs": ent_segs,
                   # Escala PRÓPRIA: este bloco não divide um volume anterior,
                   # então medi-lo na régua das faixas deixaria as três barras do
                   # tamanho de um ponto e sem contraste entre si.
                   "span": max([x["v"] for x in ent_segs], default=1.0) or 1.0,
                   "color": _FCW_MV_META["entrou"]["color"]},
        "legenda": [{"key": k,
                     "label": _FCW_DEST_LABEL.get(k, _FCW_MV_META[k]["label"]),
                     "color": _FCW_MV_META[k]["color"], "e_ficou": k == "repreco"}
                    for k in _FCW_DEST_ORDER],
        # Listas de `mv` prontas pro drill: o template não monta string de
        # parâmetro na mão, senão a definição de "piorou" passaria a existir em
        # dois lugares e um dia divergiriam.
        "mv_todos": ",".join(_FCW_DEST_ORDER),
        "mv_grupo": {g: ",".join(k for k in _FCW_DEST_ORDER
                                 if _FCW_DEST_GROUP[k] == g)
                     for g in _FCW_DEST_RESUMO},
        "resumo_meta": [("subiu", "Subiu", "#047857"), ("won", "Won", "#065F46"),
                        ("ficou", "Ficou", None), ("piorou", "Piorou", "#B91C1C")],
    }


def _fcw_people(w, grain, leader_filter) -> list:
    """Uma linha por AE (ou por líder, com o diretor englobando o gerente): a
    projeção nas duas pontas e o movimento que explica a diferença.

    No grain de líder a soma das linhas NÃO fecha com o time, de propósito — cada
    deal conta no gerente e no diretor, a mesma convenção das outras telas de
    Leaders."""
    base = (_explode_leaders(w, leader_filter) if grain == "leader"
            else drop_non_ae_people(w))
    if not len(base):
        return []
    rows = []
    for nome, g in base.groupby("account_executive"):
        r = {"name": nome, "short": short_name(nome),
             "leader": (g["leader"].dropna().iloc[0] if g["leader"].notna().any() else None),
             "p_prev": float(g["p_prev"].sum()), "p_curr": float(g["p_curr"].sum()),
             "dp": float(g["dp"].sum())}
        for k in _FCW_MV_ORDER:
            sub = g[g["mv"].eq(k)]
            r["mv_" + k] = float(sub["dp"].sum())
            r["n_" + k] = int(len(sub)) if k != "repreco" else int(
                (sub["v_curr"] != sub["v_prev"]).sum())
        # Sem projeção nas duas pontas E sem movimento: a linha não diria nada
        # (é o AE que não tem nenhum deal no trimestre).
        if r["p_prev"] or r["p_curr"] or r["dp"]:
            rows.append(r)
    rows.sort(key=lambda r: r["dp"], reverse=True)
    return rows


def _fcw_cover(w, side: str) -> dict:
    """Cobertura da faixa POR VALOR num dos domingos: quanto do pipeline aberto
    do trimestre está numa das 4 faixas previstas, quanto está em Not forecasted
    explícito e quanto está sem categoria nenhuma.

    Mesma leitura (e mesmo motivo) do Histórico do Forecast: este peso é o
    julgamento do dono do deal, então comparar duas semanas só faz sentido
    sabendo se a categoria estava preenchida nas duas. Aqui `omit` e `sem` ficam
    separados porque "o AE disse que não entra" é informação e "ninguém
    preencheu" é a falta dela — e as duas caem na MESMA faixa `nf`."""
    b, v = "b_" + side, "v_" + side
    aberto = w[w[b].isin(_FCW_BANDS)]
    tot = float(aberto[v].sum())
    if not tot:
        return {"tot": 0.0, "prev": None, "omit": None, "sem": None}
    fc = aberto["fc_" + side]
    vazio = fc.isna() | (fc.astype(str).str.strip() == "")
    return {
        "tot": tot,
        "prev": 100.0 * float(aberto.loc[~aberto[b].eq("nf"), v].sum()) / tot,
        "omit": 100.0 * float(aberto.loc[aberto[b].eq("nf") & ~vazio, v].sum()) / tot,
        "sem": 100.0 * float(aberto.loc[vazio, v].sum()) / tot,
    }


def _fcw_done_hoje(f, q0: date, q1: date, sun: date, force=False) -> float:
    """Won acumulado até `sun` lido da base de HOJE — que é como o Histórico do
    Forecast monta o Done (`_fc_done_cumulative`: "um deal ganho não deixa de ter
    sido ganho, e a data de validação não muda depois").

    Esta tela usa o Won da FOTO do domingo, que é outra coisa: se um PO foi
    lançado na terça com data de validação de sexta, a foto do domingo anterior
    não o tinha e a base de hoje tem. As duas leituras estão certas — aqui a
    pergunta é "o que a semana mudou", e reescrever o passado com informação de
    hoje apagaria exatamente isso. Medido em 06/09/2026: R$ 988 mil na foto
    contra R$ 1,006 mi na base de hoje, 0,6% da projeção.

    Existe só pra a tela DIZER esse número ao lado do dela. Sem isso as duas abas
    se contradizem em silêncio, e a pessoa fica sem saber qual acreditar."""
    d = _get_forecast_deals(force=force)
    lo, hi = pd.Timestamp(q0), pd.Timestamp(q1)
    # Mesma régua do Won do `_fcw_frame`: data da receita, com a extensão.
    d = d[d["is_won"] & d["rev_date"].notna()
          & (d["rev_date"] >= lo) & (d["rev_date"] <= hi)]
    d = _fcw_scope(f, cut_from=q0)(d)
    return float(d[d["rev_date"] <= pd.Timestamp(sun)]["amount"].sum())


def _render_forecast_wow(grain="ae"):
    """WoW do Forecast: a migração do pipeline entre as faixas de probabilidade
    entre dois domingos consecutivos."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    q0, q1 = _fc_hist_quarter(f)
    pares, curr_sun, prev_sun, faltando = _fcw_pairs(q0, q1, force=force)
    w = _fcw_frame(q0, q1, curr_sun, prev_sun, f, force=force)

    niveis = _fcw_levels(w) if len(w) else []
    por_faixa = {r["key"]: r for r in niveis}
    ponte = _fcw_bridge(w) if len(w) else []
    # Projeção dos diretores = Won do trimestre + aberto ponderado pela faixa.
    # Sai do MESMO frame das duas pontas, então Δ = Σ dp exatamente — não é a
    # subtração de dois números vindos de fontes diferentes.
    proj_prev = float(w["p_prev"].sum()) if len(w) else 0.0
    proj_curr = float(w["p_curr"].sum()) if len(w) else 0.0

    def _soma(keys, side):
        return sum(por_faixa[k]["v_" + side] for k in keys if k in por_faixa)

    previstas = tuple(k for k in _FCW_BANDS if k != "nf")

    # Meta: a MESMA das outras telas de forecast (Comp. Target dos líderes no
    # recorte), e SÓ no trimestre corrente — LEADER_BOARD_TARGET é a planilha do
    # quarter em curso, e desenhá-la sobre um trimestre passado compararia o
    # forecast de abril com a meta de julho.
    # "Corrente" pela data da RECEITA: durante a extensão de fechamento o
    # trimestre que ainda acumula realizado é o anterior (02/10/2026 → Q3).
    meta_ok = (q0 == _quarter_start_of(_rev_date(_today()))
               and q1 == _quarter_end_of(q0))       # meta é de UM trimestre
    lideres = sorted({L for L in w["leader"].dropna().unique()}) if len(w) else []
    meta = _leader_meta_total(lideres) if meta_ok else 0.0

    def _drill(**kw):
        return url_for("forecast_wow_deals", grain=grain,
                       par=(curr_sun.strftime("%Y%m%d") if curr_sun else None),
                       date_from=f.get("date_from"), date_to=f.get("date_to"),
                       dtype=f.get("dtype") or None,
                       leader=(f.get("leader") if not kw.get("name") else None),
                       region=(f.get("region") if not kw.get("name") else None),
                       **kw)

    def _chip_url(**kw):
        args = {k: v for k, v in request.args.items() if k not in kw and v}
        return request.path + "?" + urllib.parse.urlencode({**args, **kw})

    return render_template(
        "forecast_wow.html", ctx=ctx, grain=grain,
        q_label=_fc_hist_label(q0, q1),
        q_range=f"{q0.strftime('%d/%m/%Y')} → {q1.strftime('%d/%m/%Y')}",
        curr_label=(_fmt_date_short(curr_sun) if curr_sun else "—"),
        prev_label=(_fmt_date_short(prev_sun) if prev_sun else "—"),
        # Chip = o domingo que FECHA a semana (a janela inteira cabe no title):
        # 13 chips de "24 ago → 31 ago" não cabem numa linha, e a janela do par
        # escolhido já está escrita no subtítulo da página.
        pares=[{"key": c.strftime("%Y%m%d"), "label": _fmt_date_curta(c),
                "title": f"semana de {_fmt_date_short(p)} a {_fmt_date_short(c)}"}
               for c, p in pares],
        par_sel=(curr_sun.strftime("%Y%m%d") if curr_sun else ""),
        faltando=faltando, sem_par=not pares, sem_dado=not len(w),
        niveis=niveis, por_faixa=por_faixa, ponte=ponte,
        destino=(_fcw_destino(w) if len(w) else None),
        matriz=(_fcw_matrix(w) if len(w) else None),
        people=(_fcw_people(w, grain, f.get("leader")) if len(w) else []),
        proj_prev=proj_prev, proj_curr=proj_curr, proj_delta=proj_curr - proj_prev,
        meta=meta, meta_ok=meta_ok,
        att_prev=((proj_prev / meta) if meta else None),
        att_curr=((proj_curr / meta) if meta else None),
        prev_bruto_curr=_soma(previstas, "curr"), prev_bruto_prev=_soma(previstas, "prev"),
        cover_curr=(_fcw_cover(w, "curr") if len(w) else None),
        cover_prev=(_fcw_cover(w, "prev") if len(w) else None),
        won_hoje=(_fcw_done_hoje(f, q0, q1, curr_sun, force=force)
                  if (curr_sun and len(w)) else None),
        n_deals=int(len(w)),
        bands=[por_faixa[k] for k in _FCW_BANDS if k in por_faixa],
        mv_meta=_FCW_MV_META, mv_order=_FCW_MV_ORDER,
        drill=_drill, chip_url=_chip_url,
    )


@app.route("/forecast/wow")
def forecast_wow():
    return _render_forecast_wow("ae")


@app.route("/leaders/forecast/wow")
def leaders_forecast_wow():
    return _render_forecast_wow("leader")


@app.route("/forecast/wow/deals")
def forecast_wow_deals():
    """Drill do WoW do Forecast. Aceita três recortes, e SÓ eles:
      · `mv`         — um movimento da ponte (ou vários, separados por vírgula);
      · `de` + `para`— uma célula da matriz de migração;
      · `faixa` + `parte` — uma faixa e a parte do Δ dela (entrou/saiu/repreco).

    O par de domingos vem em `par` (o domingo ATUAL) e é RECONSTRUÍDO pela mesma
    `_fcw_pairs` da página, não aceito cru: assim uma URL antiga não consegue
    pedir uma janela que a tela não desenharia (par sem foto nas duas pontas)."""
    f = get_filters()
    q0, q1 = _fc_hist_quarter(f)
    pares, curr_sun, prev_sun, _falta = _fcw_pairs(q0, q1)
    if not pares:
        return jsonify(_empty_drill(""))

    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))

    w = _fcw_frame(q0, q1, curr_sun, prev_sun, f)
    return _fcw_drill_json(w, name, grain, prev_sun, curr_sun, "Movimento da semana")


def _fcw_drill_json(w, name, grain, t_prev, t_curr, sufixo_padrao):
    """O corpo do drill do WoW, separado da escolha das pontas: o DoD usa o mesmo
    recorte (mv, de+para, faixa+parte) sobre o frame dos DOIS DIAS dele."""
    if not len(w):
        return jsonify(_empty_drill(name))
    w = _entity_filter(w, name, grain, request.args.get("leader"),
                       request.args.get("region"))

    de, para = (request.args.get("de") or ""), (request.args.get("para") or "")
    faixa, parte = (request.args.get("faixa") or ""), (request.args.get("parte") or "")
    mvs = [x.strip() for x in (request.args.get("mv") or "").split(",") if x.strip()]
    mvs = [m for m in mvs if m in _FCW_MV_META]
    # `mexeu` = o deal fez ALGO na semana. A reprecificação de valor zero (o caso
    # do Lost acumulado, 1,6 mil deals parados) não é movimento, e sem esse corte
    # o drill do KPI devolvia o trimestre inteiro em vez da semana.
    mexeu = ~w["mv"].eq("repreco") | (w["v_curr"] != w["v_prev"])
    titulo, sufixo = (name or "Forecast do trimestre — total"), sufixo_padrao
    # `bruto` medido na ponta que o recorte descreve: o que SAIU só existe no
    # valor anterior (agora ele vale 0 na faixa), o resto se mede agora.
    lado = "v_curr"
    # `parte=nivel` sem faixa = a COMPOSIÇÃO da projeção atual (o número grande
    # do card), deal a deal com o quanto cada um soma — não o movimento da semana.
    composicao = parte == "nivel" and not faixa
    if composicao:
        w = w[w["p_curr"] != 0]
        sufixo = "composição da projeção atual"
    elif de and para:
        w = w[w["b_prev"].eq(de) & w["b_curr"].eq(para)]
        sufixo = f"{_FCW_META.get(de, {}).get('label', de)} → {_FCW_META.get(para, {}).get('label', para)}"
        # A matriz mede a célula no valor de ORIGEM (só a linha `out` não tem
        # origem) — o subtítulo do drill tem que dar o MESMO número da célula
        # clicada, senão a modal parece contradizer a tabela.
        lado = "v_curr" if de == "out" else "v_prev"
    elif de and mvs:
        # Um pedaço da barra do "Destino de cada faixa": a faixa de ORIGEM mais a
        # classe de movimento. Sem o `mexeu` de propósito — o bloco "ficou" da
        # barra inclui quem não mudou nada, e o drill tem que devolver a mesma
        # população que a barra desenhou, não um subconjunto dela.
        w = w[w["b_prev"].eq(de) & w["mv"].isin(mvs)]
        sufixo = (f"{_FCW_META.get(de, {}).get('label', de)} · "
                  f"{_FCW_MV_META[mvs[0]]['label'] if len(mvs) == 1 else f'{len(mvs)} movimentos'}")
        # A barra é medida na ORIGEM — menos a de `out`, que não tem origem.
        lado = "v_curr" if de == "out" else "v_prev"
    elif faixa in _FCW_KEYS and parte:
        ip, ic = w["b_prev"].eq(faixa), w["b_curr"].eq(faixa)
        w = {"entrou": w[ic & ~ip], "saiu": w[ip & ~ic],
             "repreco": w[ip & ic & (w["v_curr"] != w["v_prev"])],
             "nivel": w[ic]}.get(parte, w.iloc[0:0])
        _p = {"entrou": "entrou na faixa", "saiu": "saiu da faixa",
              "repreco": "reprecificado", "nivel": "nível atual"}.get(parte, parte)
        sufixo = f"{_FCW_META[faixa]['label']} · {_p}"
        lado = "v_prev" if parte == "saiu" else "v_curr"
    elif mvs:
        w = w[w["mv"].isin(mvs) & mexeu]
        sufixo = (_FCW_MV_META[mvs[0]]["label"] if len(mvs) == 1
                  else f"{len(mvs)} movimentos")
        lado = "v_prev" if set(mvs) <= {"saiu", "recl"} else "v_curr"
    else:
        w = w[mexeu]

    _mot = {"data": "escorregou de trimestre",
            "ext": "ganho no fechamento estendido do trimestre anterior",
            "funil": "saiu dos 6 estágios abertos",
            "novo": "deal novo", "veio": "veio de fora do trimestre"}
    rows = []
    for r in w.sort_values("dp").itertuples():
        rows.append({
            "deal_name": r.deal_name,
            "deal_url": _hs_deal_url(r.deal_id),
            "ae": r.account_executive,
            "leader": r.leader,
            "movimento": _FCW_MV_META[r.mv]["label"],
            "faixas": (f"{_FCW_META[r.b_prev]['label']} → {_FCW_META[r.b_curr]['label']}"
                       if r.b_prev != r.b_curr else _FCW_META[r.b_curr]["label"]),
            "motivo": _mot.get(r.motivo, "—"),
            "valores": f"{fmt_brl(r.v_prev)} → {fmt_brl(r.v_curr)}",
            "dp": float(r.p_curr if composicao else r.dp),
        })
    _dp = sum(x["dp"] for x in rows)
    _n = len(rows)
    # Na reprecificação o "bruto" é a VARIAÇÃO do amount (é o que a linha da
    # ponte mostra), não o valor dos deals: eles não mudaram de faixa.
    _so_repreco = (parte == "repreco") or (mvs == ["repreco"])
    _bruto = 0.0
    if len(w):
        _bruto = float((w["v_curr"] - w["v_prev"]).sum() if _so_repreco
                       else w[lado].sum())
    _b_txt = ((('+' if _bruto >= 0 else '−') + fmt_money(abs(_bruto))) if _so_repreco
              else fmt_money(_bruto))
    if composicao:
        return jsonify({
            "title": f"{titulo} · {sufixo}",
            "subtitle": (f"foto de {_fmt_date_short(t_curr)} · {_n} deal{'s' if _n != 1 else ''} · "
                         f"bruto {fmt_money(_bruto)} · projeção {fmt_money(_dp)}"),
            "columns": _with_ae_col([
                {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
                {"key": "faixas", "label": "Faixa (de → para)"},
                {"key": "valores", "label": "Amount (de → para)"},
                {"key": "dp", "label": "Projeção", "type": "money"},
            ], "leader" if (grain == "leader" or not name) else "ae"),
            "rows": rows,
        })
    return jsonify({
        "title": f"{titulo} · {sufixo}",
        "subtitle": (f"{_fmt_date_short(t_prev)} → {_fmt_date_short(t_curr)} · "
                     f"{_n} deal{'s' if _n != 1 else ''} · bruto {_b_txt} · "
                     f"Δ projeção {'+' if _dp >= 0 else '−'}{fmt_money(abs(_dp))}"),
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "movimento", "label": "Movimento"},
            {"key": "faixas", "label": "Faixa (de → para)"},
            {"key": "motivo", "label": "Por quê"},
            {"key": "valores", "label": "Amount (de → para)"},
            {"key": "dp", "label": "Δ projeção", "type": "money_signed"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


# ===========================================================================
# DoD do Forecast (só Leaders) — o "Destino de cada faixa" do WoW, dia a dia
# ===========================================================================
# Mesma pergunta do WoW ("o que o volume de cada faixa virou"), com pontas de
# MEIA-NOITE: a foto de 00h de hoje contra a de 00h de ontem, ou seja, o dia de
# ontem inteiro. Toda a conta (faixa, peso, movimento, barra) é a do WoW —
# `_fcw_frame` + `_fcw_destino` —, só as pontas mudam de lugar.
#
# As pontas vêm de `pepeuhacker.forecast_snapshots_daily`, que o próprio app
# grava logo depois da virada (`_snapshot_forecast_midnight`). A cópia do BI não
# serve de ponta exata: ela é gravada às ~17h16 BRT. Mas ela é a MELHOR
# aproximação disponível pra um dia que não tem foto própria (a série começou em
# 24/09/2026, e um deploy ou queda pode furar uma virada): a cópia do BI da
# VÉSPERA é o último estado conhecido antes daquela meia-noite, faltando só as
# ~7h da noite. Quando uma ponta é aproximada, a tela diz qual e por quê.
# ===========================================================================

# Quantos dias pra trás o chip oferece. Duas semanas: o suficiente pra rever a
# segunda-feira da semana passada, e sem virar uma fileira de 90 botões.
_FCD_DIAS = 14
# Captura depois disto não é "a meia-noite": é a manhã. Melhor furar o dia (e
# a tela cair na aproximação do BI, que ela anuncia) do que gravar uma foto das
# 9h com rótulo de 00h.
_FCD_CAPTURA_MAX_ATRASO = timedelta(hours=2)


def _fcd_brt(x):
    """Timestamp do BigQuery → BRT. O parquet do cache às vezes devolve sem fuso
    (e aí ele é UTC, que é como o BigQuery grava)."""
    if x is None or pd.isna(x):
        return None
    t = pd.Timestamp(x)
    return (t.tz_localize("UTC") if t.tzinfo is None else t).tz_convert(BRT)


def _fcd_sides(force=False, dias=None) -> dict:
    """{YYYYMMDD: ponta} — pra cada dia recente, de onde sai a foto da meia-noite
    dele: a própria (`meia-noite`) ou a cópia do BI da véspera (`bi`).

    `dias` = quais meias-noites procurar; padrão, as dos últimos `_FCD_DIAS`
    dias (o DoD). O Pipeline WoW passa só as segundas-feiras."""
    own = bq.forecast_daily_snapshots(pipeline=_pl(), force_refresh=force)
    own = ({r.dia: r for r in own.itertuples()} if len(own) else {})
    disp = bq.forecast_asof_snapshots(force_refresh=force)
    bi = set(disp["dia"]) if len(disp) else set()
    hoje = _today()
    if dias is None:
        dias = [hoje - timedelta(days=i) for i in range(_FCD_DIAS + 1)]
    bi = _fcd_bi_saudaveis(bi, hoje, force=force,
                           janela={(d - timedelta(days=1)).strftime("%Y%m%d")
                                   for d in dias})
    out = {}
    for d in dias:
        k = d.strftime("%Y%m%d")
        if k in own:
            r = own[k]
            out[k] = {"dia": d, "key": k, "fonte": "meia-noite",
                      "captured": _fcd_brt(r.captured_at),
                      "source": _fcd_brt(r.source_at)}
            continue
        kb = (d - timedelta(days=1)).strftime("%Y%m%d")
        if kb in bi:
            out[k] = {"dia": d, "key": k, "fonte": "bi", "sufixo": kb}
    return out


# Uma cópia do BI só serve de ponta se tiver pelo menos esta fração dos deals
# com AE da mediana da janela. O Sales BR cresce ~20 deals por dia; uma cópia
# que perde 10% de um dia pro outro não é crescimento, é carga quebrada.
_FCD_BI_MIN_FRAC = 0.9


def _fcd_bi_saudaveis(bi: set, hoje: date, force=False, janela=None) -> set:
    """Tira da lista as cópias do BI da janela do DoD que vieram quebradas (ver
    `bq.forecast_asof_ae_counts`). Dia que perde a cópia volta a não ter ponta,
    e os pares que tocam nele somem do chip — melhor do que um par em que o
    funil inteiro "sai" num dia. Se a checagem falhar, segue sem ela.
    `janela` = sufixos a checar; padrão, os dos últimos `_FCD_DIAS` dias."""
    if janela is None:
        janela = {(hoje - timedelta(days=i)).strftime("%Y%m%d")
                  for i in range(1, _FCD_DIAS + 2)}
    janela = set(janela) & bi
    try:
        cnt = bq.forecast_asof_ae_counts(sorted(janela), pipeline=_pl(),
                                         force_refresh=force)
    except Exception as e:  # noqa: BLE001 — sanidade é best-effort
        print(f"pepeuhacker · checagem das cópias do BI falhou ({e})", flush=True)
        return bi
    if not len(cnt):
        return bi
    n = dict(zip(cnt["sufixo"].astype(str), cnt["n_ae"].astype(float)))
    ref = float(pd.Series(list(n.values())).median())
    ruins = {k for k in janela if n.get(k, 0.0) < _FCD_BI_MIN_FRAC * ref}
    return bi - ruins


def _fcd_pairs(force=False):
    """(pares, curr, prev) — pares de dias CONSECUTIVOS com ponta nos dois, e o
    escolhido pelo chip (`?dia=YYYYMMDD`, a meia-noite ATUAL do par). Padrão: o
    mais recente, que é hoje assim que a foto das 00h existir."""
    sides = _fcd_sides(force=force)
    pares = []
    for k, c in sorted(sides.items(), reverse=True):
        kp = (c["dia"] - timedelta(days=1)).strftime("%Y%m%d")
        if kp in sides:
            pares.append((c, sides[kp]))
    if not pares:
        return [], None, None
    sel = (request.args.get("dia") or "").strip()
    curr, prev = next(((c, p) for c, p in pares if c["key"] == sel), pares[0])
    return pares, curr, prev


def _fcd_raw(sides, q0: date, q1: date, force=False) -> pd.DataFrame:
    """As duas pontas no shape de `bq.forecast_asof_series`, com `dia` = o dia
    da MEIA-NOITE (não o sufixo do BI, que é a véspera)."""
    partes = []
    for sd in sides:
        if sd["fonte"] == "meia-noite":
            x = bq.forecast_daily_series([sd["key"]], pipeline=_pl(),
                                         lost_from=q0.isoformat(),
                                         lost_to=q1.isoformat(), force_refresh=force)
        else:
            x = bq.forecast_asof_series(
                [sd["sufixo"]], _fc_open_stage_ids(), lost_stage_id=_HS_LOST_STAGE,
                lost_from=q0.isoformat(), lost_to=q1.isoformat(),
                force_refresh=force, pipeline=_pl())
        x = x.copy()
        x["dia"] = sd["key"]
        partes.append(x)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def _fcd_frame(q0, q1, curr, prev, f, force=False) -> pd.DataFrame:
    if not (curr and prev):
        return _fcw_frame(q0, q1, None, None, f)
    raw = _fcd_raw([prev, curr], q0, q1, force=force)
    return _fcw_frame(q0, q1, curr["dia"], prev["dia"], f, force=force, raw=raw)


def _fcd_side_txt(sd) -> str:
    """Como a tela descreve uma ponta — sempre dizendo de quando é o DADO."""
    if sd["fonte"] == "meia-noite":
        t = f"foto de {sd['captured']:%H:%M} BRT"
        if sd["source"] is not None:
            t += f", dado do HubSpot até {sd['source']:%d/%m %H:%M}"
        return t
    b = datetime.strptime(sd["sufixo"], "%Y%m%d").date()
    return (f"aproximada: cópia do BI de {_fmt_date_curta(b)} ~17h "
            f"(sem foto própria da meia-noite)")


@app.route("/leaders/forecast/dod")
def leaders_forecast_dod():
    """DoD do Forecast: o Destino de cada faixa entre a meia-noite de um dia e a
    do anterior. Só no Leaders, de propósito — é leitura de gestão do dia."""
    grain = "leader"
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    q0, q1 = _fc_hist_quarter(f)
    pares, curr, prev = _fcd_pairs(force=force)
    w = _fcd_frame(q0, q1, curr, prev, f, force=force)

    def _lbl(sd):
        return f"00h de {_fmt_date_short(sd['dia'])}" if sd else "—"

    def _drill(**kw):
        return url_for("leaders_forecast_dod_deals", grain=grain,
                       dia=(curr["key"] if curr else None),
                       date_from=f.get("date_from"), date_to=f.get("date_to"),
                       dtype=f.get("dtype") or None,
                       leader=(f.get("leader") if not kw.get("name") else None),
                       region=(f.get("region") if not kw.get("name") else None),
                       **kw)

    def _chip_url(**kw):
        args = {k: v for k, v in request.args.items() if k not in kw and v}
        return request.path + "?" + urllib.parse.urlencode({**args, **kw})

    return render_template(
        "forecast_dod.html", ctx=ctx, grain=grain,
        q_label=_fc_hist_label(q0, q1),
        q_range=f"{q0.strftime('%d/%m/%Y')} → {q1.strftime('%d/%m/%Y')}",
        curr_label=_lbl(curr), prev_label=_lbl(prev),
        # O dia que o par MEDE é o da ponta anterior: 00h de 22 → 00h de 23 é o
        # dia 22 inteiro. O chip mostra esse dia, que é o que a pessoa procura.
        dia_medido=(_fmt_date_short(prev["dia"]) if prev else "—"),
        pares=[{"key": c["key"], "label": _fmt_date_curta(p["dia"]),
                "aprox": "bi" in (c["fonte"], p["fonte"]),
                "title": f"{_lbl(p)} → {_lbl(c)}"} for c, p in pares],
        dia_sel=(curr["key"] if curr else ""),
        curr_txt=(_fcd_side_txt(curr) if curr else ""),
        prev_txt=(_fcd_side_txt(prev) if prev else ""),
        aprox=bool(curr and "bi" in (curr["fonte"], prev["fonte"])),
        sem_par=not pares, sem_dado=not len(w),
        destino=(_fcw_destino(w) if len(w) else None),
        drill=_drill, chip_url=_chip_url,
    )


@app.route("/leaders/forecast/dod/deals")
def leaders_forecast_dod_deals():
    """Drill do DoD — o mesmo contrato do drill do WoW, com o par de dias
    reconstruído por `_fcd_pairs` (uma URL velha não pede ponta inexistente)."""
    f = get_filters()
    q0, q1 = _fc_hist_quarter(f)
    pares, curr, prev = _fcd_pairs()
    name = (request.args.get("name") or "").strip()
    if not pares:
        return jsonify(_empty_drill(name))
    w = _fcd_frame(q0, q1, curr, prev, f)
    return _fcw_drill_json(w, name, "leader", prev["dia"], curr["dia"],
                           "Movimento do dia")


# ===========================================================================
# DoD do Pipeline (só Leaders) — o destino de cada ESTÁGIO entre meias-noites
# ===========================================================================
# A mesma pergunta do Forecast DoD ("o que o volume de ontem virou"), com o
# eixo trocado: no lugar da faixa de probabilidade, o ESTÁGIO do funil. Pra
# cada um dos 6 estágios abertos, o pipe que estava nele às 00h de ontem:
# quanto ficou, quanto avançou pro próximo (ou pulou etapas, ou virou Won),
# quanto voltou, quanto foi pra Lost e quanto sumiu do funil.
#
# As pontas são EXATAMENTE as do Forecast DoD (`_fcd_pairs`): a foto própria
# das 00h BRT (`forecast_snapshots_daily`), ou a cópia do BI da véspera quando
# a virada não tem foto — e a tela avisa. Como a foto guarda os 6 estágios +
# Won + Lost dos últimos 200 dias, ela já é a foto do FUNIL inteiro, não só do
# forecast: não precisa de tabela nova.
#
# Sem recorte de trimestre, de propósito: o pipe é o funil aberto todo. 49% dos
# deals abertos não têm close date confiável (ver PIPELINE_TOTAL_PATHS), e
# cortar por ele aqui esconderia metade do movimento.
# ===========================================================================

# Janela do Lost nas pontas. A foto da meia-noite já só guarda o perdido dos
# últimos 200 dias; o mesmo corte na cópia do BI deixa as duas fontes iguais.
# Perdido com close date mais velho que isso não é movimento do dia: o HubSpot
# escreve o close date no dia em que o deal é marcado perdido.
_PPD_LOST_DIAS = 199

_PPD_OPEN = [s["id"] for s in STAGE_REGISTRY]
_PPD_NUM = {s["id"]: s["num"] for s in STAGE_REGISTRY}
# Cor de cada estágio — a mesma rampa azul→roxo do Funil, do topo pro fundo.
_PPD_STAGE_COLOR = {1: "#93C5FD", 2: "#60A5FA", 3: "#3B82F6",
                    4: "#6366F1", 5: "#7C3AED", 6: "#5B21B6"}

# Os destinos possíveis de quem estava num estágio aberto. A ORDEM é a da barra:
# da esquerda pra direita, do pior pro melhor, com o "ficou" no meio — o olho lê
# "vermelho à esquerda = sangrou, verde à direita = andou".
_PPD_MV_META = {
    "saiu":    {"label": "Saiu do funil", "color": "#B45309",
                "hint": "não está na foto final: No Show, outro pipeline, deal "
                        "apagado ou sem AE"},
    "lost":    {"label": "Lost", "color": "#B91C1C",
                "hint": "marcado closed lost no período"},
    "voltou":  {"label": "Voltou de estágio", "color": "#DB2777",
                "hint": "foi pra um estágio ANTERIOR do funil"},
    "ficou":   {"label": "Ficou no estágio", "color": "#CBD5E1",
                "hint": "mesmo estágio nas duas meias-noites"},
    "avancou": {"label": "Avançou pro próximo", "color": "#10B981",
                "hint": "foi pro estágio seguinte do funil"},
    "pulou":   {"label": "Pulou etapas", "color": "#047857",
                "hint": "avançou dois estágios ou mais de uma vez"},
    "won":     {"label": "Won", "color": "#065F46",
                "hint": "virou PO Won no período"},
}
_PPD_MV_ORDER = ("saiu", "lost", "voltou", "ficou", "avancou", "pulou", "won")
# Colunas-resumo da tabela: as quatro perguntas do pedido ("ficou no mesmo,
# foi pro próximo, foi pra Lost") mais a volta, que é a que mais incomoda.
# Somam 100% do estágio.
_PPD_RESUMO = [
    ("ficou",   "Ficou",   None,      ("ficou",)),
    ("avancou", "Avançou", "#047857", ("avancou", "pulou", "won")),
    ("voltou",  "Voltou",  "#DB2777", ("voltou",)),
    ("perdeu",  "Lost / saiu", "#B91C1C", ("lost", "saiu")),
]
# "Deal novo" tolera este atraso na data de criação. A foto da meia-noite lê a
# curated do BI, que chega a levar 2 dias pra trazer um deal recém-criado: em
# 07/10/2026, 9 deals criados em 05/10 apareceram pela primeira vez na foto de
# 07/10. Com a régua "criado depois da meia-noite anterior" os 9 viravam "veio
# de fora" e o dia não tinha deal novo nenhum.
_PPD_NOVO_ATRASO = timedelta(days=3)
# Quem chegou num estágio aberto sem estar em nenhum na véspera.
_PPD_IN_META = {
    "novo":    {"label": "Deal novo", "color": "#2563EB",
                "hint": "criado nos últimos dias (a carga do BI leva até 2 dias "
                        "pra trazer o deal novo)"},
    "reabriu": {"label": "Reaberto", "color": "#64748B",
                "hint": "era Won ou Lost e voltou pra um estágio aberto"},
    "fora":    {"label": "Veio de fora", "color": "#0891B2",
                "hint": "deal antigo que entrou no recorte: saiu de No Show, "
                        "trocou de pipeline ou ganhou AE"},
}
_PPD_IN_ORDER = ("novo", "reabriu", "fora")


def _ppd_raw(sides, force=False) -> pd.DataFrame:
    """As duas pontas, uma linha por (deal, dia), com `dia` = o dia da
    MEIA-NOITE. Mesmo recorte do Forecast DoD, só que o Lost vem pela janela de
    `_PPD_LOST_DIAS` e não pelo trimestre."""
    lo = (min(sd["dia"] for sd in sides)
          - timedelta(days=_PPD_LOST_DIAS)).isoformat()
    hi = "2100-01-01"
    partes = []
    for sd in sides:
        if sd["fonte"] == "meia-noite":
            x = bq.forecast_daily_series([sd["key"]], pipeline=_pl(),
                                         lost_from=lo, lost_to=hi,
                                         force_refresh=force)
        else:
            x = bq.forecast_asof_series(
                [sd["sufixo"]], _fc_open_stage_ids(), lost_stage_id=_HS_LOST_STAGE,
                lost_from=lo, lost_to=hi, force_refresh=force, pipeline=_pl())
        x = x.copy()
        x["dia"] = sd["key"]
        partes.append(x)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def _ppd_side(stage) -> str:
    """Estado de um deal numa ponta: 's1'..'s6', 'won', 'lost' ou 'out'."""
    if not isinstance(stage, str):
        return "out"
    if stage in _PPD_NUM:
        return f"s{_PPD_NUM[stage]}"
    if stage == _HS_WON_STAGE:
        return "won"
    if stage == _HS_LOST_STAGE:
        return "lost"
    return "out"


def _ppd_mv(sp: str, sc: str) -> str:
    """Movimento de quem estava ABERTO na véspera (sp = 's1'..'s6')."""
    if sc == sp:
        return "ficou"
    if sc in ("won", "lost"):
        return sc
    if sc == "out":
        return "saiu"
    d = int(sc[1:]) - int(sp[1:])
    return "avancou" if d == 1 else ("pulou" if d > 1 else "voltou")


def _ppd_frame(curr, prev, f, force=False) -> pd.DataFrame:
    """UMA linha por deal que estava aberto em pelo menos uma das pontas:
      · `s_prev`/`s_curr` — estado em cada meia-noite (ver `_ppd_side`);
      · `v_prev`/`v_curr` — amount em cada uma (0 onde o deal não existe);
      · `mv`  — pra quem estava aberto na véspera, o destino (`_PPD_MV_META`);
      · `ent` — pra quem NÃO estava, como chegou (`_PPD_IN_META`).
    Deal que não estava aberto em nenhuma das duas (Won/Lost acumulado) sai."""
    cols = ["deal_id", "deal_name", "account_executive", "leader", "region",
            "deal_type", "s_prev", "s_curr", "v_prev", "v_curr", "mv", "ent"]
    if not (curr and prev):
        return pd.DataFrame(columns=cols)
    raw = _ppd_raw([prev, curr], force=force)
    if not len(raw):
        return pd.DataFrame(columns=cols)

    lv = canon_ae_names(raw).copy()
    lv["amount"] = pd.to_numeric(lv["amount"], errors="coerce").fillna(0.0)
    # Hierarquia + roster/allowlist/líder/região/deal type — o mesmo escopo do
    # Forecast DoD, com o corte de quem saiu ancorado na véspera.
    lv = lv.merge(_fc_asof_leader_meta(force=force), on="account_executive", how="left")
    mgr = lv["account_executive"].isin(NON_AE_USERS)
    lv.loc[mgr, "leader"] = lv.loc[mgr, "account_executive"]
    lv.loc[mgr, "region"] = lv.loc[mgr, "region"].fillna("BR")
    lv = _fcw_scope(f, cut_from=prev["dia"])(lv)
    if not len(lv):
        return pd.DataFrame(columns=cols)

    lv = lv.drop_duplicates(subset=["dia", "deal_id"])
    lv["s"] = lv["stage"].map(_ppd_side)
    ant = lv[lv["dia"] == prev["key"]].set_index("deal_id")
    atu = lv[lv["dia"] == curr["key"]].set_index("deal_id")
    ids = ant.index.union(atu.index)
    w = pd.DataFrame(index=ids)
    w["s_prev"] = ant["s"].reindex(ids).fillna("out")
    w["s_curr"] = atu["s"].reindex(ids).fillna("out")
    _aberto = lambda s: s.str.startswith("s")  # noqa: E731
    w = w[_aberto(w["s_prev"]) | _aberto(w["s_curr"])]
    ids = w.index
    ant, atu = ant.reindex(ids), atu.reindex(ids)
    w["v_prev"] = ant["amount"].fillna(0.0)
    w["v_curr"] = atu["amount"].fillna(0.0)
    for col in ("deal_name", "account_executive", "leader", "region", "deal_type"):
        w[col] = atu[col].fillna(ant[col])

    w["mv"] = [(_ppd_mv(p, c) if p.startswith("s") else "")
               for p, c in zip(w["s_prev"], w["s_curr"])]
    # Como chegou quem não estava aberto: novo pela data de criação (imutável,
    # com a folga de `_PPD_NOVO_ATRASO`), reaberto se a véspera o tinha como
    # Won/Lost, e o resto veio de fora.
    pipe = _get_pipeline_deals(force=force)
    criado = dict(zip(pipe["deal_id"].astype(str),
                      pd.to_datetime(pipe["created_date"], errors="coerce")))
    _t0 = pd.Timestamp(prev["dia"] - _PPD_NOVO_ATRASO)
    ent = []
    for did, p, c in zip(w.index, w["s_prev"], w["s_curr"]):
        if p.startswith("s") or not c.startswith("s"):
            ent.append("")
        elif p in ("won", "lost"):
            ent.append("reabriu")
        else:
            cd = criado.get(str(did))
            if cd is not None and not pd.isna(cd):
                cd = pd.Timestamp(cd)
                cd = (cd.tz_convert(BRT).tz_localize(None) if cd.tzinfo else cd)
            ent.append("novo" if (cd is not None and not pd.isna(cd)
                                  and cd >= _t0) else "fora")
    w["ent"] = ent
    return w.reset_index().rename(columns={"index": "deal_id"})


def _ppd_destino(w) -> dict:
    """A tela inteira a partir do frame: uma barra por estágio (medida no valor
    da VÉSPERA, então soma exatamente o pipe que o estágio tinha), o bloco de
    quem chegou, e a ponte do pipe total entre as duas meias-noites."""
    rows, span = [], 0.0
    for s in STAGE_REGISTRY:
        k = f"s{s['num']}"
        sub = w[w["s_prev"].eq(k)]
        tot = float(sub["v_prev"].sum())
        span = max(span, tot)
        segs = []
        porm = {}
        for mv in _PPD_MV_ORDER:
            s2 = sub[sub["mv"].eq(mv)]
            v, n = float(s2["v_prev"].sum()), int(len(s2))
            porm[mv] = {"v": v, "n": n}
            if not n:
                continue
            segs.append({"key": mv, "label": _PPD_MV_META[mv]["label"],
                         "color": _PPD_MV_META[mv]["color"], "v": v, "n": n,
                         "pct": (v / tot) if tot else (n / len(sub)),
                         "e_ficou": mv == "ficou"})
        resumo = {}
        for key, _lbl, _cor, mvs in _PPD_RESUMO:
            v = sum(porm[m]["v"] for m in mvs)
            n = sum(porm[m]["n"] for m in mvs)
            resumo[key] = {"v": v, "n": n, "mvs": ",".join(mvs),
                           "pct": (v / tot) if tot else 0.0}
        # O que CHEGOU neste estágio (de qualquer lugar) e o nível de hoje —
        # pra linha ler como conta de estoque: ontem − saiu + chegou = hoje.
        cheg = w[w["s_curr"].eq(k) & ~w["s_prev"].eq(k)]
        fic = sub[sub["mv"].eq("ficou")]
        hoje = w[w["s_curr"].eq(k)]
        rows.append({
            "key": k, "num": s["num"], "label": s["label"],
            "color": _PPD_STAGE_COLOR[s["num"]],
            "total": tot, "n": int(len(sub)), "segs": segs, "resumo": resumo,
            "repreco_dp": float((fic["v_curr"] - fic["v_prev"]).sum()),
            "chegou_v": float(cheg["v_curr"].sum()), "chegou_n": int(len(cheg)),
            "hoje_v": float(hoje["v_curr"].sum()), "hoje_n": int(len(hoje)),
        })

    ent = w[w["ent"].ne("")]
    ent_segs = []
    for e in _PPD_IN_ORDER:
        s2 = ent[ent["ent"].eq(e)]
        if not len(s2):
            continue
        ent_segs.append({"key": e, "n": int(len(s2)), "v": float(s2["v_curr"].sum()),
                         **_PPD_IN_META[e]})

    # Ponte do pipe aberto: fecha por construção. Quem estava aberto nas duas
    # pontas contribui com a reprecificação; quem saiu, com o valor de ontem;
    # quem chegou, com o de hoje.
    ab_p = w["s_prev"].str.startswith("s")
    ab_c = w["s_curr"].str.startswith("s")
    both = w[ab_p & ab_c]
    def _m(mvs):
        x = w[ab_p & w["mv"].isin(mvs)]
        return {"v": float(x["v_prev"].sum()), "n": int(len(x))}
    ponte = {
        "ontem": {"v": float(w.loc[ab_p, "v_prev"].sum()), "n": int(ab_p.sum())},
        "hoje":  {"v": float(w.loc[ab_c, "v_curr"].sum()), "n": int(ab_c.sum())},
        "entrou": {"v": float(ent["v_curr"].sum()), "n": int(len(ent))},
        "won": _m(("won",)), "lost": _m(("lost",)), "saiu": _m(("saiu",)),
        "repreco": float((both["v_curr"] - both["v_prev"]).sum()),
        "avancou": _m(("avancou", "pulou")), "voltou": _m(("voltou",)),
    }
    return {"rows": rows, "span": span or 1.0, "ponte": ponte,
            "entrou": {"segs": ent_segs,
                       "span": max([x["v"] for x in ent_segs], default=0) or 1.0},
            "legenda": [{"key": m, **_PPD_MV_META[m]} for m in _PPD_MV_ORDER],
            "resumo_meta": [(k, l, c) for k, l, c, _ in _PPD_RESUMO],
            "mv_todos": ",".join(_PPD_MV_ORDER)}


def _ppd_lbl(s: str) -> str:
    if s.startswith("s"):
        return _STAGE_BY_NUM[int(s[1:])]["label"]
    return {"won": "Won", "lost": "Lost", "out": "fora do funil"}.get(s, s)


# ===========================================================================
# Pipeline WoW (só Leaders) — o mesmo Destino de cada estágio, semana a semana
# ===========================================================================
# Mesma tela, mesma conta e mesmas fotos do Pipeline DoD (`_ppd_frame` +
# `_ppd_destino`); só as pontas se afastam: a meia-noite de uma SEGUNDA-FEIRA
# contra a da segunda anterior, ou seja, a semana de segunda a domingo inteira.
# Segunda 00h e não domingo 12h (a ponta do PoP Early/Late) porque a foto
# própria é a da meia-noite; a semana fica completa, sem cortar o domingo ao meio.
#
# Semana sem foto própria (antes de 24/09/2026, ou uma virada furada) cai na
# cópia do BI do domingo, como no DoD — e a tela marca a ponta como aproximada.
# Só semanas FECHADAS: a corrente ainda não tem a ponta final.
# ===========================================================================

# Quantas semanas pra trás o chip oferece: dois meses, o bastante pra ver o
# trimestre virando sem virar uma fileira de botões.
_PPW_SEMANAS = 8


def _ppw_pairs(force=False):
    """(pares, curr, prev) do Pipeline WoW — o mesmo contrato de `_fcd_pairs`,
    com as pontas nas segundas 00h. Chip: `?semana=YYYYMMDD` (a segunda que
    FECHA a semana). Padrão: a última semana fechada."""
    hoje = _today()
    seg = hoje - timedelta(days=hoje.weekday())
    dias = [seg - timedelta(weeks=i) for i in range(_PPW_SEMANAS + 1)]
    sides = _fcd_sides(force=force, dias=dias)
    pares = []
    for d in dias[:-1]:
        k, kp = d.strftime("%Y%m%d"), (d - timedelta(days=7)).strftime("%Y%m%d")
        if k in sides and kp in sides:
            pares.append((sides[k], sides[kp]))
    if not pares:
        return [], None, None
    sel = (request.args.get("semana") or "").strip()
    curr, prev = next(((c, p) for c, p in pares if c["key"] == sel), pares[0])
    return pares, curr, prev


def _ppw_semana_txt(prev, curr) -> str:
    """'5–11 out' / '28 set–4 out' — a semana MEDIDA (segunda a domingo)."""
    a, b = prev["dia"], curr["dia"] - timedelta(days=1)
    if a.month == b.month:
        return f"{a.day}–{b.day} {PT_MONTHS_SHORT[b.month-1]}"
    return f"{_fmt_date_curta(a)}–{_fmt_date_curta(b)}"


# Textos que mudam entre o DoD e o WoW. Tudo o mais (conta, barra, drill) é o
# mesmo código, de propósito: as duas telas têm de ler igual.
_PPD_MODOS = {
    "dod": {
        "titulo": "Pipeline DoD", "sigla": "Day over day: o dia", "inteiro": "inteiro",
        "chip": "dia", "pares": _fcd_pairs,
        "page": "leaders_pipeline_dod", "drill": "leaders_pipeline_dod_deals",
        "sem_par_h": "Sem par de dias comparável",
        "sem_par_p": "A tela compara a foto da meia-noite de um dia com a do dia "
                     "anterior, e ainda não há duas pontas consecutivas nos últimos dias.",
        "depois": "dia depois", "valor_ant": "valor de ontem",
        "foto_atual": "foto de hoje", "dentro": "DENTRO do dia",
        "mov": "Movimento do dia", "col_estagio": "Estágio (00h ontem → 00h hoje)",
        "col_ant": "MRR ontem", "col_atu": "MRR hoje",
    },
    "wow": {
        "titulo": "Pipeline WoW", "sigla": "Week over week: a semana", "inteiro": "inteira",
        "chip": "semana", "pares": _ppw_pairs,
        "page": "leaders_pipeline_wow", "drill": "leaders_pipeline_wow_deals",
        "sem_par_h": "Sem par de semanas comparável",
        "sem_par_p": "A tela compara a foto da meia-noite de uma segunda-feira com "
                     "a da segunda anterior, e ainda não há duas pontas assim nas "
                     "últimas semanas.",
        "depois": "semana depois", "valor_ant": "valor do início da semana",
        "foto_atual": "foto do fim da semana", "dentro": "DENTRO da semana",
        "mov": "Movimento da semana",
        "col_estagio": "Estágio (início → fim da semana)",
        "col_ant": "MRR início", "col_atu": "MRR fim",
    },
}


@app.route("/leaders/pipeline/dod")
def leaders_pipeline_dod():
    """DoD do Pipeline: o destino do pipe de cada estágio entre a meia-noite de
    um dia e a do anterior. Só no Leaders, como o Forecast DoD."""
    return _ppd_page("dod")


@app.route("/leaders/pipeline/wow")
def leaders_pipeline_wow():
    """WoW do Pipeline: o destino do pipe de cada estágio entre a meia-noite de
    uma segunda-feira e a da anterior. Só no Leaders, como o DoD."""
    return _ppd_page("wow")


def _ppd_page(modo: str):
    L = _PPD_MODOS[modo]
    grain = "leader"
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    pares, curr, prev = L["pares"](force=force)
    w = _ppd_frame(curr, prev, f, force=force)

    def _lbl(sd):
        return f"00h de {_fmt_date_short(sd['dia'])}" if sd else "—"

    def _drill(**kw):
        return url_for(L["drill"], grain=grain,
                       **{L["chip"]: (curr["key"] if curr else None)},
                       dtype=f.get("dtype") or None,
                       leader=f.get("leader") or None,
                       region=f.get("region") or None, **kw)

    def _chip_url(**kw):
        args = {k: v for k, v in request.args.items() if k not in kw and v}
        return request.path + "?" + urllib.parse.urlencode({**args, **kw})

    def _chip_lbl(c, p):
        return (_fmt_date_curta(p["dia"]) if modo == "dod"
                else _ppw_semana_txt(p, c))

    if not prev:
        periodo = "—"
    elif modo == "dod":
        periodo = _fmt_date_short(prev["dia"])
    else:
        periodo = f"{_ppw_semana_txt(prev, curr)} {curr['dia'].year}"

    return render_template(
        "pipeline_dod.html", ctx=ctx, grain=grain, L=L,
        curr_label=_lbl(curr), prev_label=_lbl(prev),
        dia_medido=periodo,
        pares=[{"key": c["key"], "label": _chip_lbl(c, p),
                "aprox": "bi" in (c["fonte"], p["fonte"]),
                "title": f"{_lbl(p)} → {_lbl(c)}"} for c, p in pares],
        dia_sel=(curr["key"] if curr else ""),
        curr_txt=(_fcd_side_txt(curr) if curr else ""),
        prev_txt=(_fcd_side_txt(prev) if prev else ""),
        aprox=bool(curr and "bi" in (curr["fonte"], prev["fonte"])),
        sem_par=not pares, sem_dado=not len(w),
        destino=(_ppd_destino(w) if len(w) else None),
        drill=_drill, chip_url=_chip_url,
    )


@app.route("/leaders/pipeline/dod/deals")
def leaders_pipeline_dod_deals():
    """Drill do Pipeline DoD. `de` = estágio de origem ('s1'..'s6') ou 'in' (quem
    chegou); `mv` = destinos (ou modos de chegada) separados por vírgula."""
    return _ppd_deals("dod")


@app.route("/leaders/pipeline/wow/deals")
def leaders_pipeline_wow_deals():
    """Drill do Pipeline WoW — o mesmo do DoD, com as pontas da semana."""
    return _ppd_deals("wow")


def _ppd_deals(modo: str):
    L = _PPD_MODOS[modo]
    f = get_filters()
    pares, curr, prev = L["pares"]()
    name = (request.args.get("name") or "").strip()
    if not pares:
        return jsonify(_empty_drill(name))
    w = _ppd_frame(curr, prev, f)
    if not len(w):
        return jsonify(_empty_drill(name))
    de = (request.args.get("de") or "").strip()
    mvs = [x for x in (request.args.get("mv") or "").split(",") if x]
    if de == "in":
        w = w[w["ent"].ne("")]
        if mvs:
            w = w[w["ent"].isin(mvs)]
        titulo = "Chegou no funil"
        if len(mvs) == 1 and mvs[0] in _PPD_IN_META:
            titulo += f" · {_PPD_IN_META[mvs[0]]['label']}"
    else:
        w = w[w["s_prev"].str.startswith("s")]
        if de:
            w = w[w["s_prev"].eq(de)]
        if mvs:
            w = w[w["mv"].isin(mvs)]
        titulo = _ppd_lbl(de) if de else "Pipe aberto"
        if len(mvs) == 1 and mvs[0] in _PPD_MV_META:
            titulo += f" → {_PPD_MV_META[mvs[0]]['label']}"
        elif mvs and set(mvs) != set(_PPD_MV_ORDER):
            titulo += " → " + " / ".join(_PPD_MV_META[m]["label"] for m in mvs
                                         if m in _PPD_MV_META)

    _tipo = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}
    rows = []
    for r in w.itertuples():
        mv = (_PPD_IN_META[r.ent]["label"] if r.ent
              else _PPD_MV_META.get(r.mv, {}).get("label", r.mv))
        rows.append({
            "deal_name": r.deal_name or "—",
            "deal_url": _hs_deal_url(r.deal_id),
            "ae": r.account_executive or "—",
            "tipo": _tipo.get(r.deal_type, r.deal_type or "—"),
            "movimento": mv,
            "de_para": f"{_ppd_lbl(r.s_prev)} → {_ppd_lbl(r.s_curr)}",
            "v_prev": float(r.v_prev), "v_curr": float(r.v_curr),
            "delta": float(r.v_curr - r.v_prev),
        })
    # Quem saiu é medido pelo valor de ontem, quem chegou pelo de hoje — a mesma
    # régua da barra, pra o total do drill bater com o pedaço clicado.
    _med = "v_curr" if de == "in" else "v_prev"
    rows.sort(key=lambda x: x[_med], reverse=True)
    _tot = sum(x[_med] for x in rows)
    return jsonify({
        "title": f"{titulo} · {L['mov']}",
        "subtitle": (f"{_fmt_date_short(prev['dia'])} → {_fmt_date_short(curr['dia'])} · "
                     + _drill_money_subtitle(len(rows), _tot)),
        "columns": [
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "ae", "label": "AE"},
            {"key": "movimento", "label": "Movimento"},
            {"key": "de_para", "label": L["col_estagio"]},
            {"key": "v_prev", "label": L["col_ant"], "type": "money"},
            {"key": "v_curr", "label": L["col_atu"], "type": "money"},
        ],
        "rows": rows,
    })


@app.route("/forecast")
def forecast():
    return _forecast_page("ae")


@app.route("/leaders/forecast")
def leaders_forecast():
    return _forecast_page("leader")


def _forecast_hold_by_ae(f, from_d, to_d, force=False):
    """(aep recortado, on-hold MRR por AE) do período — `total_mrr_on_hold_brl`
    da curated, com gestores. Compartilhado pela aba Forecast e pelo Modelo
    Analytics, pra o card On-hold ser o MESMO número nas duas."""
    aep = _get_ae_perf_with_managers(force=force)
    aep = aep[(aep["quarter_end"] >= from_d) & (aep["quarter"] <= to_d)]
    aep = aep[~aep["account_executive"].isin(DEPARTED_AES)]
    aep = aep[aep["leader"].isin(ALLOWED_LEADERS)]
    if f["region"]:
        aep = aep[aep["region"] == f["region"]]
    if f["leader"]:
        aep = aep[aep["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        aep = aep[aep["account_executive"] == f["ae"]]
    hold = (aep.groupby("account_executive")["total_mrr_on_hold_brl"].sum()
            if len(aep) else pd.Series(dtype=float))
    return aep, hold


def _forecast_page(grain="ae"):
    """Forecast ponderado: deals com close date no quarter, nos estágios 1-6
    (Scheduled→Procurement), ponderados pela % da forecast category do HubSpot,
    que na tela é nomeada pelo próprio peso (100% · 75% · 50% · 25% · 0%).
    Mesma parametrização de líder/região/AE de Demanda e Pipeline.
    grain='leader' → a tabela/gráfico por AE vira por líder (soma do time)."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # Close date define o que está no quarter. Default = quarter atual (calendário).
    today = _today()
    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else _quarter_start_of(today)
    to_d   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else _quarter_end_of(today)

    deals = _get_forecast_deals(force=force)

    # Parametrização de líder/região/AE (igual demand/pipeline). Mantém vendas
    # diretas dos gestores (já atribuídas a eles em _get_forecast_deals); remove só
    # AEs desligados. O recorte por DATA é separado por tipo (abaixo).
    dd = apply_leader_overrides(deals.copy())
    dd = dd[~dd["account_executive"].isin(DEPARTED_AES)]
    dd = dd[dd["leader"].isin(ALLOWED_LEADERS)]
    if f["region"]:
        dd = dd[dd["region"] == f["region"]]
    if f["leader"]:
        dd = dd[dd["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        dd = dd[dd["account_executive"] == f["ae"]]
    # Deal Type — mesma chave curta da URL (dtype=acq/exp) e o mesmo helper do
    # Pipeline Total e do WoW, pra o corte significar a mesma coisa em toda aba.
    # Recorta o frame ANTES do split won/pipe, então realizado e ponderado falam
    # do mesmo universo e a Projeção continua fechando.
    dtype_sel = (f.get("dtype") or "").strip()
    dd = _pt_apply_dim_filter(dd, "dtype", dtype_sel)
    # Duas coisas desta tela NÃO se dividem por deal type, e a tela prefere
    # apagá-las a mostrar um número que não responde à pergunta:
    #   · On-hold vem de `ae_performance` — é MRR agregado POR AE, sem deal do
    #     lado, então não há como saber quanto do hold é aquisição;
    #   · Comp. Target é meta de receita CHEIA do líder, não tem versão "só
    #     aquisição" — dividir a projeção de um tipo por ela daria um
    #     atingimento sistematicamente baixo e falso.
    hold_ok = meta_ok = not dtype_sel

    lo, hi = pd.Timestamp(from_d), pd.Timestamp(to_d)
    # Pipeline aberto (estágios 1-6): recorte pelo CLOSE DATE (quando esperam fechar).
    pipe = dd[(~dd["is_won"]) & dd["close_date"].notna()
              & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)]
    # Realizado (PO Won): quarter definido pela DATA DE VALIDAÇÃO (po_emission,
    # fallback close date) — priorizada sobre o close date do HubSpot. É isso que
    # joga os deals fechados em 1-3/abr (validados em março) pro Q1, não pro Q2.
    # Mais a extensão de fechamento (`rev_date`): validado em 01–02/10/2026 é
    # realizado do Q3. `realized_deals` sai deste mesmo frame de propósito — é a
    # legenda do card ("N PO Won"), a composição do próprio R$ do Realizado.
    won = dd[dd["is_won"] & dd["rev_date"].notna()
             & (dd["rev_date"] >= lo) & (dd["rev_date"] <= hi)]

    # `label` = o parâmetro cru do HubSpot; `pct` = o peso EFETIVO, o que de fato
    # multiplica o amount (mesma convenção do `_pt_dim_meta`).
    cats = [{"key": _fc_key(c), "raw": c, "label": FORECAST_CATEGORY_LABEL[c],
             "pct": FORECAST_CATEGORY_PCT_EFF[c], "color": FORECAST_CATEGORY_COLOR[c],
             "ink": FORECAST_CATEGORY_INK[c]}
            for c in FORECAST_CATEGORY_ORDER]

    # ----- on-hold (MRR) por AE — total_mrr_on_hold_brl da curated (escolha do
    # usuário pro hold). Inclui gestores. Unidade = MRR → mostrado à parte. -----
    aep, hold_by_ae = _forecast_hold_by_ae(f, from_d, to_d, force=force)
    if not hold_ok:
        hold_by_ae = pd.Series(dtype=float)

    # ----- agrega por AE: pipeline ponderado + realizado (amount) + hold (MRR) -----
    rows = {}
    def _row(ae_name, leader, region):
        r = rows.get(ae_name)
        if r is None:
            r = {"account_executive": ae_name, "leader": leader, "region": region,
                 "is_manager": ae_name in _managers_in(_roster_window(from_d, to_d)),
                 "gross": 0.0, "weighted": 0.0, "deals": 0,
                 "realized": 0.0, "realized_deals": 0, "hold": 0.0,
                 **{"w_" + c["key"]: 0.0 for c in cats}}
            rows[ae_name] = r
        return r

    for (ae_name, leader, region), grp in pipe.groupby(
            ["account_executive", "leader", "region"], dropna=False):
        r = _row(ae_name, leader, region)
        r["gross"] = float(grp["amount"].sum())
        r["weighted"] = float(grp["weighted"].sum())
        r["deals"] = int(len(grp))
        for c in cats:
            r["w_" + c["key"]] = float(
                grp.loc[grp["forecast_category"] == c["raw"], "weighted"].sum())

    for (ae_name, leader, region), grp in won.groupby(
            ["account_executive", "leader", "region"], dropna=False):
        r = _row(ae_name, leader, region)
        r["realized"] = float(grp["amount"].sum())     # PO Won, por data de validação
        r["realized_deals"] = int(len(grp))

    if len(hold_by_ae):
        aep_meta = (aep.sort_values("quarter")
                       .drop_duplicates("account_executive", keep="last")
                       .set_index("account_executive")[["leader", "region"]])
        for ae_name, hv in hold_by_ae.items():
            if not hv:
                continue
            meta = aep_meta.loc[ae_name] if ae_name in aep_meta.index else None
            r = _row(ae_name,
                     meta["leader"] if meta is not None else None,
                     meta["region"] if meta is not None else None)
            r["hold"] = float(hv)

    by_ae = list(rows.values())

    # Grain de líder: agrega as linhas por-AE em linhas por-líder (soma do time).
    if grain == "leader":
        _fc_fields = ["gross", "weighted", "deals", "realized", "realized_deals",
                      "hold"] + ["w_" + c["key"] for c in cats]
        by_ae = _leader_rollup(by_ae, f["leader"], _fc_fields)
        for r in by_ae:
            r["is_manager"] = False

    for r in by_ae:
        r["projection"] = r["realized"] + r["weighted"]   # realizado + ponderado (ambos amount)
        r["pct_of_gross"] = (r["weighted"] / r["gross"]) if r["gross"] else 0.0

    # Meta do líder (Comp. Target) — MESMO número do Modelo Analytics, pra que as
    # duas telas de forecast comparem a projeção contra o mesmo alvo. Só existe no
    # grain de líder: no grain de AE a meta é a quota individual, que esta tela
    # (composição por forecast category) não usa.
    _fc_targets = _leader_targets(_roster_window(from_d, to_d))
    if grain == "leader" and meta_ok:
        _apply_leader_meta(by_ae, targets=_fc_targets)
    by_ae.sort(key=lambda r: r["projection"], reverse=True)

    # Gráfico = Realizado (Won) + forecast ponderado por AE/líder (ambos deal amount).
    # Gestores ficam fora das barras; inclui quem tem realizado mesmo sem pipeline.
    chart_rows = [r for r in by_ae if not r["is_manager"] and (r["weighted"] > 0 or r["realized"] > 0)]
    max_projection = max((r["projection"] for r in chart_rows), default=0.0)
    # Escala do gráfico inclui a meta: sem isso a tracejada da meta de quem está
    # abaixo do alvo (meta > projeção do topo) sairia fora da área plotada.
    chart_max = max([max_projection] + [float(r.get("quota") or 0.0) for r in chart_rows])

    # ----- composição por forecast category (só pipeline aberto) -----
    by_cat = []
    for c in cats:
        sub = pipe[pipe["forecast_category"] == c["raw"]]
        by_cat.append({**c,
                       "gross": float(sub["amount"].sum()),
                       "weighted": float(sub["weighted"].sum()),
                       "deals": int(len(sub))})
    # Não previstos (Omit/None): pipeline real nos estágios 1-6, mas 0% no forecast.
    nf = pipe[~pipe["forecast_category"].isin([c["raw"] for c in cats])]
    not_forecasted = {"gross": float(nf["amount"].sum()), "deals": int(len(nf))}

    mgr_rows = [r for r in by_ae if r["is_manager"]]
    totals = {
        "gross": float(pipe["amount"].sum()),
        "weighted": float(pipe["weighted"].sum()),
        "deals": int(len(pipe)),
        "realized": float(won["amount"].sum()),
        "realized_deals": int(len(won)),
        "hold": float(hold_by_ae.sum()) if len(hold_by_ae) else 0.0,
        # contagem de AEs (exclui gestores) com pipeline ou realizado no período
        "n_ae": sum(1 for r in by_ae if not r["is_manager"] and (r["weighted"] or r["realized"])),
        # vendas diretas dos gestores já embutidas nos números acima (vão na nota)
        "mgr_weighted": float(sum(r["weighted"] for r in mgr_rows)),
        "mgr_realized": float(sum(r["realized"] for r in mgr_rows)),
        "mgr_count": sum(1 for r in mgr_rows if r["weighted"] or r["realized"]),
    }
    totals["projection"] = totals["realized"] + totals["weighted"]
    totals["blended"] = (totals["weighted"] / totals["gross"]) if totals["gross"] else 0.0
    # Meta do time = soma dos Comp. Targets dos líderes de TOPO no recorte
    # (_leader_meta_total tira o gerente que já está dentro do diretor). Igual ao
    # rodapé do Modelo Analytics.
    if grain == "leader" and meta_ok:
        totals["quota"] = _leader_meta_total([r["leader"] for r in by_ae], targets=_fc_targets)
        totals["attainment"] = ((totals["projection"] / totals["quota"])
                                if totals["quota"] else None)
        totals["gap"] = (totals["quota"] - totals["projection"]) if totals["quota"] else None
    else:
        totals["quota"] = 0.0
        totals["attainment"] = None
        totals["gap"] = None

    range_label = f"{from_d.strftime('%d/%m/%Y')} → {to_d.strftime('%d/%m/%Y')}"
    return render_template(
        "forecast.html", ctx=ctx, cats=cats,
        by_ae=by_ae, chart_rows=chart_rows, by_cat=by_cat, totals=totals,
        not_forecasted=not_forecasted, max_projection=max_projection,
        chart_max=chart_max, range_label=range_label, grain=grain,
        dtype_sel=dtype_sel, dtype_label=_pt_dim_label("dtype", dtype_sel),
        hold_ok=hold_ok, meta_ok=meta_ok)


@app.route("/forecast/model")
def forecast_model():
    return _forecast_model_page("ae")


@app.route("/leaders/forecast/model")
def leaders_forecast_model():
    return _forecast_model_page("leader")


def _forecast_model_page(grain="ae"):
    """Sub-aba 'Modelo Analytics' do Forecast. Projeção = Done (PO Won no período)
    + forecast por conversão histórica: cada deal ABERTO em Negotiation/Decision/
    Procurement é ponderado pela probabilidade do seu segmento×estágio×tipo
    (FORECAST_MODEL_PROB). Chart: barras Quota × (Done+Forecast) + linha de % de
    atingimento por vendedor/líder — mesmo componente da cobertura de pipeline."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    today = _today()
    from_d = pd.to_datetime(f["date_from"]).date() if f["date_from"] else _quarter_start_of(today)
    to_d   = pd.to_datetime(f["date_to"]).date()   if f["date_to"]   else _quarter_end_of(today)
    lo, hi = pd.Timestamp(from_d), pd.Timestamp(to_d)

    deals = _get_forecast_deals(force=force)
    dd = deals[~deals["account_executive"].isin(DEPARTED_AES)].copy()
    dd = dd[dd["leader"].isin(ALLOWED_LEADERS)]
    if f["region"]:
        dd = dd[dd["region"] == f["region"]]
    if f["leader"]:
        dd = dd[dd["leader"].isin(expand_leader(f["leader"]))]
    if f["ae"]:
        dd = dd[dd["account_executive"] == f["ae"]]

    # Done = PO Won no período (data de validação).
    # Done = receita realizada: data de validação com a extensão de fechamento.
    won = dd[dd["is_won"] & dd["rev_date"].notna()
             & (dd["rev_date"] >= lo) & (dd["rev_date"] <= hi)]
    done_by_ae = won.groupby("account_executive")["amount"].sum()

    # Forecast por conversão = deals ABERTOS nos 3 estágios modelados, com close
    # date no período, ponderados pela probabilidade do segmento×estágio×tipo.
    openm = dd[(~dd["is_won"]) & dd["dealstage"].isin(MODEL_STAGE.keys())
               & dd["close_date"].notna()
               & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)].copy()
    if len(openm):
        openm["prob"] = openm.apply(
            lambda r: _model_prob(r["leader"], r["dealstage"], r["deal_type"],
                                  r.get("account_executive")), axis=1)
        openm["weighted_model"] = openm["amount"] * openm["prob"]
        fc_by_ae = openm.groupby("account_executive")["weighted_model"].sum()
    else:
        fc_by_ae = pd.Series(dtype=float)

    # Quota por AE — MESMA fonte do overview/pipeline (roster do trimestre, senão curated).
    rq = _roster_window(from_d, to_d)
    period_is_q3 = rq is not None
    if period_is_q3:
        scope = expand_leader(f["leader"]) if f["leader"] else None
        quota_by_ae = {}
        for name, v in _roster_of(rq).items():
            if f["region"] and (_roster_region() != f["region"]):
                continue
            if scope is not None and v["leader"] not in scope:
                continue
            if f["ae"] and name != f["ae"]:
                continue
            quota_by_ae[name] = float(v["quota"])
    else:
        aep = _get_clean_ae_perf(force=force)
        aep = aep[(aep["quarter"] <= to_d) & (aep["quarter_end"] >= from_d)]
        if f["region"]:
            aep = aep[aep["region"] == f["region"]]
        if f["leader"]:
            aep = aep[aep["leader"].isin(expand_leader(f["leader"]))]
        if f["ae"]:
            aep = aep[aep["account_executive"] == f["ae"]]
        quota_by_ae = (aep.groupby("account_executive")["quota_in_brl"].sum().to_dict()
                       if len(aep) else {})

    meta = {}
    for _, r in dd.drop_duplicates("account_executive").iterrows():
        meta[r["account_executive"]] = {"leader": r["leader"], "region": r["region"]}
    if period_is_q3:
        for name in quota_by_ae:
            meta.setdefault(name, {"leader": _roster_of(rq)[name]["leader"], "region": _roster_region()})

    ae_names = sorted(set(done_by_ae.index) | set(fc_by_ae.index)
                      | {a for a, q in quota_by_ae.items() if q > 0})
    by_ae = []
    for a in ae_names:
        done = float(done_by_ae.get(a, 0.0)); fc = float(fc_by_ae.get(a, 0.0))
        m = meta.get(a, {}); lead = m.get("leader")
        by_ae.append({
            "account_executive": a, "leader": lead, "region": m.get("region"),
            "is_manager": a in _managers_in(rq),
            "segment": SEGMENT_LABEL.get(LEADER_SEGMENT.get(lead), "—"),
            "done": done, "forecast": fc, "projection": done + fc,
            "quota": float(quota_by_ae.get(a, 0.0) or 0.0),
        })

    if grain == "leader":
        rows = _leader_rollup(by_ae, f["leader"], ["done", "forecast", "projection", "quota"])
        # Meta do líder = Comp. Target da planilha, não a soma das quotas do time.
        # Mesmo helper do forecast ponderado — os dois modelos leem o mesmo alvo.
        _apply_leader_meta(rows, targets=_leader_targets(rq))
        mpath, dkey, unit = "/leaders/forecast/model", "leader", "líder"
    else:
        rows = [r for r in by_ae if not r["is_manager"]]
        mpath, dkey, unit = "/forecast/model", "ae", "vendedor"
    for r in rows:
        r["attainment"] = (r["projection"] / r["quota"]) if r["quota"] else None
        if grain == "leader":
            r["segment"] = SEGMENT_LABEL.get(LEADER_SEGMENT.get(r["leader"]), "—")

    _drill = {k: v for k, v in (("date_from", f["date_from"]), ("date_to", f["date_to"]),
                                ("leader", f["leader"]), ("region", f["region"])) if v}
    _sorted = sorted(rows, key=lambda r: (r["attainment"] is not None,
                                          r["attainment"] if r["attainment"] is not None else -1.0),
                     reverse=True)
    items = [{"name": r.get("name") or r["account_executive"],
              "short": r.get("short") or short_name(r.get("name") or r["account_executive"]),
              "leader": r.get("leader"), "realized": r["projection"],
              "quota": r["quota"], "attainment": r["attainment"]} for r in _sorted]
    chart = (_build_rank_chart(items, _drill, axis_mode="currency", line_mode="percent",
                               drill_path=mpath, drill_key=dkey) if items else None)

    table = sorted(rows, key=lambda r: r["projection"], reverse=True)
    # Done/Forecast/Projeção: sempre do by_ae por-AE (verdade do time). Quota: no
    # grain de líder é a soma dos Board Targets de topo; no AE é a soma das quotas dos AEs.
    totals = {
        "done": sum(r["done"] for r in by_ae),
        "forecast": sum(r["forecast"] for r in by_ae),
        "projection": sum(r["projection"] for r in by_ae),
        "quota": (_leader_meta_total([r["leader"] for r in rows], targets=_leader_targets(rq)) if grain == "leader"
                  else sum(r["quota"] for r in by_ae)),
    }
    totals["attainment"] = (totals["projection"] / totals["quota"]) if totals["quota"] else None
    totals["gap"] = (totals["quota"] - totals["projection"]) if totals["quota"] else None

    # ---- Cards — os mesmos 6 da aba Forecast, com o ponderado do MODELO. ----
    # Pipeline bruto = TODO o aberto (estágios 1-6) com close date no período, a
    # mesma base da aba Forecast: as duas telas mostram o mesmo bruto e só
    # diferem no peso. Aqui só Negotiation/Decision/Procurement pesam (o resto
    # vale 0 no modelo), então a parte que pesa vai no rodapé do card.
    pipe_all = dd[(~dd["is_won"]) & dd["close_date"].notna()
                  & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)]
    _aep, hold_by_ae = _forecast_hold_by_ae(f, from_d, to_d, force=force)
    mgr = [r for r in by_ae if r["is_manager"]]
    totals.update({
        "realized_deals": int(len(won)),
        "gross": float(pipe_all["amount"].sum()),
        "deals": int(len(pipe_all)),
        "gross_model": float(openm["amount"].sum()) if len(openm) else 0.0,
        "deals_model": int(len(openm)),
        "hold": float(hold_by_ae.sum()) if len(hold_by_ae) else 0.0,
        "n_ae": sum(1 for r in by_ae if not r["is_manager"] and (r["forecast"] or r["done"])),
        "mgr_done": float(sum(r["done"] for r in mgr)),
        "mgr_forecast": float(sum(r["forecast"] for r in mgr)),
        "mgr_count": sum(1 for r in mgr if r["forecast"] or r["done"]),
    })
    totals["blended"] = (totals["forecast"] / totals["gross"]) if totals["gross"] else 0.0

    range_label = f"{from_d.strftime('%d/%m/%Y')} → {to_d.strftime('%d/%m/%Y')}"
    return render_template("forecast_model.html", ctx=ctx, grain=grain, unit=unit,
                           chart=chart, table=table, totals=totals, range_label=range_label,
                           prob=FORECAST_MODEL_PROB, seg_label=SEGMENT_LABEL,
                           stage_order=MODEL_STAGE_ORDER, stage_label=MODEL_STAGE_LABEL,
                           safety=MODEL_SAFETY_FACTOR)


@app.route("/forecast/model/deals")
def forecast_model_deals():
    """Drill da projeção do Modelo Analytics: Done (PO Won pela data de validação,
    com a extensão de fechamento) + os deals abertos em Negotiation/Decision/
    Procurement com close date no período, cada um com a probabilidade do modelo
    e o ponderado. Mesmo recorte do `_forecast_model_page`, então a soma da
    coluna Ponderado é a Projeção do card."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    lo, hi = _forecast_window(request.args)
    dd = _get_forecast_deals()
    dd = dd[~dd["account_executive"].isin(DEPARTED_AES)]
    dd = dd[dd["leader"].isin(ALLOWED_LEADERS)]
    dd = _entity_filter(dd, name, grain, request.args.get("leader"), request.args.get("region"))

    won = dd[dd["is_won"] & dd["rev_date"].notna()
             & (dd["rev_date"] >= lo) & (dd["rev_date"] <= hi)]
    openm = dd[(~dd["is_won"]) & dd["dealstage"].isin(MODEL_STAGE.keys())
               & dd["close_date"].notna()
               & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)]
    _tipo = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}
    rows = []
    for _, r in won.iterrows():
        amt = float(r["amount"] or 0)
        _tag = _rev_tag(r["valid_date"])
        rows.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                     "ae": r["account_executive"],
                     "deal_type": _tipo.get(r.get("deal_type"), r.get("deal_type") or "—"),
                     "categoria": f"Done (realizado · {_tag})" if _tag else "Done (realizado)",
                     "prob": 1.0, "amount": amt, "ponderado": amt,
                     "data": r["valid_date"].date().isoformat() if pd.notna(r["valid_date"]) else ""})
    for _, r in openm.iterrows():
        p = _model_prob(r["leader"], r["dealstage"], r["deal_type"], r.get("account_executive"))
        amt = float(r["amount"] or 0)
        rows.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                     "ae": r["account_executive"],
                     "deal_type": _tipo.get(r.get("deal_type"), r.get("deal_type") or "—"),
                     "categoria": stage_display(r["dealstage"]),
                     "prob": p, "amount": amt, "ponderado": amt * p,
                     "data": r["close_date"].date().isoformat() if pd.notna(r["close_date"]) else ""})
    for x in rows:
        x["prob_txt"] = f"{round(x['prob'] * 100)}%"
    rows.sort(key=lambda x: x["ponderado"], reverse=True)
    proj = float(sum(x["ponderado"] for x in rows))
    return jsonify({
        "title": (name or "Modelo Analytics — total") + " · Projeção",
        "subtitle": (f"{len(won)} done + {len(openm)} em Negotiation/Decision/Procurement · "
                     f"projeção {fmt_money(proj)}"),
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "deal_type", "label": "Tipo"},
            {"key": "categoria", "label": "Done / estágio"},
            {"key": "prob_txt",  "label": "Prob.", "align": "right", "sortKey": "prob"},
            {"key": "amount",    "label": "Valor bruto", "type": "money"},
            {"key": "ponderado", "label": "Ponderado",   "type": "money"},
            {"key": "data",      "label": "Data",        "type": "date"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


_GZIP_STATIC_CACHE = {}


def _gzip_static_file(disk_path):
    """Lê um asset do /static e devolve o conteúdo gzipado (bytes), ou None se
    não der pra ler. Cacheado em memória por (path, mtime) — o send_file usa
    direct_passthrough e escaparia do gzip de body, então comprimimos aqui uma
    vez e reservimos os bytes prontos nas próximas requests."""
    try:
        mtime = os.path.getmtime(disk_path)
    except OSError:
        return None
    cached = _GZIP_STATIC_CACHE.get(disk_path)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    try:
        with open(disk_path, "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    import gzip, io
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
        gz.write(raw)
    gzdata = buf.getvalue()
    _GZIP_STATIC_CACHE[disk_path] = (mtime, gzdata)
    return gzdata


@app.after_request
def perf_headers(response):
    """Gzip pra HTML/CSS/JS + cache forte pro /static + ETag/304 pra revisitas.

    ETag em rotas dinâmicas: revisitar a mesma URL (back/forward, segunda aba)
    devolve 304 sem reenviar body — Flask faz hash do body uma vez e o navegador
    serve do cache local depois. Não ajuda em mudança de filtro (URL nova),
    ajuda em re-acesso da mesma URL."""
    # Assets estáticos: cache-busted via ?v=mtime (base.html) → pode cachear forte
    # e nunca revalidar. Também gzipa aqui, pois o send_file usa direct_passthrough
    # e escapa do gzip de body lá embaixo (era o CSS de 64KB saindo cru).
    if request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        ct = response.content_type or ""
        accept = request.headers.get("Accept-Encoding", "")
        gzippable = (ct.startswith("text/") or "javascript" in ct
                     or "css" in ct or "svg" in ct or "json" in ct)
        already = "gzip" in (response.headers.get("Content-Encoding") or "")
        if ("gzip" in accept and gzippable and not already
                and response.status_code == 200):
            static_root = os.path.join(app.root_path, "static")
            fname = request.path[len("/static/"):].lstrip("/")
            disk = os.path.normpath(os.path.join(static_root, fname))
            if disk.startswith(static_root + os.sep):
                gzdata = _gzip_static_file(disk)
                if gzdata is not None:
                    response.direct_passthrough = False
                    response.set_data(gzdata)
                    response.headers["Content-Encoding"] = "gzip"
                    response.headers["Content-Length"] = str(len(gzdata))
                    response.headers["Vary"] = "Accept-Encoding"
        return response

    # ETag/304 só pra GET de páginas dinâmicas (HTML), nunca pra refresh forçado.
    if (request.method == "GET"
            and not request.path.startswith("/static/")
            and request.args.get("refresh") != "1"
            and (response.content_type or "").startswith("text/html")
            and not response.direct_passthrough
            and 200 <= response.status_code < 300):
        # add_etag faz md5 do body; make_conditional troca pra 304 se If-None-Match bate.
        response.add_etag()
        response.headers["Cache-Control"] = "private, must-revalidate"
        response.make_conditional(request)
        if response.status_code == 304:
            return response  # body vazio — pula gzip

    # Gzip somente para text-like content e payload >500 bytes
    accept = request.headers.get("Accept-Encoding", "")
    if "gzip" not in accept:
        return response
    ct = response.content_type or ""
    if not (ct.startswith("text/") or ct.startswith("application/javascript")
            or ct.startswith("application/json")):
        return response
    if response.direct_passthrough:
        return response
    data = response.get_data()
    if len(data) < 500:
        return response
    import gzip, io
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz:
        gz.write(data)
    response.set_data(buf.getvalue())
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = str(len(response.get_data()))
    response.headers["Vary"] = "Accept-Encoding"
    return response


@app.errorhandler(Exception)
def on_error(e):
    import traceback
    return f"<pre style='padding:20px'>{traceback.format_exc()}</pre>", 500


def _warm_all(force: bool = False):
    """Aquece os caches dos dois níveis (parquet + memória). force=True re-consulta
    o BQ e reaquece a memória (usado pelo refresher). Aquece cada dataset isolado
    pra que uma falha não impeça os demais."""
    def _try(fn, *a, **kw):
        try:
            fn(*a, **kw)
        except Exception as e:  # noqa: BLE001 — best-effort por dataset
            print(f"pepeuhacker · warm {getattr(fn,'__name__',fn)} falhou: {e}", flush=True)
    _try(_get_clean_ae_perf, force=force)
    _try(_get_ae_perf_with_managers, force=force)
    _try(_get_demand_deals, force=force)
    _try(_get_pipeline_deals, force=force)
    _try(_get_forecast_deals, force=force)
    # Aba Legal: só o lado BARATO (7 MB varridos). `legal_mail_touch_raw` fica
    # FORA de propósito — são ~25 GB por execução, e este refresher roda de 45
    # em 45 min com force=True: aquecê-la aqui custaria ~800 GB/dia pra um sinal
    # que não muda de hora em hora. Ela popula sozinha no 1º acesso à aba, com
    # TTL de 6h (ver LEGAL_MAIL_TTL).
    _try(bq.legal_deals_raw, force_refresh=force)
    # /confirm é live BQ (cache 5min) — aquece o range default da rota
    _try(bq.confirm_totals,
         _quarter_start_of(date(2025, 4, 1)).isoformat(),
         _quarter_start_of(date(2026, 1, 31)).isoformat(),
         force_refresh=force)


def _prewarm():
    """Carrega caches em memória ao subir. Sem isso, o primeiro request da
    sessão paga ~50-150ms (parquet read + overrides) ou ~6-11s (BQ se parquet
    expirou). Pré-aquece silenciosamente — falha não impede subir."""
    _warm_all(force=False)
    print("pepeuhacker · cache pré-aquecido", flush=True)


def _access_urls():
    """URLs por onde o time acessa este mesmo servidor. O IP da máquina é DHCP
    (muda a cada renovação de lease), então o link por IP QUEBRA sozinho — foi o
    que aconteceu com o link antigo dos diretores (10.8.160.226). O nome mDNS
    (`<LocalHostName>.local`) acompanha o IP automaticamente na mesma rede e é o
    endereço que deve ser compartilhado."""
    port = os.environ.get("PORT", "8080")
    out = {"port": port, "mdns": None, "ip": None, "public": None}
    # URL pública do túnel (tunnel.sh grava; some do arquivo quando o túnel cai).
    # É o único endereço que serve diretor em OUTRO local — o mDNS exige mesma LAN.
    # Quick tunnel = URL nova a cada reinício, então ela é lida do disco a cada
    # request em vez de memorizada.
    try:
        _tf = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "cache", "tunnel_url.txt")
        with open(_tf, encoding="utf-8") as fh:
            out["public"] = fh.read().strip() or None
    except OSError:
        pass
    try:
        name = subprocess.run(["scutil", "--get", "LocalHostName"],
                              capture_output=True, text=True, timeout=3).stdout.strip()
        if name:
            out["mdns"] = f"http://{name}.local:{port}"
    except Exception:      # noqa: BLE001 — nome é opcional, não pode derrubar o boot
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        s.connect(("10.255.255.255", 1))   # não envia pacote; só resolve a rota de saída
        out["ip"] = f"http://{s.getsockname()[0]}:{port}"
        s.close()
    except Exception:      # noqa: BLE001
        pass
    return out


@app.route("/link")
def access_link():
    """Página de 1 tela com o endereço ATUAL pra compartilhar com os diretores.
    Existe porque o IP muda: em vez de adivinhar, abre /link e copia."""
    u = _access_urls()
    return render_template("link.html", urls=u, ctx=base_context())


def _snapshot_forecast_week():
    """Grava o snapshot semanal do forecast do último domingo, se ainda não
    existir. Best-effort: NUNCA pode derrubar o app nem o reaquecimento — se o
    BigQuery recusar (permissão, cota), o dashboard segue funcionando e só a
    série ponderada futura deixa de ganhar mais um ponto.

    DEIXOU DE SER A FONTE DA TELA em 31/08/2026: descobrimos que o próprio BI
    já guarda uma cópia DIÁRIA da curated em `materialized_data_source`, com
    forecast category e close date as-of desde 23/03/2026 (ver
    `bq.forecast_asof_series`) — o Histórico do Forecast lê de lá. Esta tabela
    continua sendo gravada por dois motivos: ela é a validação independente da
    outra fonte (nos domingos 23/08 e 30/08/2026 o ponderado bate na 3ª casa) e
    é a garantia de que a série sobrevive se o BI parar de materializar, como já
    aconteceu com `close_date_history_df`. Custo: um INSERT por semana.

    A validação bate exato nos domingos capturados NO DIA (23/08 e 30/08/2026:
    R$ 2.374.652,04 e R$ 2.301.495,12 nas duas fontes). No de 16/08 há 2,1% de
    diferença — e a culpada é esta tabela, não a do BI: a captura dela só rodou
    em 18/08, dois dias depois, quando 17 deals já tinham mudado."""
    try:
        last_sun, _ = _last_two_sundays()
        n = bq.forecast_snapshot_write(last_sun.isoformat(), _fc_open_stage_ids())
        if n:
            print(f"pepeuhacker · snapshot do forecast {last_sun}: {n} linhas",
                  flush=True)
    except Exception as e:  # noqa: BLE001 — best-effort, ver docstring
        print(f"pepeuhacker · snapshot do forecast falhou ({e})", flush=True)


def _snapshot_forecast_midnight() -> None:
    """Grava a foto das 00h BRT de HOJE, se ainda não existe e se ainda dá tempo
    de ela ser "a meia-noite" (ver `_FCD_CAPTURA_MAX_ATRASO`). Best-effort, como
    o semanal: falha aqui só fura um dia do DoD, que cai na aproximação do BI."""
    agora = datetime.now(BRT)
    virada = agora.replace(hour=0, minute=0, second=0, microsecond=0)
    if agora - virada > _FCD_CAPTURA_MAX_ATRASO:
        return
    # Os dois funis, no MESMO instante (pedido do Pedro, 12/10/2026: "pode ser no
    # mesmo horário"). Um por vez e cada um no seu try: o México falhar não pode
    # custar a foto do Brasil, que é a série mais antiga.
    for pl in (bq.SALES_BR, _MX_PIPELINE):
        try:
            n = bq.forecast_daily_write(virada.date().isoformat(), _fc_open_stage_ids(),
                                        won_stage_id=_HS_WON_STAGE,
                                        lost_stage_id=_HS_LOST_STAGE, pipeline=pl)
            if n:
                bq.forecast_daily_snapshots(pipeline=pl, force_refresh=True)
                print(f"pepeuhacker · foto da meia-noite {pl} {virada:%d/%m}: {n} "
                      f"linhas ({agora:%H:%M:%S} BRT)", flush=True)
        except Exception as e:  # noqa: BLE001 — best-effort, ver docstring
            print(f"pepeuhacker · foto da meia-noite {pl} falhou ({e})", flush=True)


def _midnight_loop() -> None:
    """Dorme até a próxima 00:00 BRT e tira a foto. Laço próprio, e não o
    `_refresh_loop`: aquele gira de 15 em 15 min e pula volta quando o
    reaquecimento demora, então a foto sairia em qualquer ponto até 00h15+ — e o
    DoD promete meia-noite. A 1ª chamada, no boot, cobre um deploy que caia
    entre 00h e 02h."""
    _snapshot_forecast_midnight()
    while True:
        agora = datetime.now(BRT)
        prox = (agora + timedelta(days=1)).replace(hour=0, minute=0, second=5,
                                                     microsecond=0)
        time.sleep(max(1.0, (prox - agora).total_seconds()))
        _snapshot_forecast_midnight()


# Estado do frescor, preenchido pelo `_refresh_loop` e lido pelo selo das telas.
# (epoch da última gravação NA FONTE, epoch do último reaquecimento daqui)
_FRESH_LOCK = threading.Lock()
_FRESH = {"source_epoch": 0.0, "warmed_epoch": 0.0,
          # da sonda de dev (ver _fresh_probe_async)
          "probed_epoch": 0.0, "probing": False}


def _fresh_probe_async():
    """Lê o frescor da fonte FORA do request. Em produção quem mantém o valor em
    dia é o `_refresh_loop`; isto cobre o caso em que ele não está rodando —
    `flask run` do dev, que nem chega no bloco `__main__`. Dedup por flag: uma
    sondagem por vez, e nenhum request espera por ela."""
    with _FRESH_LOCK:
        if _FRESH["probing"] or time.time() - _FRESH["probed_epoch"] < 900:
            return
        _FRESH["probing"] = True

    def _run():
        try:
            src = bq.hs_data_last_modified()
            with _FRESH_LOCK:
                if src:
                    _FRESH["source_epoch"] = src
                _FRESH["probed_epoch"] = time.time()
        except Exception as e:  # noqa: BLE001 — selo nunca derruba tela
            print(f"pepeuhacker · sonda de frescor falhou ({e})", flush=True)
        finally:
            with _FRESH_LOCK:
                _FRESH["probing"] = False

    threading.Thread(target=_run, name="fresh-probe", daemon=True).start()


def source_freshness() -> dict:
    """O que o selo das telas mostra: idade do DADO, não idade do cache.

    A diferença importa. O cache pode ter 1 minuto de vida e conter um funil de
    3 dias atrás — foi exatamente o que aconteceu entre 11 e 14/09/2026, quando
    a tabela legada do HubSpot parou de receber sync e o dashboard seguiu
    mostrando número velho com cara de número novo. Quem olha a tela em fim de
    trimestre precisa saber de QUANDO é o dado."""
    _fresh_probe_async()
    with _FRESH_LOCK:
        src, warm = _FRESH["source_epoch"], _FRESH["warmed_epoch"]
    if not src:
        return {"ok": False}
    age_min = int((time.time() - src) / 60)
    return {
        "ok": True,
        # Em BRT: o selo é lido por gente em São Paulo, e num container UTC
        # ele mostrava hora à frente do relógio de quem olhava.
        "source_dt": datetime.fromtimestamp(src, BRT),
        "source_age_min": age_min,
        "warmed_dt": datetime.fromtimestamp(warm, BRT) if warm else None,
        # A ingestão do HubSpot roda de ~2 em 2h, com pausa de madrugada. O
        # limiar é 4h — dois ciclos — pra um atraso isolado não acender o alerta
        # à toa: alarme que toca todo dia é alarme que ninguém olha. De
        # madrugada e no fim de semana ele acende mesmo, e está certo: o dado é
        # velho de verdade, e quem abre a tela às 8h precisa saber disso.
        "stale": age_min > 240,
    }


def _refresh_loop(interval: int = 900):
    """Reaquece os caches principais a cada `interval`s — 15 min desde
    14/09/2026 (antes: 45), pedido do Pedro pra operar o fim de trimestre.

    O laço é DISPARADO POR MUDANÇA, não por relógio: a cada volta pergunta ao
    BigQuery quando a tabela de deals do HubSpot foi gravada pela última vez
    (`bq.hs_source_last_modified`, metadado, 0 byte varrido) e só re-consulta as
    queries caras se a fonte de fato mudou. Por que assim: a ingestão do HubSpot
    roda de ~2 em 2 horas, então um ciclo cego de 15 min pagaria 8 rodadas de
    BigQuery pra reescrever parquet idêntico. Com o gate, o dado novo chega à
    tela em no máximo 15 min e o custo fica no mesmo patamar do ciclo de 45.

    Junto com o stale-while-revalidate do bq.query, nenhuma visita — nem a
    primeira do dia — paga o round-trip do BigQuery.

    Também é aqui que o snapshot semanal do forecast é tentado: o laço já roda
    sempre, então não precisa de um 2º agendador só pra isso, e a gravação é
    idempotente por domingo."""
    _snapshot_forecast_week()          # 1ª tentativa já no boot
    last_src = 0.0
    while True:
        try:
            # Duas perguntas diferentes, de propósito (ver as docstrings das duas
            # em bq.py): o GATE pergunta se alguma fonte foi gravada — metadado,
            # 0 byte, pode ser generoso e reaquecer à toa; o SELO pergunta de
            # quando é o dado que está lá dentro, que é o que a tela promete.
            src = bq.hs_source_last_modified()
            mudou = src > last_src
            if mudou:
                _warm_all(force=True)
                last_src = src
            dado = bq.hs_data_last_modified()
            with _FRESH_LOCK:
                # Fonte parada também registra: se o selo sumisse, sumiria
                # justamente quando o dado está envelhecendo e alguém precisa ver.
                _FRESH["source_epoch"] = dado or _FRESH["source_epoch"]
                if mudou:
                    _FRESH["warmed_epoch"] = time.time()
            if mudou:
                print(f"pepeuhacker · fonte nova (dado de "
                      f"{datetime.fromtimestamp(dado, BRT):%d/%m %H:%M} BRT)"
                      " — caches reaquecidos (bg)", flush=True)
            _snapshot_forecast_week()
        except Exception as e:  # noqa: BLE001 — o refresher NUNCA pode morrer
            print(f"pepeuhacker · refresher falhou ({e}); segue no próximo ciclo",
                  flush=True)
        time.sleep(interval)


# ============================================================
# Sales MX — dashboard do time do México
# ============================================================
# Espelha o Sales AE BR, com três diferenças que NÃO são cosméticas:
#
#   1) o funil é `Sales MX` (query própria em bq.mx_deals_raw);
#   2) TUDO é ARR em dólar. O BR alterna MRR R$ / ARR US$ num toggle; aqui não
#      há toggle, porque o time é medido em dólar e um deal em peso não tem
#      leitura em real. A conversão de moeda acontece na query (ver a nota longa
#      em bq.mx_deals_raw), então o que chega aqui já é MRR USD e só falta ×12;
#   3) o roster é chaveado pelo nome EXATO do BigQuery, com o nome de exibição à
#      parte. Três dos sete chegaram com grafia diferente da que o time usa no
#      Slack — "Santiago Oriol" é "Santiago de Oriol" na base, "Viri Garza" é
#      "Viridiana Garza" e "Adrián Hernández" está sem acento nenhum. Casar pelo
#      nome bonito daria zero calado, que foi exatamente o caso do Diego Loures.
MX_LEADER = "Mauricio Del Hierro"

# Estágios do funil MX. São OUTROS ids: o México é um pipeline separado no
# HubSpot e não compartilha nenhum id com o BR (lá é 'closedwon'/'appointment...',
# aqui é numérico). Ordem = ordem do funil, com No Show entre Scheduled e
# Qualifying, igual ao BR.
MX_STAGES = [
    ("8352441",  "Scheduled"),
    ("61414206", "No Show"),
    ("8352512",  "Qualifying"),
    ("94159788", "Scoping"),
    ("8352513",  "Negotiation"),
    ("89673855", "Decision"),
    ("66011247", "Procurement"),
]
MX_STAGE_LABEL = dict(MX_STAGES)
MX_STAGE_ORDER = {sid: i for i, (sid, _) in enumerate(MX_STAGES)}
MX_WON_STAGE, MX_LOST_STAGE = "13023548", "8352516"

# nome no BigQuery -> {display: como o time chama, tier: segmento}
MX_ROSTER = {
    "Santiago de Oriol": {"display": "Santiago Oriol",   "tier": "Enterprise"},
    "Sabina Diaz":       {"display": "Sabina Diaz",      "tier": "Mid Market"},
    "Viridiana Garza":   {"display": "Viri Garza",       "tier": "Account Executive"},
    "Adrian Hernandez":  {"display": "Adrián Hernández", "tier": "Account Executive"},
    "Adolfo Mora":       {"display": "Adolfo Mora",      "tier": "Enterprise"},
    "Armando Roman":     {"display": "Armando Roman",    "tier": "Mid Market"},
    "Luis Terrazas":     {"display": "Luis Terrazas",    "tier": "Mid Market"},
}
MX_DISPLAY = {k: v["display"] for k, v in MX_ROSTER.items()}
# Único gestor do país: os 7 respondem a ele, e ele ao VP (ver ORG_CHART).
MX_LEADER_BY_AE = {ae: MX_LEADER for ae in MX_ROSTER}
# O gestor do México fecha deal próprio (US$ 540 mil desde 2025), mas NÃO é
# vendedor: sem isto ele ganhava barra no ranking, card no Funil e linha na
# matriz do PoP, competindo com o time que ele lidera. Mesma regra que
# NON_AE_USERS já aplica aos diretores brasileiros.
NON_AE_USERS_MX = {MX_LEADER}

_MX_ROSTER_CACHE: dict = {}


# Meta do México por trimestre, em ARR USD, por vendedor. Fonte: o painel de
# Coverage do Mauricio (coluna cinza, print do Pedro em 12/10/2026 — "meta do
# México é a coluna em cinza"). Substitui a tabela do BI a partir do Q4/26: lá
# só 2 dos 7 vendedores tinham meta, e os outros 5 apareciam com zero.
# Trimestre novo sem entrada aqui segue o último declarado (mesma regra do
# roster do Brasil); trimestre ANTERIOR ao primeiro declarado segue a tabela do
# BI, porque o pedido foi "Q4 pra frente".
MX_QUOTAS_ARR_USD = {
    date(2026, 10, 1): {
        "Armando Roman":     106_300,
        "Adolfo Mora":       300_000,
        "Sabina Diaz":        88_500,
        "Viridiana Garza":   177_100,
        "Adrian Hernandez":  212_500,
        "Luis Terrazas":     212_500,
        "Santiago de Oriol": 300_000,
    },
}


def _mx_quota_declarada(q) -> dict | None:
    """{AE: quota MRR USD} do trimestre `q` pelo registro acima, ou None se `q`
    é anterior ao primeiro trimestre declarado. ARR ÷ 12 porque o roster guarda
    meta na unidade dos `amount` do México (MRR USD)."""
    if q is None:
        return None
    validos = [k for k in MX_QUOTAS_ARR_USD if k <= q]
    if not validos:
        return None
    return {ae: v / MONTHS_PER_YEAR for ae, v in MX_QUOTAS_ARR_USD[max(validos)].items()}


def _mx_roster(q) -> dict:
    """Roster do México no trimestre `q`, no formato dos rosters do Brasil.

    Existe pra o México ter o MESMO universo de vendedores que o BR tem: sem uma
    lista, o ranking só mostrava quem teve deal na janela (5 de 7 no Q3), e quem
    não vendeu sumia — que é justamente a informação que um ranking de meta
    precisa dar. Com a lista, os 7 aparecem, e quem está sem meta cadastrada
    aparece com quota 0 em vez de sumir.

    Quota em MRR USD — a mesma unidade dos `amount` do México, que a query
    converte na volta. Do Q4/26 em diante vem de `MX_QUOTAS_ARR_USD` (o painel do
    Mauricio); antes, da tabela do BI (`sales_rep_monthly_quota`, região MX),
    onde só 2 dos 7 tinham meta.

    O quarter é o pedido (o da janela, via `_roster_of`), não o de hoje: a
    partir de 01/10/2026 o "quarter de hoje" passou a trazer a quota do Q4 pra
    medir o Q3. A lista de pessoas é a mesma em todo trimestre."""
    if q in _MX_ROSTER_CACHE:
        return _MX_ROSTER_CACHE[q]
    quotas = _mx_quota_declarada(q)
    if quotas is None:      # antes do Q4/26: a tabela do BI, como sempre foi
        quotas = {}
        try:
            qdf = bq.mx_quota()
            if qdf is not None and len(qdf):
                qdf = qdf[pd.to_datetime(qdf["quarter"]).dt.date == q]
                for r in qdf.itertuples(index=False):
                    ae = getattr(r, "account_executive", None)
                    if ae:
                        quotas[ae] = float(getattr(r, "quota_usd_mrr", 0) or 0)
        except Exception:
            pass
    out = {ae: {"leader": MX_LEADER, "tier": meta["tier"],
                "quota": quotas.get(ae, 0.0), "new_logos": 0}
           for ae, meta in MX_ROSTER.items()}
    _MX_ROSTER_CACHE[q] = out
    return out


def _mx_deals(force: bool = False) -> pd.DataFrame:
    """Deals do Sales MX restritos ao roster atual, com `arr` em USD.

    O funil MX tem 52 nomes historicamente; o roster são os 7 de hoje. Quem saiu
    fica de fora — mesma política do BR, e sem isso o histórico de um vendedor
    que não existe mais entraria no total do time."""
    df = bq.mx_deals_raw(force_refresh=force).copy()
    df = df[df["account_executive"].isin(MX_ROSTER)]
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["arr"] = df["amount"] * MONTHS_PER_YEAR      # MRR USD -> ARR USD
    df["display"] = df["account_executive"].map(MX_DISPLAY)
    for c in ("close_date", "po_emission_date", "created_date"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    # Data da RECEITA REALIZADA (validação com a extensão de fechamento): só as
    # somas de realizado leem esta coluna; contagem e ticket seguem o calendário.
    df["rev_date"] = _rev_date(df["po_emission_date"].fillna(df["close_date"]))
    return df


def _mx_quota_arr(quarter) -> dict:
    """Quota do trimestre por AE, em ARR USD. Só quem tem linha na tabela do BI —
    hoje 2 dos 7 (Adolfo e Santiago); os outros aparecem SEM meta, e a tela diz
    isso em vez de fingir meta zero (que leria como 'bateu infinito%')."""
    try:
        q = bq.mx_quota()
    except Exception:
        return {}
    if not len(q):
        return {}
    q = q[pd.to_datetime(q["quarter"]).dt.date == quarter]
    out = {}
    for r in q.itertuples(index=False):
        ae = getattr(r, "account_executive", None)
        if ae in MX_ROSTER:
            out[ae] = float(getattr(r, "quota_usd_mrr", 0) or 0) * MONTHS_PER_YEAR
    return {k: v for k, v in out.items() if v > 0}


def _mx_realized_official(quarter) -> dict:
    """Realizado do trimestre por AE em ARR USD, direto da curada
    `account_executives_performance` (`new_arr_in_usd`) — o número OFICIAL do BI.

    Existe separado da soma deal-a-deal porque os dois DISCORDAM, e muito: no
    Q2/2026 o funil soma US$ 325.688 pro Adolfo contra US$ 222.455 da curada
    (+46%), e US$ 72.310 contra US$ 34.624 pro Adrian (+109%). Os deals do funil
    são todos legítimos (closed-won, em dólar, com PO no trimestre), então a
    diferença é de REGRA e não de dado — provavelmente o mesmo tipo de exclusão
    que o BR trata via on-hold, ou uma data de atribuição diferente. Enquanto
    isso não for reconciliado com o time de BI, a tela mostra as duas e diz qual
    é qual: sumir com uma delas seria escolher um número sem base pra escolher."""
    try:
        raw = bq.ae_performance()
    except Exception:
        return {}
    if raw is None or not len(raw):
        return {}
    df = raw.copy()
    df["quarter"] = pd.to_datetime(df["quarter"]).dt.date
    df = df[(df["quarter"] == quarter) & df["account_executive"].isin(MX_ROSTER)]
    if "new_arr_in_usd" not in df.columns:
        return {}
    return {r.account_executive: float(r.new_arr_in_usd or 0.0)
            for r in df.itertuples(index=False)}


def _mx_window(f):
    """Janela da tela: o filtro de período, ou o trimestre corrente."""
    today = _today()
    lo = pd.to_datetime(f["date_from"]).date() if f.get("date_from") else _quarter_start_of(today)
    hi = pd.to_datetime(f["date_to"]).date() if f.get("date_to") else _quarter_end_of(today)
    return lo, hi


@app.route("/mx/overview")
def mx_overview():
    """Sales MX · Overview — realizado × meta por vendedor e pipeline aberto,
    tudo em ARR USD."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"
    lo, hi = _mx_window(f)
    lo_ts, hi_ts = pd.Timestamp(lo), pd.Timestamp(hi)

    df = _mx_deals(force=force)
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]

    # Realizado = deal ganho com PO emitida na janela; se não houver PO, cai no
    # close date — no MX a PO não é preenchida em todo deal, e exigir PO como no
    # BR esconderia ganho real.
    # Janela pela data da RECEITA (validação com a extensão de fechamento). Os
    # "N deals" e o ticket desta tela são a composição de "Ganhos no funil" (o
    # card e a coluna ao lado, e o drill deles), então saem da mesma população.
    won = df[df["is_won"]].copy()
    won = won[won["rev_date"].notna() & (won["rev_date"] >= lo_ts) & (won["rev_date"] <= hi_ts)]
    # Pipeline aberto = close date na janela; deal sem close date entra à parte,
    # senão metade do funil sumiria sem aviso (mesmo problema do Pipeline Total).
    op = df[df["is_open"]]
    pipe = op[op["close_date"].notna() & (op["close_date"] >= lo_ts) & (op["close_date"] <= hi_ts)]
    pipe_sem_data = op[op["close_date"].isna()]

    q_ini = _quarter_start_of(lo)
    quota = _mx_quota_arr(q_ini)
    oficial = _mx_realized_official(q_ini)
    # O ranking usa o número OFICIAL quando ele existe: é contra ele que a
    # diretoria compara. O deal-a-deal vira coluna ao lado, com o drill.
    rows = []
    for ae, meta in MX_ROSTER.items():
        w = won[won["account_executive"] == ae]
        p = pipe[pipe["account_executive"] == ae]
        funil = float(w["arr"].sum())
        of = oficial.get(ae)
        realized = of if of is not None else funil
        qt = quota.get(ae)
        rows.append({
            "account_executive": ae, "display": meta["display"], "tier": meta["tier"],
            "realized": realized, "funil": funil, "tem_oficial": of is not None,
            "deals": int(len(w)),
            "ticket": (funil / len(w)) if len(w) else 0.0,
            "pipe": float(p["arr"].sum()), "pipe_deals": int(len(p)),
            "quota": qt or 0.0, "has_quota": qt is not None,
            "attainment": (realized / qt) if qt else None,
        })
    rows.sort(key=lambda r: (r["attainment"] is not None,
                            r["attainment"] if r["attainment"] is not None else -1.0,
                            r["realized"]), reverse=True)

    _drill_base = {k: v for k, v in (("date_from", f["date_from"]),
                                     ("date_to", f["date_to"])) if v}
    rank = _build_rank_chart(
        [{"name": r["account_executive"], "short": short_name(r["display"]),
          "leader": MX_LEADER, "realized": r["realized"], "quota": r["quota"],
          "attainment": r["attainment"]} for r in rows],
        _drill_base, axis_mode="usd", drill_path="/mx/overview")

    total_funil = float(won["arr"].sum())
    total_real = float(sum(r["realized"] for r in rows))
    total_quota = float(sum(v for v in quota.values()))
    divergencia = total_real - total_funil
    return render_template(
        "mx_overview.html", ctx=ctx, rows=rows, rank=rank,
        leader=MX_LEADER,
        range_label=_format_period(f, ctx["presets"], ctx["active_preset"]),
        total_real=total_real, total_funil=total_funil, divergencia=divergencia,
        total_deals=int(len(won)),
        total_ticket=(total_funil / len(won)) if len(won) else 0.0,
        total_quota=total_quota,
        total_att=(total_real / total_quota) if total_quota else None,
        total_pipe=float(pipe["arr"].sum()), pipe_deals=int(len(pipe)),
        pipe_sem_data=float(pipe_sem_data["arr"].sum()),
        pipe_sem_data_deals=int(len(pipe_sem_data)),
        sem_quota=[r["display"] for r in rows if not r["has_quota"]],
    )


# Dimensões de corte do pipeline MX. Mesmo papel do `_PT_DIMS` do BR (filtro E
# empilhamento pelo mesmo registro), mas com os valores que o México realmente
# usa — reaproveitar o registro do BR traria "Expansion" como source, que aqui
# quase não existe, e deixaria de fora o que existe.
MX_DIMS = {
    "dtype": {"col": "deal_type", "label": "Deal Type", "values": [
        ("acq", "Aquisition",      "Aquisição", "#2563EB", "#1D4ED8"),
        ("exp", "Up & cross sell", "Expansão",  "#047857", "#065F46"),
    ]},
    "source": {"col": "deal_source_agg", "label": "Source", "values": [
        ("inbound",   "Inbound",   "Inbound",   "#10B981", "#047857"),
        ("outbound",  "Outbound",  "Outbound",  "#6366F1", "#4338CA"),
        ("expansion", "Expansion", "Expansion", "#0891B2", "#0E7490"),
    ]},
    "fcat": {"col": "forecast_category", "label": "Forecast Category", "values": [
        ("commit",   "COMMIT",    "100%",          "#047857", "#065F46"),
        ("bestcase", "BEST_CASE", "75%",           "#10B981", "#047857"),
        ("probable", "PROBABLE",  "50%",           "#2563EB", "#1D4ED8"),
        ("funnel",   "PIPELINE",  "25%",           "#93C5FD", "#1E40AF"),
        ("omit",     "OMIT",      "0%",            "#F59E0B", "#B45309"),
        ("nocat",    None,        "Sem categoria", "#94A3B8", "#475569"),
    ]},
}


def _mx_dim_meta(dim):
    out = []
    for k, raw, lbl, c, ink in MX_DIMS[dim]["values"]:
        item = {"key": k, "raw": raw, "label": lbl, "color": c, "ink": ink}
        if dim == "fcat":
            # Peso efetivo (com DIR_FORECAST_FACTOR) — mesma convenção do BR.
            item["pct"] = (FORECAST_CATEGORY_PCT_EFF.get(raw)
                           if raw is not None else None)
        out.append(item)
    return out


def _mx_dim_key(df, dim):
    """Coluna crua -> chave curta, com o que não mapeia caindo no bucket vazio.
    Garante soma dos segmentos == total da barra (mesma invariante do BR)."""
    col = MX_DIMS[dim]["col"]
    lookup = {raw: k for k, raw, *_ in MX_DIMS[dim]["values"] if raw is not None}
    return df[col].map(lookup).fillna("nocat")


def _mx_dim_filter(df, dim, sel):
    if not sel:
        return df
    entry = next((v for v in MX_DIMS[dim]["values"] if v[0] == sel), None)
    if entry is None:
        return df
    col, raw = MX_DIMS[dim]["col"], entry[1]
    if raw is None:
        return df[df[col].isna() | (df[col].astype(str).str.strip() == "")]
    return df[df[col] == raw]


def _mx_stage_rows(df, by=""):
    """Uma linha por estágio do funil MX, na ordem do funil. Estágio sem pipe
    continua na lista (barra vazia) — sumir daria a impressão de funil menor."""
    if by:
        df = df.copy()
        df["_seg"] = _mx_dim_key(df, by)
    segs = _mx_dim_meta(by) if by else []
    rows = []
    for sid, label in MX_STAGES:
        g = df[df["dealstage"] == sid]
        row = {"sid": sid, "label": label,
               "arr": float(g["arr"].sum()), "deals": int(len(g))}
        if by:
            row["segs"] = {s["key"]: float(g.loc[g["_seg"] == s["key"], "arr"].sum())
                           for s in segs}
        rows.append(row)
    if by:
        live = set(df["_seg"].unique())
        segs = [s for s in segs if s["key"] in live]
    return rows, segs


@app.route("/mx/pipeline")
def mx_pipeline():
    """Sales MX · Pipeline — ARR aberto por estágio do funil, cortável por deal
    type, source e forecast category."""
    ctx = base_context()
    f = ctx["filters"]
    df = _mx_deals(force=request.args.get("refresh") == "1")
    df = df[df["is_open"]]
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]
    for dim in MX_DIMS:
        df = _mx_dim_filter(df, dim, f.get(dim, ""))

    by = request.args.get("by", "")
    if by not in MX_DIMS:
        by = ""
    rows, segs = _mx_stage_rows(df, by)
    total = float(df["arr"].sum())
    keep = {k: v for k, v in f.items() if v and k in ("ae", "dtype", "source", "fcat")}

    seg_totals = []
    if by and len(df):
        key = _mx_dim_key(df, by)
        for sdef in segs:
            amt = float(df.loc[key == sdef["key"], "arr"].sum())
            n = int((key == sdef["key"]).sum())
            if n:
                seg_totals.append({**sdef, "arr": amt, "deals": n,
                                   "share": (amt / total) if total else 0.0})
        seg_totals.sort(key=lambda r: r["arr"], reverse=True)

    return render_template(
        "mx_pipeline.html", ctx=ctx, rows=rows, segs=segs, by=by, keep=keep,
        seg_totals=seg_totals, total=total, deals=int(len(df)),
        max_arr=max([r["arr"] for r in rows], default=0.0),
        dims=[{"key": k, "label": v["label"]} for k, v in MX_DIMS.items()],
        leader=MX_LEADER,
        sem_close=int(df["close_date"].isna().sum()),
    )


@app.route("/mx/forecast")
def mx_forecast():
    """Sales MX · Forecast — pipeline aberto com close date no período, ponderado
    pelo peso da forecast category."""
    ctx = base_context()
    f = ctx["filters"]
    lo, hi = _mx_window(f)
    lo_ts, hi_ts = pd.Timestamp(lo), pd.Timestamp(hi)
    df = _mx_deals(force=request.args.get("refresh") == "1")
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]
    pipe = df[df["is_open"] & df["close_date"].notna()
              & (df["close_date"] >= lo_ts) & (df["close_date"] <= hi_ts)].copy()

    cats = _mx_dim_meta("fcat")
    pipe["_cat"] = _mx_dim_key(pipe, "fcat")
    bruto = float(pipe["arr"].sum())
    linhas, ponderado, sem_peso = [], 0.0, 0.0
    for c in cats:
        sub = pipe[pipe["_cat"] == c["key"]]
        if not len(sub):
            continue
        amt = float(sub["arr"].sum())
        w = c.get("pct")
        pond = (amt * w) if w is not None else None
        if pond is None:
            sem_peso += amt
        else:
            ponderado += pond
        linhas.append({**c, "arr": amt, "deals": int(len(sub)), "ponderado": pond,
                       "share": (amt / bruto) if bruto else 0.0})
    linhas.sort(key=lambda r: r["arr"], reverse=True)

    # Realizado do período pela mesma regra do Overview, pra a projeção fechar.
    # Realizado = receita: janela pela data com a extensão de fechamento.
    won = df[df["is_won"]].copy()
    won = won[won["rev_date"].notna() & (won["rev_date"] >= lo_ts) & (won["rev_date"] <= hi_ts)]

    return render_template(
        "mx_forecast.html", ctx=ctx, linhas=linhas, bruto=bruto,
        ponderado=ponderado, sem_peso=sem_peso,
        cobertura=(1 - sem_peso / bruto) if bruto else 0.0,
        deals=int(len(pipe)), realizado=float(won["arr"].sum()),
        projecao=float(won["arr"].sum()) + ponderado, leader=MX_LEADER,
        range_label=_format_period(f, ctx["presets"], ctx["active_preset"]),
    )


@app.route("/mx/funnel")
def mx_funnel():
    """Sales MX · Funil — conversão de estágio a estágio e win rate.

    A conversão é medida sobre deals que JÁ SAÍRAM do estágio (ganho, perdido ou
    avançado). Deal ainda parado no estágio não conta como falha de conversão —
    ele só não decidiu ainda, e contá-lo puxaria a taxa pra baixo todo dia."""
    ctx = base_context()
    f = ctx["filters"]
    lo, hi = _mx_window(f)
    df = _mx_deals(force=request.args.get("refresh") == "1")
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]
    for dim in ("dtype", "source"):
        df = _mx_dim_filter(df, dim, f.get(dim, ""))
    # Coorte por criação: um funil precisa de uma coorte, senão mistura deal de
    # 2022 com deal de ontem e a taxa não descreve período nenhum.
    if f.get("date_from") or f.get("date_to"):
        cd = df["created_date"]
        df = df[cd.notna() & (cd >= pd.Timestamp(lo)) & (cd <= pd.Timestamp(hi))]

    # "Alcançou o estágio" vem da DATA DE ENTRADA, não do estágio atual.
    # A 1ª versão usava o estágio atual e os perdidos sumiam do denominador (o
    # estágio atual deles é "Lost"): dava 383 no topo do funil com 760 perdidos
    # na base, e conversões infladas. Com a data de entrada, o deal perdido conta
    # em todas as etapas por onde passou, que é como uma taxa de conversão tem
    # que ser lida.
    ETAPAS = [
        ("hs_date_entered_scheduled",       "Scheduled"),
        ("hs_date_entered_qualification",   "Qualifying"),
        ("hs_date_entered_project_scoping", "Scoping"),
        ("hs_date_entered_negotiation",     "Negotiation"),
        ("hs_date_entered_consensus",       "Decision"),
        ("hs_date_entered_procurement",     "Procurement"),
    ]
    won = df[df["dealstage"] == MX_WON_STAGE]
    reached = {col: df[pd.to_datetime(df[col], errors="coerce").notna()]
               for col, _ in ETAPAS}

    rows = []
    for i, (col, label) in enumerate(ETAPAS):
        atual = reached[col]
        prox = reached[ETAPAS[i + 1][0]] if i + 1 < len(ETAPAS) else won
        n, nn = len(atual), len(prox)
        rows.append({
            "sid": col, "label": label,
            "deals": n, "arr": float(atual["arr"].sum()),
            "next_deals": nn,
            "conv": (nn / n) if n else None,
            "next_label": (ETAPAS[i + 1][1] if i + 1 < len(ETAPAS) else "Ganho"),
        })
    topo = rows[0]["deals"] if rows else 0
    perdidos = df[df["dealstage"] == MX_LOST_STAGE]
    return render_template(
        "mx_funnel.html", ctx=ctx, rows=rows, leader=MX_LEADER,
        topo=topo, won_deals=int(len(won)), won_arr=float(won["arr"].sum()),
        lost_deals=int(len(perdidos)),
        win_rate=(len(won) / (len(won) + len(perdidos))) if (len(won) + len(perdidos)) else None,
        e2e=(len(won) / topo) if topo else None,
        range_label=_format_period(f, ctx["presets"], ctx["active_preset"]),
        tem_coorte=bool(f.get("date_from") or f.get("date_to")),
    )


@app.route("/mx/coverage")
def mx_coverage():
    """Sales MX · Cobertura — quanto de pipeline existe pra cada dólar que falta
    da meta. Olha pra FRENTE: o que já foi ganho abate a meta."""
    ctx = base_context()
    f = ctx["filters"]
    lo, hi = _mx_window(f)
    lo_ts, hi_ts = pd.Timestamp(lo), pd.Timestamp(hi)
    df = _mx_deals(force=request.args.get("refresh") == "1")
    q_ini = _quarter_start_of(lo)
    quota = _mx_quota_arr(q_ini)
    oficial = _mx_realized_official(q_ini)

    # Realizado = receita: janela pela data com a extensão de fechamento.
    won = df[df["is_won"]].copy()
    won = won[won["rev_date"].notna() & (won["rev_date"] >= lo_ts) & (won["rev_date"] <= hi_ts)]
    op = df[df["is_open"]]
    pipe = op[op["close_date"].notna() & (op["close_date"] >= lo_ts) & (op["close_date"] <= hi_ts)]

    rows = []
    for ae, meta in MX_ROSTER.items():
        qt = quota.get(ae)
        real = oficial.get(ae, float(won.loc[won["account_executive"] == ae, "arr"].sum()))
        p = float(pipe.loc[pipe["account_executive"] == ae, "arr"].sum())
        falta = max((qt or 0.0) - real, 0.0)
        rows.append({
            "display": meta["display"], "account_executive": ae, "tier": meta["tier"],
            "quota": qt or 0.0, "has_quota": qt is not None, "realizado": real,
            "falta": falta, "pipe": p,
            # Cobertura só existe se ainda falta meta. Quem já bateu não tem
            # "quantas vezes cobre" — dividir por zero viraria infinito na tela.
            "cob": (p / falta) if falta > 0 else None,
            "bateu": bool(qt and real >= qt),
        })
    rows.sort(key=lambda r: (r["cob"] is None, -(r["cob"] or 0)))
    tq = sum(r["quota"] for r in rows)
    tr = sum(r["realizado"] for r in rows)
    tf = max(tq - tr, 0.0)
    tp = sum(r["pipe"] for r in rows)
    return render_template(
        "mx_coverage.html", ctx=ctx, rows=rows, leader=MX_LEADER,
        total_quota=tq, total_real=tr, total_falta=tf, total_pipe=tp,
        total_cob=(tp / tf) if tf > 0 else None,
        sem_quota=[r["display"] for r in rows if not r["has_quota"]],
        range_label=_format_period(f, ctx["presets"], ctx["active_preset"]),
    )


# Faixas de inatividade, em dias sem atividade registrada no HubSpot.
MX_STALE_BUCKETS = [(0, 7, "até 7d", "#10B981"), (7, 15, "8–15d", "#84CC16"),
                    (15, 30, "16–30d", "#F59E0B"), (30, 60, "31–60d", "#F97316"),
                    (60, 10**6, "60d+", "#DC2626")]


@app.route("/mx/stale")
def mx_stale():
    """Sales MX · Inatividade — pipeline aberto por tempo sem atividade.

    Mede `last_activity_date`, e deal SEM data nenhuma entra num bucket próprio
    em vez de virar "infinitamente parado": nunca ter registrado atividade é um
    problema de CRM diferente de ter parado há 3 meses."""
    ctx = base_context()
    f = ctx["filters"]
    df = _mx_deals(force=request.args.get("refresh") == "1")
    df = df[df["is_open"]]
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]
    hoje = pd.Timestamp(_today())
    la = pd.to_datetime(df["last_activity_date"], errors="coerce")
    dias = (hoje - la).dt.days
    sem_data = df[la.isna()]

    buckets = []
    for lo_d, hi_d, label, color in MX_STALE_BUCKETS:
        m = dias.notna() & (dias > lo_d) & (dias <= hi_d) if lo_d else dias.notna() & (dias <= hi_d)
        g = df[m]
        buckets.append({"label": label, "color": color, "deals": int(len(g)),
                        "arr": float(g["arr"].sum())})
    por_ae = []
    for ae, meta in MX_ROSTER.items():
        m = df["account_executive"] == ae
        d_ae = dias[m]
        parados = df[m & dias.notna() & (dias > 30)]
        por_ae.append({"display": meta["display"], "account_executive": ae,
                       "deals": int(m.sum()), "arr": float(df.loc[m, "arr"].sum()),
                       "mediana": float(d_ae.median()) if d_ae.notna().any() else None,
                       "parados_deals": int(len(parados)),
                       "parados_arr": float(parados["arr"].sum())})
    por_ae.sort(key=lambda r: r["parados_arr"], reverse=True)
    return render_template(
        "mx_stale.html", ctx=ctx, buckets=buckets, por_ae=por_ae, leader=MX_LEADER,
        total_deals=int(len(df)), total_arr=float(df["arr"].sum()),
        sem_data_deals=int(len(sem_data)), sem_data_arr=float(sem_data["arr"].sum()),
        max_arr=max([b["arr"] for b in buckets], default=0.0),
    )



# Colunas do drill do MX. Traz a moeda ORIGINAL junto do ARR convertido: sem
# isso, um deal digitado em peso apareceria com um número que não existe em
# lugar nenhum do HubSpot e pareceria erro.
_DRILL_MX_COLUMNS = [
    {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
    {"key": "ae",        "label": "AE"},
    {"key": "stage",     "label": "Estágio"},
    {"key": "arr",       "label": "ARR US$", "type": "money"},
    {"key": "original",  "label": "Valor no HubSpot"},
    {"key": "deal_type", "label": "Tipo"},
    {"key": "data",      "label": "Data", "type": "date"},
]


@app.route("/mx/overview/deals")
def mx_overview_deals():
    """Drill do Sales MX: ganhos e/ou pipeline de um AE na janela da tela."""
    f = get_filters()
    lo, hi = _mx_window(f)
    lo_ts, hi_ts = pd.Timestamp(lo), pd.Timestamp(hi)
    name = (request.args.get("name") or "").strip()
    kind = request.args.get("kind") or "won"

    df = _mx_deals()
    if name:
        df = df[df["account_executive"] == name]
    # Filtro de AE da barra por cima do clique — a tela já recorta por ele.
    if f.get("ae"):
        df = df[df["account_executive"] == f["ae"]]
    if kind == "pipe":
        d = df[df["is_open"] & df["close_date"].notna()
               & (df["close_date"] >= lo_ts) & (df["close_date"] <= hi_ts)].copy()
        d["data"] = d["close_date"]
    else:
        # Mesma régua do realizado da tela (data da receita, com a extensão);
        # a coluna Data mostra a validação real.
        d = df[df["is_won"]].copy()
        d["data"] = d["po_emission_date"].fillna(d["close_date"])
        d = d[d["rev_date"].notna() & (d["rev_date"] >= lo_ts) & (d["rev_date"] <= hi_ts)]

    d = d.sort_values("arr", ascending=False)
    rows = [{
        "deal_name": r.deal_name or "—",
        "deal_url": _hs_deal_url(r.deal_id),
        "ae": MX_DISPLAY.get(r.account_executive, r.account_executive),
        "stage": r.stage_label or "—",
        "arr": float(r.arr or 0),
        "original": (f"{r.deal_currency} {r.amount_orig:,.0f}".replace(",", ".")
                     if pd.notna(r.amount_orig) else "—"),
        "deal_type": r.deal_type or "—",
        "data": r.data.date().isoformat() if pd.notna(r.data) else "",
    } for r in d.itertuples(index=False)]
    total = sum(x["arr"] for x in rows)
    return jsonify({
        "title": MX_DISPLAY.get(name, name) or ("Pipeline aberto" if kind == "pipe" else "Realizado"),
        "subtitle": f"{len(rows)} deal(s) · ARR US$ {round(total):,}".replace(",", "."),
        "columns": _DRILL_MX_COLUMNS,
        "rows": rows,
    })


# ============================================================
# Sales MX — as MESMAS telas do BR, servidas sob /mx/
# ============================================================
# Nenhuma tela nova: cada rota aponta pra a MESMA função de view do Brasil. O que
# troca o funil é `_pl()`, lido do caminho; o que traduz o vocabulário do México
# é a volta da query (`bq.to_usd` converte moeda, `bq.normalize_stages` reescreve
# os ids de estágio pro canônico do BR); e o que neutraliza a camada de time é o
# ramo MX de `_apply_roster_current`. Por isso "idêntico" é literal, e continua
# idêntico quando alguém mexer numa tela — não existe segunda cópia pra esquecer.
_MX_ESPELHO = [
    ("/mx/pipeline/total",            "mx_pipeline_total",       "pipeline_total"),
    ("/mx/pipeline/total/deals",      "mx_pipeline_total_deals", "pipeline_total_deals"),
    ("/mx/forecast/full",             "mx_forecast_full",        "forecast"),
    ("/mx/forecast/deals",            "mx_forecast_deals",       "forecast_deals"),
    ("/mx/pipeline/funnel",           "mx_pipeline_funnel",      "pipeline_funnel"),
    ("/mx/pipeline/funnel/deals",     "mx_pipeline_funnel_deals","pipeline_funnel_deals"),
    ("/mx/pipeline/stalled",          "mx_pipeline_stalled",     "pipeline_stalled"),
    ("/mx/pipeline/stalled/deals",    "mx_pipeline_stalled_deals","pipeline_stalled_deals"),
    ("/mx/pipeline/wow/history",      "mx_wow_history",          "pipeline_wow_history"),
    ("/mx/pipeline/wow/history/deals","mx_wow_history_deals",    "pipeline_wow_history_deals"),
    ("/mx/overview/sales",            "mx_overview_sales",       "overview"),
    # Geração de Demanda. Substitui a tela própria que existia aqui antes (deals
    # criados × 1ªs reuniões por semana): metade dela era `first_meeting_eb`, que
    # no México está morto desde 11/04/2024, e a outra metade (criados por semana)
    # é o gráfico de tendência desta, com grão de semana. Como toda tela espelho,
    # o recorte de quem aparece vem do data layer — ver o ramo MX de
    # _get_demand_deals, que corta pelos 7 vendedores do organograma do Mauricio.
    ("/mx/demand",                    "mx_demand",               "demand"),
    ("/mx/demand/deals",              "mx_demand_deals",         "demand_deals"),
    ("/mx/overview/full-deals",       "mx_overview_full_deals",  "overview_deals"),
    ("/mx/pipeline/cobertura",        "mx_pipeline_cob",         "pipeline"),
    ("/mx/pipeline/deals",            "mx_pipeline_deals",       "pipeline_coverage_deals"),
    ("/mx/pipeline/stalled/wow-deals","mx_stalled_wow_deals",    "pipeline_stalled_wow_deals"),
    ("/mx/pipeline/stalled-activity", "mx_pipeline_activity",    "pipeline_activity"),
    ("/mx/pipeline/stalled-activity/deals","mx_activity_deals",  "pipeline_activity_deals"),
    ("/mx/pipeline/stalled-activity/wow-deals","mx_activity_wow_deals","pipeline_activity_wow_deals"),
    ("/mx/pipeline/funnel/moves",     "mx_funnel_moves",         "pipeline_funnel_moves"),
    ("/mx/pipeline/funnel/moves/deals","mx_funnel_move_deals",   "pipeline_funnel_move_deals"),
    # 12/10/2026 — o resto do AE BR ("layout das abas e subabas idêntico ao AE
    # Br", pedido do Pedro). Sales Overtime fica de fora a pedido dele. O México
    # tem uma visão só (um gestor), então o DoD e o WoW — que no BR são só do
    # Leaders — entram aqui, senão a foto do México não teria onde aparecer.
    ("/mx/overview/ae",               "mx_overview_ae",          "overview_ae"),
    ("/mx/overview/funnel-stages",    "mx_overview_funnel_stages","overview_funnel_stages"),
    ("/mx/overview/funnel-stages/deals","mx_overview_funnel_stage_deals","overview_funnel_stage_deals"),
    ("/mx/forecast/model",            "mx_forecast_model",       "forecast_model"),
    ("/mx/forecast/model/deals",      "mx_forecast_model_deals", "forecast_model_deals"),
    ("/mx/forecast/wow",              "mx_forecast_wow",         "forecast_wow"),
    ("/mx/forecast/wow/deals",        "mx_forecast_wow_deals",   "forecast_wow_deals"),
    ("/mx/forecast/dod",              "mx_forecast_dod",         "leaders_forecast_dod"),
    ("/mx/forecast/dod/deals",        "mx_forecast_dod_deals",   "leaders_forecast_dod_deals"),
    ("/mx/forecast/history",          "mx_forecast_history",     "forecast_history"),
    ("/mx/forecast/history/deals",    "mx_forecast_history_deals","forecast_history_deals"),
    ("/mx/pipeline/dod",              "mx_pipeline_dod",         "leaders_pipeline_dod"),
    ("/mx/pipeline/dod/deals",        "mx_pipeline_dod_deals",   "leaders_pipeline_dod_deals"),
    ("/mx/pipeline/wow",              "mx_pipeline_wow",         "leaders_pipeline_wow"),
    ("/mx/pipeline/wow/deals",        "mx_pipeline_wow_deals",   "leaders_pipeline_wow_deals"),
    ("/mx/pipeline/legal",            "mx_pipeline_legal",       "pipeline_legal"),
    ("/mx/pipeline/legal/deals",      "mx_pipeline_legal_deals", "pipeline_legal_deals"),
    ("/mx/pipeline/funnel-ii",        "mx_pipeline_funnel2",     "pipeline_funnel2"),
    ("/mx/pipeline/funnel-ii/deals",  "mx_pipeline_funnel2_deals","pipeline_funnel2_deals"),
    ("/mx/pipeline/visits",           "mx_pipeline_visits",      "pipeline_visits"),
]
def _regra_do_endpoint(nome):
    """Caminho registrado de um endpoint (o 1o, ignorando variantes)."""
    for r in app.url_map.iter_rules():
        if r.endpoint == nome and "<" not in r.rule:
            return r.rule
    return None


for _rule, _endpoint, _target in _MX_ESPELHO:
    _br_rule = _regra_do_endpoint(_target)
    app.add_url_rule(_rule, _endpoint, app.view_functions[_target])
    _MX_ENDPOINT_BY_TARGET[_target] = _endpoint
    if _br_rule:
        _MX_PATH_TO_BR[_rule] = _br_rule
# Os PoP têm foco no path e não entram na lista acima.
_MX_ENDPOINT_BY_TARGET.update({
    "pipeline_wow_early": "mx_wow_early", "pipeline_wow_late": "mx_wow_late",
    "pipeline_wow_deals": "mx_wow_deals",
})
_MX_PATH_TO_BR.update({
    "/mx/pipeline/wow/early": "/pipeline/wow/early",
    "/mx/pipeline/wow/late":  "/pipeline/wow/late",
})


@app.route("/mx/pipeline/wow/early", endpoint="mx_wow_early")
def _mx_wow_early():
    return _render_pipeline_wow("early")


@app.route("/mx/pipeline/wow/late", endpoint="mx_wow_late")
def _mx_wow_late():
    return _render_pipeline_wow("late")


@app.route("/mx/pipeline/wow/<focus>/deals", endpoint="mx_wow_deals")
def _mx_wow_deals(focus):
    return pipeline_wow_deals(focus)


# Servidor MCP pros agentes de IA. Fica DEPOIS de todas as rotas: o catálogo de
# telas que ele oferece sai do url_map. K_REVISION = revisão do Cloud Run.
agent_api.init(app, base_context, versao_app=os.environ.get("K_REVISION", ""))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    # PEPEU_PAPEL=api é o gêmeo `pepeuhacker-api` (AGENTES.md), que só atende
    # agente. Sem pré-aquecimento: com min-instances 0 o boot tem que caber no
    # probe de inicialização, e o _prewarm leva ~3,5 min. Sem os dois laços:
    # são eles que gravam as fotos do forecast no BigQuery, e quem grava é o
    # serviço principal — duas instâncias gravando a mesma foto não ajuda ninguém.
    if os.environ.get("PEPEU_PAPEL") != "api":
        _prewarm()
        threading.Thread(target=_refresh_loop, name="cache-refresher", daemon=True).start()
        threading.Thread(target=_midnight_loop, name="forecast-midnight", daemon=True).start()
    from waitress import serve
    # HOST: 127.0.0.1 (padrão, só local) ou 0.0.0.0 (exposto na rede/VPN p/ o time
    # testar). SEM autenticação — usar só em rede interna/VPN confiável.
    host = os.environ.get("HOST", "0.0.0.0")
    print(f"pepeuhacker · waitress on http://{host}:{port}", flush=True)
    # Deixa o endereço compartilhável no log: o IP é DHCP e muda, então ter o
    # registro (e o nome .local, que não muda) evita passar link morto pro time.
    _u = _access_urls()
    print(f"pepeuhacker · compartilhar: {_u['mdns'] or '(sem nome mDNS)'}"
          f"   |   por IP (muda): {_u['ip'] or '(sem IP de rede)'}", flush=True)
    serve(app, host=host, port=port, threads=8, _quiet=False)
