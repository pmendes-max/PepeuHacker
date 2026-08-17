"""Pepeu Hacker — Sales performance dashboard for Tractian.
Flask app serving on localhost. Data source: BigQuery (tractian-bi)."""
import os
import re
import socket
import subprocess
import threading
import time
import urllib.parse
from datetime import date, datetime, timedelta
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, jsonify

import bq
import hubspot

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True
app.jinja_env.globals.update(zip=zip, enumerate=enumerate)


def _asset_v(filename: str) -> str:
    """mtime do arquivo em /static — usado pra cache-bust automático."""
    try:
        path = os.path.join(app.root_path, "static", filename)
        return str(int(os.path.getmtime(path)))
    except OSError:
        return "0"

app.jinja_env.globals["asset_v"] = _asset_v


# Allowlist: only these leaders' data is shown anywhere in the dashboard.
# Hides US/MX leaders + BR leaders not in this list (Francisco Maia, Pedro Mendes,
# Victor Ferraz, João Vitor Granzotti Machado, "No leader assigned").
ALLOWED_LEADERS = {
    "Leonardo Scopel",
    "Fernando Barati",
    "Bernardo Agosti",
    "João Tavares",
    "Gustavo Giovani",
    "Arthur Tkotz",        # RSM Mid Market (player-coach) — pod sob Fernando (Q3/2026)
}

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
HIDDEN_FROM_AE_VIEWS = DEPARTED_AES | NON_AE_USERS

# AEs que saíram A PARTIR de um quarter específico: mantêm o histórico ANTES do
# cutoff, mas somem do cutoff em diante. Diferente de DEPARTED_AES (que apaga o
# histórico inteiro). Chave = nome EXATO do BQ; valor = 1º dia do quarter a partir
# do qual a pessoa some. Aplicado nos loaders de dados (propaga p/ todas as telas).
DEPARTED_FROM = {
    "Victor Paravani": date(2026, 7, 1),   # sai no Q3/2026 — mantém Q2
    "Raul Tezza":      date(2026, 7, 1),
}


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
# ser dirigida pelo Q3_ROSTER (abaixo), que é quarter-scoped na curated (preserva
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
# Roster Q3/2026 — fonte da verdade informada pela liderança (planilha).
# Define TIME (leader = RSM/gerente direto), QUOTA (MRR, BRL) e META DE NEW LOGOS
# por AE. Chave = nome EXATO no BigQuery. Aplicado:
#   • curated  → só nas linhas do quarter Q3 (preserva histórico Q2 no /perf etc.)
#   • deal-level → global por nome (deal-level já usa 1 líder = time atual)
#   • /overview  → quota + meta de new logos autoritativas quando o período é o Q3
# Arthur Tkotz é player-coach (tem quota própria E gerencia um pod sob o Fernando).
# ============================================================
Q3_START = date(2026, 7, 1)
Q3_END   = date(2026, 9, 30)
Q3_ROSTER = {
    # Bernardo Agosti (Commercial + Mid Market)
    "Felipe Martins":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 50000,  "new_logos": 4},
    "Rafael Avellar":                      {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 28000,  "new_logos": 2},
    "Diego Loures":                        {"leader": "Bernardo Agosti", "tier": "Commercial", "quota": 5000,   "new_logos": 1},  # "Diego Loures Peter" — nome exato no BQ = "Diego Loures"
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
# region do roster = BR (todo o time). Mapa nome→região p/ deal-level.
Q3_ROSTER_REGION = "BR"

# ============================================================
# Câmbio p/ reporte global. O time BR é medido em MRR (BRL); a receita é reportada
# ao global em ARR (USD).  ARR_USD = MRR_BRL × 12 ÷ USD_BRL_RATE  (e o inverso).
# >>> Pra mudar a taxa do dólar: edite USD_BRL_RATE aqui (ou defina a env var
#     USD_BRL_RATE) e reinicie o app. Hoje: R$ 5,05 por US$.
# ============================================================
USD_BRL_RATE = float(os.environ.get("USD_BRL_RATE", "5.05"))
MONTHS_PER_YEAR = 12  # MRR → ARR


def mrr_brl_to_arr_usd(mrr_brl) -> float:
    """MRR em BRL → ARR em USD (anualiza ×12 e converte pela taxa do dólar)."""
    return (to_float(mrr_brl) * MONTHS_PER_YEAR / USD_BRL_RATE) if USD_BRL_RATE else 0.0


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


def apply_q3_roster_curated(df: pd.DataFrame) -> pd.DataFrame:
    """Nas linhas do quarter Q3, sobrescreve leader e quota_in_brl pelo Q3_ROSTER.
    Quarter-scoped → preserva o histórico dos quarters anteriores. Requer coluna
    `quarter` (date)."""
    if df is None or len(df) == 0 or "quarter" not in df.columns or not Q3_ROSTER:
        return df
    is_q3 = df["quarter"] == Q3_START
    for ae, v in Q3_ROSTER.items():
        m = is_q3 & (df["account_executive"] == ae)
        if m.any():
            df.loc[m, "leader"] = v["leader"]
            if "region" in df.columns:
                df.loc[m, "region"] = Q3_ROSTER_REGION
            if "quota_in_brl" in df.columns:
                df.loc[m, "quota_in_brl"] = v["quota"]
    return df


# Marca de ORIGEM da linha na curada: False = medida pelo BI, True = declarada pelo
# Q3_ROSTER. Existe porque depois do concat "AE que o BI ainda não mediu" e "AE com
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
    """Cria na curada a linha que falta pra AE nomeado no Q3_ROSTER.

    INVARIANTE: o Q3_ROSTER é a fonte da verdade de QUEM está no time no Q3; a
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
    if (df is None or len(df) == 0 or not Q3_ROSTER
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

    # Ausência testada por (nome, Q3_START), não "ausente da tabela inteira": o
    # invariante que as telas precisam é por trimestre, e assim o caso "AE antigo
    # que a curada ainda não fechou no Q3" também fica coberto.
    have = set(df.loc[df["quarter"] == Q3_START, "account_executive"].dropna())
    missing = [ae for ae in Q3_ROSTER if ae not in have]
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
        "quarter": [Q3_START] * n,
        "quarter_end": [_quarter_end_of(Q3_START)] * n,
        "leader": [Q3_ROSTER[a]["leader"] for a in missing],
        "region": [Q3_ROSTER_REGION] * n,
        "role": ["AE"] * n,
        "is_currently_active": [True] * n,
        "quota_in_brl": [Q3_ROSTER[a]["quota"] for a in missing],
        "new_logos_quota": [Q3_ROSTER[a]["new_logos"] for a in missing],
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


def apply_roster_leader(df: pd.DataFrame) -> pd.DataFrame:
    """Deal-level: atribui leader/region do Q3_ROSTER a todos os deals dos AEs do
    roster (deal-level usa 1 líder = time atual). Global por nome. Usado no
    /pipeline (visão corrente). Rotas com histórico (overview) reatribuem por
    quarter via _attr_deal_leader pra não vazar o roster pro passado."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns or not Q3_ROSTER:
        return df
    lead = df["account_executive"].map({a: v["leader"] for a, v in Q3_ROSTER.items()})
    m = lead.notna()
    if m.any():
        df.loc[m, "leader"] = lead[m]
        if "region" in df.columns:
            df.loc[m, "region"] = Q3_ROSTER_REGION
    return df


def _apply_roster_current(df: pd.DataFrame) -> pd.DataFrame:
    """Frames de SNAPSHOT vindos direto do BQ (pipeline stalled/funnel/wow) trazem
    o leader/region CRU da SQL (não-roster). Reatribui pelo Q3_ROSTER e remove quem
    já saiu (cutoff <= hoje) — espelha o que _get_pipeline_deals faz. Visão corrente
    (não histórica), então usa o time atual global."""
    if df is None or len(df) == 0 or "account_executive" not in df.columns:
        return df
    df = apply_roster_leader(df.copy())
    _gone = {ae for ae, cut in DEPARTED_FROM.items() if cut <= _today()}
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
    Fallback: gestor→ele mesmo; senão roster Q3; senão o leader atual do frame.
    Corrige atribuição histórica (Q2 fica no líder de Q2, Q3 no do roster)."""
    if not len(frame):
        return []
    out = []
    for ae, dt, cur in zip(frame["account_executive"], frame[date_col], frame["leader"]):
        q = _quarter_start_of(dt.date()) if pd.notna(dt) else None
        ld = lmap.get((ae, q))
        if ld is None:
            if ae in NON_AE_USERS:
                ld = ae                                   # venda direta do gestor
            elif q == Q3_START and ae in Q3_ROSTER:
                ld = Q3_ROSTER[ae]["leader"]              # AE novo do roster sem linha curated
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
    "Pedro Mendes":    ["Leonardo Scopel", "Fernando Barati", "Bernardo Agosti", "Gustavo Giovani"],
    "Leonardo Scopel": [],
    "Fernando Barati": ["Arthur Tkotz"],
    "Bernardo Agosti": ["João Tavares"],
    "Gustavo Giovani": [],
    "João Tavares":    [],
    "Arthur Tkotz":    [],
}

# Ordem hierárquica do dropdown (VP → Diretores → Gerentes/RSMs).
LEADER_ORDER = [
    "Pedro Mendes",
    "Leonardo Scopel", "Fernando Barati", "Bernardo Agosti",
    "Gustavo Giovani", "Arthur Tkotz", "João Tavares",
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
#   1) mexer no Q3_ROSTER (mover AE, mudar quota) → muda a capacity;
#   2) mudar Q3_TEAM_REVENUE_META_ARR_USD → muda o numerador;
#   3) mudar USD_BRL_RATE → a meta em BRL é derivada do câmbio.
# Em qualquer um dos casos, recalcular pela fórmula acima e reconferir a soma.
#
# Bônus: foi por esse fator que se confirmou que o alvo do João já embute o
# Daniel Del Paschoa — com o Daniel o fator dele casa com os outros 5 na 7ª
# decimal, sem o Daniel dispara pra 1,509.
LEADER_BOARD_TARGET = {
    "Bernardo Agosti":   459240.97,
    "João Tavares":      183614.43,
    "Fernando Barati":   898651.49,
    "Arthur Tkotz":      385223.98,
    "Leonardo Scopel":   414054.44,
    "Gustavo Giovani":   732011.44,
}

# Cargo de cada um — usado pra mostrar contexto no dropdown.
LEADER_ROLE = {
    "Pedro Mendes":    "VP",
    "Leonardo Scopel": "Diretor Enterprise",
    "Fernando Barati": "Diretor Mid Market",
    "Bernardo Agosti": "Diretor Commercial",
    "Gustavo Giovani": "Gerente Enterprise",
    "João Tavares":    "Gerente Commercial",
    "Arthur Tkotz":    "RSM Mid Market",
}


def expand_leader(leader: str) -> set:
    """Set de leader values na base que devem ser incluídos quando `leader` é
    selecionado no filtro. Inclui o próprio + subordinados transitivos."""
    if not leader: return set()
    out = {leader}
    for child in ORG_CHART.get(leader, []):
        out |= expand_leader(child)
    return out


def _leader_meta_total(leader_names) -> float:
    """Total da meta (Board Target) de um conjunto de líderes, contando só os de
    TOPO — exclui quem é subordinado de outro líder presente (evita somar o alvo
    do gerente dentro do do diretor). Ex.: {Fernando, Arthur} → só Fernando."""
    names = set(leader_names)
    top = [L for L in names if not any(L != o and L in expand_leader(o) for o in names)]
    return float(sum(LEADER_BOARD_TARGET.get(L, 0.0) for L in top))


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


def _pipe_leader_df(by_ae_df, leader_filter, use_board_target=True):
    """Rollup por LÍDER do by_ae de pipeline: cada líder soma seu time
    (expand_leader). remaining_quota recalculado no nível do líder (clip 0).
    Devolve DataFrame com as MESMAS colunas do by_ae por-AE (account_executive=L)
    pra reusar `_cov_items` sem mudança.

    `use_board_target=False` soma a quota do time em vez de usar o Board Target.
    O Board Target da planilha é do trimestre CORRENTE, então não vale quando a
    janela é outro trimestre (caso do Early no M3, que aponta pro próximo)."""
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
        q = (LEADER_BOARD_TARGET.get(L, float(sub["quota_brl"].sum()))
             if use_board_target else float(sub["quota_brl"].sum()))
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
    if axis_mode == "count":
        _axis_max, _ticks = _nice_int_axis(_maxv)
        _scale = float(_axis_max)
        yL = [{"label": fmt_int(t), "y": round(_base_y - t / _scale * _PLOT, 1)} for t in _ticks]
    else:
        _scale = _maxv
        yL = [{"label": fmt_brl(_maxv * k / 4), "y": round(_base_y - k / 4 * _PLOT, 1)} for k in range(5)]

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


def _today():
    return date.today()


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
        now = now_dt or datetime.now()
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

# Funil II — o período aqui não é close date nem snapshot: é a data em que o deal
# ENTROU na etapa. Janela aberta (sem datas) = histórico inteiro, de propósito.
FUNNEL2_PATHS = ("/pipeline/funnel-ii", "/leaders/pipeline/funnel-ii")

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
}


def filter_presets_for_path(presets: list[dict], path: str) -> list[dict]:
    allowed = PRESET_KEYS_BY_PATH.get(path)
    if allowed is None:
        return presets
    by_key = {p["key"]: p for p in presets}
    return [by_key[k] for k in allowed if k in by_key]


# Label do filtro de período varia por rota — na Cobertura é Close Date do HubSpot
# e no WoW são os 2 domingos comparados.
PERIOD_LABEL_BY_PATH = {**{p: "Close Date" for p in COVERAGE_PATHS},
                        **{p: "Close Date" for p in PIPELINE_TOTAL_PATHS},
                        **{p: "Domingos comparados" for p in WOW_PATHS},
                        **{p: "Entrada na etapa" for p in FUNNEL2_PATHS}}


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
        "fcat":      request.args.get("fcat", ""),
    }


def _money_mode() -> str:
    """'arr' quando o filtro mestre de moeda está em ARR US$; senão 'mrr'."""
    from flask import has_request_context
    if has_request_context() and request.args.get("money") == "arr":
        return "arr"
    return "mrr"


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
    """ae_performance + leader overrides + ALLOWED_LEADERS + HIDDEN_FROM_AE_VIEWS,
    pré-computado com quarter (date) e quarter_end. Cacheado em memória —
    invalidação via mtime do parquet ou force=True."""
    df_raw = bq.ae_performance(force_refresh=force)
    # Chave versionada: o mtime do parquet só detecta dado novo, não CÓDIGO novo —
    # o sufixo garante que nenhum frame montado por uma versão anterior da curada
    # (sem a linha sintética do roster) sobreviva a um reload a quente.
    key = ("ae_perf_clean_v2", _parquet_mtime("ae_performance"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = df_raw.copy()
    df["quarter"] = pd.to_datetime(df["quarter"]).dt.date
    df["quarter_end"] = df["quarter"].map(_quarter_end_of)
    # Nome canônico ANTES do roster: é o rename que faz a linha órfã do AE casar
    # com o Q3_ROSTER, e a fusão evita a quota sair dobrada.
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
    key = ("unit_price_events_v1", _parquet_mtime("unit_price_events_v1"))
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
    key = ("quota_history_v2", _parquet_mtime("quota_history_v2"))
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
                or (Q3_ROSTER[ae]["leader"] if ae in Q3_ROSTER else None)
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
    out = {ae: v["leader"] for ae, v in Q3_ROSTER.items()}
    for r in _get_clean_ae_perf(force=force).sort_values("quarter").itertuples(index=False):
        out.setdefault(r.account_executive, r.leader)
    return out


def _get_funnel_entries(force: bool = False) -> pd.DataFrame:
    """Pivot deal × estágio → timestamp da 1ª entrada, uma coluna por número de
    estágio (s1..s7). Base das coortes de conversão do funil."""
    raw = bq.funnel_stage_entries(force_refresh=force)
    key = ("funnel_entries_v6", _parquet_mtime("funnel_stage_entries_v5"))
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
    key = ("ae_perf_mgrs_v2", _parquet_mtime("ae_performance"))  # ver nota da chave em _get_clean_ae_perf
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = df_raw.copy()
    df["quarter"] = pd.to_datetime(df["quarter"]).dt.date
    df["quarter_end"] = df["quarter"].map(_quarter_end_of)
    # Nome canônico ANTES do roster: é o rename que faz a linha órfã do AE casar
    # com o Q3_ROSTER, e a fusão evita a quota sair dobrada.
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
    raw = bq.demand_deals_raw(force_refresh=force)
    key = ("demand_deals", _parquet_mtime("demand_deals_raw_v3"))
    if not force:
        with _DATA_CACHE_LOCK:
            cached = _DATA_CACHE.get(key)
        if cached is not None:
            return cached
    df = canon_ae_names(raw)
    df["created_date"] = pd.to_datetime(df["created_date"])

    is_auto = df["sdr_id"].isna() | (df["sdr_id"] == df["account_executive_id"])
    df["source"] = pd.Series("received", index=df.index)
    df.loc[is_auto, "source"] = "auto"

    is_acq  = df["deal_type"] == "Aquisition"
    is_exp  = df["deal_type"] == "Up & cross sell"
    is_outb = df["deal_source_agg"] == "Outbound"

    # Bucket canônico (5 + other) — mesmo set usado por by_ae e by_grain.
    bucket = pd.Series("other", index=df.index)
    bucket = bucket.mask(is_auto  & is_acq, "auto_acq")
    bucket = bucket.mask(is_auto  & is_exp, "auto_exp")
    bucket = bucket.mask(~is_auto & is_exp, "rcv_exp")
    bucket = bucket.mask(~is_auto & is_acq &  is_outb, "rcv_outbound")
    bucket = bucket.mask(~is_auto & is_acq & ~is_outb, "rcv_inbound")
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
    df = drop_departed_from(df, "created_date")
    with _DATA_CACHE_LOCK:
        _DATA_CACHE[key] = df
    return df


def _get_visits(force: bool = False) -> pd.DataFrame:
    """Visitas (HubSpot) por AE, nível-visita, com leader/region anexados pela
    MESMA atribuição de time do resto do app (snapshot ae_performance + roster Q3).
    Colunas: account_executive, start_date, category ('field'|'ia_center'), leader, region."""
    raw = bq.visits_raw(force_refresh=force)
    key = ("visits", _parquet_mtime("visits_raw_v2"))
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
            "auto_acq","auto_exp","rcv_outbound","rcv_inbound","rcv_exp","total_generated"]
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
    for col in ["auto_acq","auto_exp","rcv_outbound","rcv_inbound","rcv_exp"]:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot["total_generated"] = (pivot["auto_acq"] + pivot["auto_exp"]
                                + pivot["rcv_outbound"] + pivot["rcv_inbound"] + pivot["rcv_exp"])
    return (pivot[pivot["total_generated"] > 0]
            .sort_values("total_generated", ascending=False)
            .reset_index(drop=True))


def _demand_deals_rows(deals: pd.DataFrame, date_from: date, date_to: date,
                       name: str = None, grain: str = "ae",
                       leader: str = None, region: str = None,
                       source: str = None) -> list:
    """Drill-down de demanda no MESMO universo das contagens (criados na janela,
    bucket != other). Cada linha traz nome, tipo, qualification owner (SDR; ou o
    próprio AE quando auto), estágio, valor e data de criação.

    Recorte igual ao dos outros drills (`_filter_entity`):
      · `name` + grain='ae'     → só os deals daquele vendedor;
      · `name` + grain='leader' → os deals de TODOS os AEs que respondem àquele
        líder (expand_leader, transitivo — pega gerente sob diretor);
      · sem `name`              → TOTAL, com filtros opcionais de líder/região.

    `source='auto'|'received'` restringe à origem — usado pelos gráficos que
    contam só um dos lados, pra lista explodida fechar com o número da barra."""
    if len(deals) == 0:
        return []
    mask = ((deals["created_date"] >= pd.Timestamp(date_from)) &
            (deals["created_date"] <= pd.Timestamp(date_to)) &
            (deals["bucket"] != "other"))
    if source in ("auto", "received"):
        mask &= (deals["source"] == source)
    d = deals.loc[mask]
    if name and grain == "ae":
        d = d[d["account_executive"] == name]
    else:
        # Líder ou total: aplica o recorte permitido antes, senão o time de um
        # líder poderia arrastar AE desligado/escondido.
        d = d[d["leader"].isin(ALLOWED_LEADERS)]
        d = d[~d["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
        if name and grain == "leader":
            d = d[d["leader"].isin(expand_leader(name))]
        else:
            if region:
                d = d[d["region"] == region]
            if leader:
                d = d[d["leader"].isin(expand_leader(leader))]
    d = d.sort_values("created_date", ascending=False)
    out = []
    for _, r in d.iterrows():
        cd = r["created_date"]
        out.append({
            "deal_id":      str(r.get("deal_id") or ""),
            "deal_url":     _hs_deal_url(r.get("deal_id")),
            "ae":           r["account_executive"],
            "deal_name":    r.get("deal_name") or "—",
            "deal_type":    r.get("deal_type") or "—",
            "qual_owner":   r.get("sdr") or r["account_executive"],
            "stage":        stage_display(r.get("stage_label")),
            "amount":       float(r.get("amount") or 0),
            "created_date": cd.date().isoformat() if pd.notna(cd) else "",
        })
    return out


# Mapa bucket canônico → (source, kind) no shape esperado pela rota grain.
_BUCKET_TO_SOURCE_KIND = {
    "auto_acq":     ("auto",     "acq"),
    "auto_exp":     ("auto",     "exp"),
    "rcv_exp":      ("received", "exp"),
    "rcv_outbound": ("received", "outbound"),
    "rcv_inbound":  ("received", "inbound"),
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

    Universo = Q3_ROSTER (time atual) ∪ quem gerou na semana. Vem do roster, e
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
    for ae_name, cfg in Q3_ROSTER.items():
        if ae_name in HIDDEN_FROM_AE_VIEWS:
            continue
        if f["ae"] and ae_name != f["ae"]:
            continue
        if f["region"] and f["region"] != Q3_ROSTER_REGION:
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
# Categorias previstas, da maior confiança pra menor (ordem de segmentos/tabela).
FORECAST_CATEGORY_ORDER = ["COMMIT", "BEST_CASE", "PROBABLE", "PIPELINE"]
FORECAST_CATEGORY_LABEL = {
    "COMMIT": "Commit", "BEST_CASE": "Best Case", "PROBABLE": "Probable",
    "PIPELINE": "Funnel", "OMIT": "Not forecasted", "CLOSED": "Closed won",
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


def _model_prob(leader, dealstage, deal_type) -> float:
    """Probabilidade do modelo p/ um deal aberto, já com o desconto de segurança de
    10% (× MODEL_SAFETY_FACTOR). 0 se fora do escopo (estágio não modelado — só
    Negotiation/Decision/Procurement —, tipo desconhecido ou líder sem segmento)."""
    seg = LEADER_SEGMENT.get(leader)
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
    raw = bq.pipeline_deals_raw(force_refresh=force)
    key = ("pipeline_deals", _parquet_mtime("pipeline_deals_raw_v5"))
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
    raw = bq.forecast_deals_raw(force_refresh=force)
    key = ("forecast_deals", _parquet_mtime("forecast_deals_raw_v5"))
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
    df["forecast_category"] = df["forecast_category"].fillna("(None)")
    df["is_won"] = df["dealstage"] == "closedwon"  # PO Won = realizado
    # Ponderado só faz sentido pro pipeline aberto; closedwon não está no PCT → 0.
    df["pct"] = df["forecast_category"].map(FORECAST_CATEGORY_PCT).fillna(0.0)
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

    # Corte de saída: won conta pela data de validação, aberto pela close date.
    # Data efetiva >= cutoff → some (mantém histórico anterior). NaT fica.
    if DEPARTED_FROM:
        eff = df["valid_date"].where(df["is_won"], df["close_date"])
        eff = pd.to_datetime(eff, errors="coerce").dt.date
        drop = pd.Series(False, index=df.index)
        for ae, cutoff in DEPARTED_FROM.items():
            drop |= (df["account_executive"] == ae) & eff.notna() & (eff >= cutoff)
        df = df[~drop]

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
    }
    # Nas telas de janela fixa (PoP Early/Late) as datas são IGNORADAS, então um
    # chip "De: …" vindo de link antigo/salvo mentiria sobre o que está na tela.
    _skip = {"date_from", "date_to"} if current_path in WOW_PATHS else set()
    for k, v in filters.items():
        if not v or k not in labels or k in _skip: continue  # 'money' não vira chip (é o toggle mestre)
        remaining = {kk: vv for kk, vv in filters.items() if vv and kk != k}
        qs = urllib.parse.urlencode(remaining)
        url = current_path + ("?" + qs if qs else "")
        # Os cortes do Pipeline Total viajam na URL como chave curta (dtype=acq);
        # o chip mostra o rótulo de negócio ("Aquisição"), não a chave.
        shown = v
        if k in _PT_DIMS:
            shown = next((lbl for kk, _raw, lbl, *_ in _PT_DIMS[k]["values"] if kk == v), v)
        chips.append({"label": f"{labels[k]}: {shown}", "remove_url": url})
    return chips


def _extra_filters_for_path(path: str) -> list:
    """Selects extra da barra de filtros, POR ABA — filtro é por aba, não global
    (Deal Type não significa nada no Overview, então lá os campos nem existem).

    Pipeline Total pede as 3 dimensões; o Funil II só Deal Type, porque expansão
    converte numa ordem de grandeza diferente de aquisição e misturar as duas na
    mesma taxa esconde isso. Fora dessas abas, lista vazia."""
    if path in PIPELINE_TOTAL_PATHS:
        return [{"key": k, "label": v["label"], "opts": _pt_dim_meta(k)}
                for k, v in _PT_DIMS.items()]
    if path in FUNNEL2_PATHS:
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
    period_label = PERIOD_LABEL_BY_PATH.get(request.path, "Período")
    # No Funil II o que o período recorta é escolhido no chip da página, então o
    # rótulo do filtro segue o chip em vez de ser fixo por rota.
    if request.path in FUNNEL2_PATHS and request.args.get("base") == "created":
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
    if request.path in WOW_PATHS and not f["date_from"] and not f["date_to"]:
        active_preset = active_preset or "wow_1w"

    if request.path in COVERAGE_PATHS and not (f["date_from"] and f["date_to"]):
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
        "period_locked": request.path in WOW_PATHS,
        "extra_filters": _extra_filters_for_path(request.path),
        "period_text": _format_period(f, presets, active_preset),
        "view": view,
        "view_url_ae": view_url_ae,
        "view_url_leaders": view_url_leaders,
        "view_url_sdr": view_url_sdr,
        "leader_tabs": LEADER_TABS,
        "sdr_tabs": SDR_TABS,
        "usd_rate": USD_BRL_RATE,
        "months_per_year": MONTHS_PER_YEAR,
        # Filtro de PERÍODO mutável por aba: 'quarter' (Sales Overtime) troca o
        # date-range por presets de N trimestres + intervalo aberto quarter→quarter.
        **_period_widget_ctx(),
        "money_mode": ("arr" if request.args.get("money") == "arr" else "mrr"),
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
        return (url_for("overview_funnel_stage_deals") + "?" +
                urllib.parse.urlencode({**_carry, "stage": str(stage)}))

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
    if len(won) and f["leader"]:
        # Só recorta por líder quando pedido — usando o líder do quarter do PO.
        won["leader"] = _attr_deal_leader(won, "po_emission_date", _period_leader_map(force=force))
        won = won[won["leader"].isin(expand_leader(f["leader"]))]
    if len(won):
        won["q"] = won["po_emission_date"].dt.date.map(_quarter_start_of)
        won["is_acq"] = won["deal_type"] == "Aquisition"
        won["is_exp"] = won["deal_type"] == "Up & cross sell"

    # ---- Janela de quarters ----
    cur_q = _quarter_start_of(_today())
    universe = sorted({q for q in (set(qh["quarter"]) | (set(won["q"]) if len(won) else set()))
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
    real_by_q = won.groupby("q")["amount"].sum().to_dict() if len(won) else {}
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
            d["name"] = f["ae"]
        return url_for("overview_deals") + "?" + urllib.parse.urlencode(d)

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
                         "modal_url": url_for("overview_unit_price_deals") + "?" +
                                      urllib.parse.urlencode(_up_qs)})

    rk = _build_rank_chart(items, {}, axis_mode="currency",
                           drill_path="/overview/overtime") if items else None
    rk_nl = (_build_rank_chart(items_nl, {}, axis_mode="count",
                               drill_path="/overview/overtime")
             if any(i["realized"] or i["quota"] for i in items_nl) else None)
    rk_tk = (_build_rank_chart(items_tk, {}, axis_mode="currency", show_line=False,
                               drill_path="/overview/overtime")
             if any(i["realized"] or i["quota"] for i in items_tk) else None)
    rk_up = (_build_rank_chart(items_up, {}, axis_mode="currency", show_line=False,
                               drill_path="/overview/overtime")
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
    won  = dd[dd["is_won"] & dd["po_emission_date"].notna()
              & ~dd["deal_id"].isin(hold_ids)
              & (dd["po_emission_date"] >= lo) & (dd["po_emission_date"] <= hi)].copy()
    pipe = dd[(~dd["is_won"]) & dd["close_date"].notna()
              & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)].copy()
    hld  = dd[dd["deal_id"].isin(hold_ids) & dd["po_emission_date"].notna()
              & (dd["po_emission_date"] >= lo) & (dd["po_emission_date"] <= hi)].copy()

    # Atribui o líder de CADA deal pelo quarter da sua data (won/hold→PO, pipe→close):
    # o realizado cai no MESMO líder que a quota daquele quarter. Depois aplica
    # ALLOWED_LEADERS + filtro de líder.
    lmap = _period_leader_map(force=force)
    won["leader"]  = _attr_deal_leader(won,  "po_emission_date", lmap)
    pipe["leader"] = _attr_deal_leader(pipe, "close_date",       lmap)
    hld["leader"]  = _attr_deal_leader(hld,  "po_emission_date", lmap)
    won  = won[won["leader"].isin(ALLOWED_LEADERS)]
    pipe = pipe[pipe["leader"].isin(ALLOWED_LEADERS)]
    hld  = hld[hld["leader"].isin(ALLOWED_LEADERS)]
    if f["leader"]:
        _scope = expand_leader(f["leader"])
        won  = won[won["leader"].isin(_scope)]
        pipe = pipe[pipe["leader"].isin(_scope)]
        hld  = hld[hld["leader"].isin(_scope)]
    hold_by_ae = (hld.groupby("account_executive")["amount"].sum().to_dict()
                  if len(hld) else {})

    # Ciclo de vendas (dias) = validação − criação, só nos won com created_date.
    won["cycle_days"] = (won["valid_date"] - won["created_date"]).dt.days
    cyc = won.loc[won["cycle_days"].notna() & (won["cycle_days"] >= 0), "cycle_days"]

    def _stats(frame):
        amt = float(frame["amount"].sum())
        n = int(len(frame))
        return {"mrr": amt, "deals": n, "ticket": (amt / n) if n else 0.0}

    acq = _stats(won[won["deal_type"] == "Aquisition"])
    exp = _stats(won[won["deal_type"] == "Up & cross sell"])

    kpis = {
        "realized": float(won["amount"].sum()),
        "deals_won": int(len(won)),
        "ticket": (float(won["amount"].sum()) / len(won)) if len(won) else 0.0,
        "cycle_avg": float(cyc.mean()) if len(cyc) else 0.0,
        "cycle_median": float(cyc.median()) if len(cyc) else 0.0,
        "cycle_n": int(len(cyc)),
        "pipe_gross": float(pipe["amount"].sum()),
        "pipe_weighted": float(pipe["weighted"].sum()),
        "pipe_deals": int(len(pipe)),
        "projection": float(won["amount"].sum()) + float(pipe["weighted"].sum()),
        "n_ae": int(won.loc[~won["account_executive"].isin(NON_AE_USERS),
                            "account_executive"].nunique()),
    }

    # Quebra por líder: realizado, deals, ticket, ciclo médio, pipeline ponderado, projeção.
    by_leader = []
    leaders = sorted(set(won["leader"].dropna()) | set(pipe["leader"].dropna()))
    for ld in leaders:
        w = won[won["leader"] == ld]
        p = pipe[pipe["leader"] == ld]
        c = w.loc[w["cycle_days"].notna() & (w["cycle_days"] >= 0), "cycle_days"]
        realized = float(w["amount"].sum())
        weighted = float(p["weighted"].sum())
        by_leader.append({
            "leader": ld,
            "realized": realized,
            "deals": int(len(w)),
            "ticket": (realized / len(w)) if len(w) else 0.0,
            "cycle": float(c.mean()) if len(c) else None,
            "pipe_weighted": weighted,
            "projection": realized + weighted,
        })
    by_leader.sort(key=lambda r: r["projection"], reverse=True)

    # ----- Metas (quota) + meta de new logos por AE.
    # Quando o período é o Q3/2026, a fonte da verdade é o Q3_ROSTER (time + quota
    # + meta de new logos informados pela liderança). Fora do Q3, cai na quota da
    # curated (quota_in_brl, MRR) somada nos quarters do período — sem meta de
    # new logos (só existe pro Q3). Realizado (deal amount) é comparável à quota
    # (mesma unidade MRR). -----
    period_is_q3 = (from_d >= Q3_START and to_d <= Q3_END)

    # New logos REALIZADOS = deals de Aquisição ganhos no período, por AE.
    newlogos_actual = (won[won["deal_type"] == "Aquisition"]
                       .groupby("account_executive").size()
                       if len(won) else pd.Series(dtype=int))

    newlogos_goal = {}          # meta de new logos por AE (só Q3)
    roster_meta = {}            # leader/region p/ AEs do roster sem deal no período
    if period_is_q3:
        scope = expand_leader(f["leader"]) if f["leader"] else None
        quota_by_ae = {}
        for name, v in Q3_ROSTER.items():
            if f["region"] and Q3_ROSTER_REGION != f["region"]:
                continue
            if scope is not None and v["leader"] not in scope:
                continue
            if f["ae"] and name != f["ae"]:
                continue
            quota_by_ae[name] = float(v["quota"])
            newlogos_goal[name] = int(v["new_logos"])
            roster_meta[name] = {"leader": v["leader"], "region": Q3_ROSTER_REGION}
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
                      | set(pipe["account_executive"].dropna())
                      | {a for a, q in quota_by_ae.items() if q > 0}
                      | {a for a, hv in hold_by_ae.items() if hv}
                      | set(newlogos_goal.keys()))
    for ae in ae_names:
        w = won[won["account_executive"] == ae]
        p = pipe[pipe["account_executive"] == ae]
        c = w.loc[w["cycle_days"].notna() & (w["cycle_days"] >= 0), "cycle_days"]
        realized = float(w["amount"].sum())
        weighted = float(p["weighted"].sum())
        # leader/region: primeiro won, senão pipeline, senão o snapshot/roster.
        src = w if len(w) else p
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
            "is_manager": ae in NON_AE_USERS,
            "realized": realized,                       # Won validado (PO Won)
            "hold": hold,                               # Won em aprovação financeira
            "won_total": realized + hold,               # ranking = validado + hold
            "deals": int(len(w)),
            "ticket": (realized / len(w)) if len(w) else 0.0,
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
            r["quota"] = LEADER_BOARD_TARGET.get(r["leader"], r["quota"])
            r["attainment"] = (r["realized"] / r["quota"]) if r["quota"] else None
        _rev_items = [{"name": r["name"], "short": r["short"], "leader": r["leader"],
                       "realized": r["realized"], "quota": r["quota"],
                       "attainment": r["attainment"]} for r in _sort_att(_lr, "attainment")]
        rank = _build_rank_chart(_rev_items, _drill_base, axis_mode="currency",
                                 drill_path=_rk_path, drill_key=_rk_key)
        rank_nl = None
        if period_is_q3:
            _lnl = [r for r in _leader_rollup(by_ae, f["leader"], ["nl_real", "nl_goal"])
                    if r["nl_goal"] > 0]
            for r in _lnl:
                r["attainment"] = (r["nl_real"] / r["nl_goal"]) if r["nl_goal"] else None
            if _lnl:
                _nl_items = [{"name": r["name"], "short": r["short"], "leader": r["leader"],
                              "realized": int(r["nl_real"]), "quota": int(r["nl_goal"]),
                              "attainment": r["attainment"]} for r in _sort_att(_lnl, "attainment")]
                rank_nl = _build_rank_chart(_nl_items, _drill_base, axis_mode="count",
                                            drill_path=_rk_path, drill_key=_rk_key)
    else:
        # Chart 1 — RECEITA: Realizado × Quota (R$) + % atingimento. Ordenado por
        # % de atingimento DECRESCENTE. Geometria pré-computada em _build_rank_chart.
        _rev = _sort_att(_sellers, "attainment")
        _rev_items = [{"name": r["account_executive"], "short": short_name(r["account_executive"]),
                       "leader": r["leader"], "realized": r["realized"], "quota": r["quota"],
                       "attainment": r["attainment"]} for r in _rev]
        rank = _build_rank_chart(_rev_items, _drill_base, axis_mode="currency",
                                 drill_path="/overview/ae")

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
                                        drill_path="/overview/ae")

    # Totais de meta do time. IMPORTANTE: attainment compara MESMA população —
    # numerador (realizado) e denominador (quota) só dos AEs com meta (carriers de
    # quota). Vendas de gestor e de AEs fora do roster entram em "Vendas realizadas"
    # (total honesto), mas NÃO no attainment (senão infla, pois não têm quota).
    quota_carriers = [r for r in by_ae if r["quota"] > 0]
    total_quota = float(sum(r["quota"] for r in by_ae if not r["is_manager"]))  # soma das quotas individuais no escopo
    carrier_realized = float(sum(r["realized"] for r in quota_carriers))
    # Meta de RECEITA (card): no TIME INTEIRO usa a meta top-down FIXA
    # (Q3_TEAM_REVENUE_META, direto no card). "Time inteiro" = sem filtro de AE/região
    # e sem líder OU com o líder = VP (Pedro Mendes), que expande p/ todo o time.
    # Ao filtrar por um líder/AE/região específico, cai na soma das quotas individuais
    # daquele escopo (quotas individuais ficam como estão). Fora do Q3, usa a curated.
    is_whole_team = (not f["ae"] and not f["region"]
                     and f["leader"] in ("", "Pedro Mendes"))
    if period_is_q3 and is_whole_team:
        meta_receita = float(Q3_TEAM_REVENUE_META)
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
                    "date": (r["po_emission_date"].strftime("%d/%m/%Y")
                             if pd.notna(r["po_emission_date"]) else "—"),
                })
            return out
        deal_rows = _mk_deals(won, "validado") + _mk_deals(hld, "aprovação")
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
    # Recebidas total agora soma os 3 buckets (Inbound + Outbound + Expansão).
    by_ae["rcv_total"]  = by_ae["rcv_inbound"] + by_ae["rcv_outbound"] + by_ae["rcv_exp"]
    by_ae["pct_auto"]   = by_ae.apply(
        lambda r: (r["auto_total"] / r["total_generated"] * 100) if r["total_generated"] else 0,
        axis=1,
    )

    # Meta semanal por AE: 3 opps × semanas decorridas no período
    weekly_goal = _weekly_ae_goal(from_d, to_d)
    # Reescala o eixo Y pra meta ser visível mesmo se ninguém bater.
    # `.max()` em série vazia → NaN, e NaN é truthy, então `or 0` não basta.
    _auto_max = by_ae["auto_total"].max() if len(by_ae) else 0
    _rcv_max  = by_ae["rcv_total"].max()  if len(by_ae) else 0
    max_auto = max(int(_auto_max) if pd.notna(_auto_max) else 0, weekly_goal, 1)
    max_rcv  = max(int(_rcv_max)  if pd.notna(_rcv_max)  else 0, 1)

    totals = {
        "n_ae": len(by_ae),
        "total_generated": int(by_ae["total_generated"].sum()),
        "auto_total":  int(by_ae["auto_acq"].sum() + by_ae["auto_exp"].sum()),
        "rcv_total":   int(by_ae["rcv_inbound"].sum() + by_ae["rcv_outbound"].sum() + by_ae["rcv_exp"].sum()),
        "acq_total":   int(by_ae["auto_acq"].sum() + by_ae["rcv_inbound"].sum() + by_ae["rcv_outbound"].sum()),
        "exp_total":   int(by_ae["auto_exp"].sum() + by_ae["rcv_exp"].sum()),
        "auto_acq":    int(by_ae["auto_acq"].sum()),
        "auto_exp":    int(by_ae["auto_exp"].sum()),
        "rcv_inbound": int(by_ae["rcv_inbound"].sum()),
        "rcv_outbound":int(by_ae["rcv_outbound"].sum()),
        "rcv_exp":     int(by_ae["rcv_exp"].sum()),
    }

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
        # 5 buckets agora: auto_acq, auto_exp, received_inbound, received_outbound, received_exp
        for col in ["auto_acq","auto_exp","received_inbound","received_outbound","received_exp"]:
            if col not in gp.columns: gp[col] = 0
        gp["total"] = (gp["auto_acq"] + gp["auto_exp"]
                       + gp["received_inbound"] + gp["received_outbound"] + gp["received_exp"])
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
        max_auto=max_auto, max_rcv=max_rcv,
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


def _entity_filter(df, name, grain, leader=None, region=None):
    """Filtro do drill-down: com `name` → um AE/líder; sem `name` → TOTAL (todos
    os AEs, respeitando filtros opcionais de líder/região da página). Assume que
    ALLOWED_LEADERS/HIDDEN já foram aplicados pelo caller."""
    if name:
        return _filter_entity(df, name, grain)
    if region:
        df = df[df["region"] == region]
    if leader:
        df = df[df["leader"].isin(expand_leader(leader))]
    return df


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
    deals = _get_demand_deals(force=request.args.get("refresh") == "1")
    rows = _demand_deals_rows(
        deals, from_d, to_d,
        name=None if is_total else name, grain=grain,
        leader=request.args.get("leader"), region=request.args.get("region"),
        source=src,
    )
    title = "Geração de Demanda — total" if is_total else name
    amount = float(sum(r["amount"] for r in rows))
    cols = [
        {"key": "deal_name",  "label": "Deal", "wrap": True, "link": "deal_url"},
        {"key": "deal_type",  "label": "Tipo"},
        {"key": "qual_owner", "label": "Qualification owner"},
        {"key": "stage",      "label": "Estágio"},
        {"key": "amount",     "label": "Valor",     "type": "money"},
        {"key": "created_date", "label": "Criado em", "type": "date"},
    ]
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
    return ids or None


def _stalled_drill(df, name, grain, days_col, stage_ids_col="stage_curr",
                   subtitle_suffix="parado", leader=None, region=None, title=None,
                   stage_ids=None):
    """Payload do drill-down de inatividade. Com `name` → um AE/líder; sem `name`
    → TOTAL (todos os AEs). Marca stalled pelo threshold do stage e devolve só os
    parados, ordenados por dias desc. Mostra a coluna AE no total e na visão líder.
    `stage_ids` (dos chips da legenda) restringe aos estágios selecionados."""
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    df = _entity_filter(df, name, grain, leader, region)
    if stage_ids:
        df = df[df[stage_ids_col].isin(stage_ids)]
    rows = []
    if len(df):
        tmap = _deal_type_map()
        df = df.copy()
        df = df[df.apply(lambda r: _STALLED_THRESHOLDS.get(r[stage_ids_col], 999) < r[days_col],
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
    payload = _stalled_drill(bq.pipeline_stalled(force_refresh=request.args.get("refresh") == "1"),
                             name, grain, days_col="days_stalled",
                             subtitle_suffix="parado (sem mudança de stage)",
                             leader=request.args.get("leader"), region=request.args.get("region"),
                             title=name or "Stale — total",
                             stage_ids=_stalled_stage_ids_arg())
    return jsonify(payload)


def _stale_wow_deals_payload(dw, prev_sun, last_sun, title_all, days_label):
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
    dw = dw[~dw["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    dw = _entity_filter(dw, name, grain,
                        request.args.get("leader"), request.args.get("region"))

    def _stalled_at(stage, days):
        th = _STALLED_THRESHOLDS.get(stage)
        return th is not None and days is not None and days > th
    dw = dw.copy()
    dw["parado_curr"] = dw.apply(lambda r: _stalled_at(r["stage_curr"], r["days_curr"]), axis=1)
    dw["parado_prev"] = dw.apply(lambda r: _stalled_at(r["stage_prev"], r["days_prev"]), axis=1)

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
                                 f"{prev_sun.isoformat()} 15:00:00 UTC")
    return jsonify(_stale_wow_deals_payload(dw, prev_sun, last_sun,
                                            "Movimentação WoW · Stale", "Dias parado"))


@app.route("/pipeline/stalled-activity/wow-deals")
def pipeline_activity_wow_deals():
    """Drill da Movimentação WoW da página Inatividade (dias sem retorno de
    contato). Rota/arquivos seguem com o nome antigo 'stalled-activity' — só o
    rótulo de tela mudou, pra não quebrar links salvos."""
    last_sun, prev_sun = _last_two_sundays()
    dw = bq.pipeline_activity_wow(f"{last_sun.isoformat()} 15:00:00 UTC",
                                  f"{prev_sun.isoformat()} 15:00:00 UTC")
    return jsonify(_stale_wow_deals_payload(dw, prev_sun, last_sun,
                                            "Movimentação WoW · Inatividade",
                                            "Dias sem retorno"))


@app.route("/pipeline/stalled-activity/deals")
def pipeline_activity_deals():
    """Drill-down: deals sem retorno de contato (inatividade por atividade), por
    AE/líder ou TOTAL (sem name)."""
    name = (request.args.get("name") or "").strip()
    grain = "leader" if request.args.get("grain") == "leader" else "ae"
    if name and grain == "ae" and name in HIDDEN_FROM_AE_VIEWS:
        return jsonify(_empty_drill(name))
    payload = _stalled_drill(bq.pipeline_activity(force_refresh=request.args.get("refresh") == "1"),
                             name, grain, days_col="days_since_activity",
                             subtitle_suffix="sem retorno de contato",
                             leader=request.args.get("leader"), region=request.args.get("region"),
                             title=name or "Inatividade — total",
                             stage_ids=_stalled_stage_ids_arg())
    payload["columns"] = _with_ae_col([
        {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
        {"key": "deal_type", "label": "Tipo"},
        {"key": "stage",     "label": "Estágio"},
        {"key": "amount",    "label": "Valor",              "type": "money"},
        {"key": "days",      "label": "Dias sem atividade", "type": "int"},
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
    dd = _get_forecast_deals()
    dd = dd[dd["leader"].isin(ALLOWED_LEADERS)]
    dd = dd[~dd["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    dd = _entity_filter(dd, name, grain, request.args.get("leader"), request.args.get("region"))

    won = dd[dd["is_won"] & dd["po_emission_date"].notna()
             & (dd["po_emission_date"] >= lo) & (dd["po_emission_date"] <= hi)]
    pipe = dd[(~dd["is_won"]) & dd["close_date"].notna()
              & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)]

    rows = []
    for _, r in won.iterrows():
        amt = float(r["amount"] or 0)
        rows.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                     "ae": r["account_executive"], "deal_type": r.get("deal_type") or "—",
                     "categoria": "Won (realizado)", "amount": amt, "ponderado": amt,
                     "data": r["po_emission_date"].date().isoformat() if pd.notna(r["po_emission_date"]) else ""})
    for _, r in pipe.iterrows():
        rows.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                     "ae": r["account_executive"], "deal_type": r.get("deal_type") or "—",
                     "categoria": r.get("forecast_category") or "Pipeline", "amount": float(r["amount"] or 0),
                     "ponderado": float(r.get("weighted") or 0),
                     "data": r["close_date"].date().isoformat() if pd.notna(r["close_date"]) else ""})
    rows.sort(key=lambda x: x["data"], reverse=True)
    amount = float(sum(x["amount"] for x in rows))
    return jsonify({
        "title": name or "Forecast — total",
        "subtitle": (f"{len(won)} realizado(s) + {len(pipe)} em pipeline · "
                     f"valor bruto R$ {round(amount):,}".replace(",", ".")),
        "columns": _with_ae_col(_DRILL_FORECAST_COLUMNS, "leader" if (grain == "leader" or not name) else "ae"),
        "rows": rows,
    })


def _newlogo_goal_aes(from_d, to_d, region=None, leader=None, ae=None):
    """AEs que CARREGAM meta de new logo no período (Q3_ROSTER). Fora do Q3 não
    existe meta → conjunto vazio. Espelha `newlogos_goal` de _overview_page: o KPI
    'Realizado de new logo' só soma esses AEs, então o drill-down precisa do mesmo
    recorte pra a contagem bater com o card."""
    if not (from_d >= Q3_START and to_d <= Q3_END):
        return set()
    scope = expand_leader(leader) if leader else None
    out = set()
    for nm, v in Q3_ROSTER.items():
        if region and Q3_ROSTER_REGION != region:
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
    in_win = dd["po_emission_date"].notna() & (dd["po_emission_date"] >= lo) & (dd["po_emission_date"] <= hi)
    if scope_all:
        won = dd[dd["is_won"] & in_win].copy()
        hld = dd.iloc[0:0].copy()
    else:
        won = dd[dd["is_won"] & in_win & ~dd["deal_id"].isin(hold_ids)].copy()
        hld = dd[in_win & dd["deal_id"].isin(hold_ids)].copy()
    lmap = _period_leader_map()
    won["leader"] = _attr_deal_leader(won, "po_emission_date", lmap)
    hld["leader"] = _attr_deal_leader(hld, "po_emission_date", lmap)
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

    def _mk(frame, status):
        out = []
        for _, r in frame.iterrows():
            out.append({"deal_name": r.get("deal_name") or "—", "deal_url": _hs_deal_url(r.get("deal_id")),
                        "ae": r["account_executive"], "deal_type": r.get("deal_type") or "—",
                        "amount": float(r["amount"] or 0), "status": status,
                        "data": r["po_emission_date"].date().isoformat() if pd.notna(r["po_emission_date"]) else ""})
        return out

    kind = (request.args.get("kind") or "").strip()

    # Ticket médio (Sales Overtime): TODOS os deals ganhos do trimestre, com o
    # tipo — a barra de aquisição e a de expansão saem da mesma população.
    if kind == "ticket":
        _tipo = {"Aquisition": "Aquisição", "Up & cross sell": "Expansão"}
        rows = []
        for _, r in won.iterrows():
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
        if kind == "newlogo":
            carriers = _newlogo_goal_aes(lo.date(), hi.date(),
                                         region=request.args.get("region"),
                                         leader=request.args.get("leader"),
                                         ae=(name if grain == "ae" else None))
            sel = sel[sel["account_executive"].isin(carriers)]
        rows = _mk(sel, "Validado")
        rows.sort(key=lambda x: x["data"], reverse=True)
        amount = float(sum(x["amount"] for x in rows))
        _titles = {"acq": "Deals de aquisição", "exp": "Deals de expansão",
                   "newlogo": "New logos"}
        cols = [{"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
                {"key": "ae",        "label": "AE"},
                {"key": "amount",    "label": "MRR",   "type": "money"},
                {"key": "data",      "label": "PO em", "type": "date"}]
        return jsonify({
            "title": _titles[kind] + (f" · {name}" if name else ""),
            "subtitle": _drill_money_subtitle(len(rows), amount) + " no período",
            "columns": cols, "rows": rows,
        })

    rows = _mk(won, "Validado") + _mk(hld, "Em aprovação")
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
            "rcv_inbound":  int(subset["rcv_inbound"].sum()),
            "rcv_outbound": int(subset["rcv_outbound"].sum()),
            "rcv_exp":      int(subset["rcv_exp"].sum()),
        })
    by_leader = pd.DataFrame(rows)
    if len(by_leader):
        by_leader["auto_total"] = by_leader["auto_acq"] + by_leader["auto_exp"]
        by_leader["rcv_total"]  = by_leader["rcv_inbound"] + by_leader["rcv_outbound"] + by_leader["rcv_exp"]
        by_leader["total_generated"] = by_leader["auto_total"] + by_leader["rcv_total"]
        by_leader["pct_auto"]   = by_leader.apply(
            lambda r: (r["auto_total"] / r["total_generated"] * 100) if r["total_generated"] else 0,
            axis=1,
        )
        by_leader = by_leader.sort_values("total_generated", ascending=False).reset_index(drop=True)
    else:
        by_leader = pd.DataFrame(columns=[
            "leader","account_executive","region","role","team_size","goal",
            "auto_acq","auto_exp","rcv_inbound","rcv_outbound","rcv_exp",
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
        "acq_total":   int(by_leader["auto_acq"].sum() + by_leader["rcv_inbound"].sum() + by_leader["rcv_outbound"].sum()),
        "exp_total":   int(by_leader["auto_exp"].sum() + by_leader["rcv_exp"].sum()),
        "auto_acq":    int(by_leader["auto_acq"].sum()),
        "auto_exp":    int(by_leader["auto_exp"].sum()),
        "rcv_inbound": int(by_leader["rcv_inbound"].sum()),
        "rcv_outbound":int(by_leader["rcv_outbound"].sum()),
        "rcv_exp":     int(by_leader["rcv_exp"].sum()),
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
        for col in ["auto_acq","auto_exp","received_inbound","received_outbound","received_exp"]:
            if col not in gp.columns: gp[col] = 0
        gp["total"] = (gp["auto_acq"] + gp["auto_exp"]
                       + gp["received_inbound"] + gp["received_outbound"] + gp["received_exp"])
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
    """Quota por AE numa janela. Na janela do Q3 a verdade é o Q3_ROSTER (inclui
    AE novo sem linha curada, que senão ficaria com quota 0 e sumiria do chart);
    fora dela soma a curada nos quarters que intersectam a janela."""
    if cov_from >= Q3_START and cov_to <= Q3_END:
        scope = expand_leader(f["leader"]) if f["leader"] else None
        rows = []
        for name, v in Q3_ROSTER.items():
            if f["region"] and Q3_ROSTER_REGION != f["region"]:
                continue
            if scope is not None and v["leader"] not in scope:
                continue
            if f["ae"] and name != f["ae"]:
                continue
            rows.append({"account_executive": name, "quota_brl": float(v["quota"]),
                         "leader": v["leader"], "region": Q3_ROSTER_REGION})
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
    quarter do PO). Devolve (validated, hold, acq, exp) como Series por AE."""
    dd = fdeals[~fdeals["account_executive"].isin(DEPARTED_AES)].copy()
    if f["region"]:
        dd = dd[dd["region"] == f["region"]]
    if f["ae"]:
        dd = dd[dd["account_executive"] == f["ae"]]
    po = dd["po_emission_date"]
    wonv = dd[dd["is_won"] & po.notna() & ~dd["deal_id"].isin(hold_ids)
              & (po >= lo) & (po <= hi)].copy()
    hldv = dd[dd["deal_id"].isin(hold_ids) & po.notna()
              & (po >= lo) & (po <= hi)].copy()
    wonv["leader"] = _attr_deal_leader(wonv, "po_emission_date", lmap)
    hldv["leader"] = _attr_deal_leader(hldv, "po_emission_date", lmap)
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

    # ---- Janela da cobertura = TRIMESTRE VIGENTE (Q3) por padrão. Cobertura olha
    # pra frente: quanto da quota DESTE trimestre ainda falta. Se o usuário escolher
    # um período, respeita; senão Q3. (Sem isso, o df cai em TODOS os quarters e a
    # quota/realizado somam o histórico inteiro → remaining ~0 → cobertura estoura.)
    cov_from = pipe_from or Q3_START
    cov_to   = pipe_to   or Q3_END
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

    # ---- Quota por AE. MESMA fonte do /overview: na janela do Q3, a verdade é o
    # Q3_ROSTER (inclui AEs novos SEM linha curada — ex Breno/Diego/Gabriel — que de
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
        rank_src = _pipe_leader_df(by_ae, f["leader"])
        pipe_path, drill_key = "/leaders/pipeline", "leader"
    else:
        rank_src = by_ae
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
        # No M3 a janela é outro trimestre, então o Board Target (que é do
        # corrente) não vale — soma a quota do time do trimestre-alvo.
        e_rank_src = _pipe_leader_df(e_by_ae, f["leader"],
                                     use_board_target=not early["use_full_quota"])
    else:
        e_rank_src = e_by_ae
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


def _pipeline_activity_page(grain="ae"):
    """Inatividade por ATIVIDADE (V2): mesma cara da página de inatividade por
    stage, mas o eixo de tempo é 'dias desde o último RETORNO de um contato do
    deal' (email entrante / call conectada) em vez de 'dias no stage'. Reusa as
    thresholds por stage pra as duas visões serem comparáveis lado a lado.
    "Retorno" NÃO inclui reuniões — a tabela de meetings do BQ não tem
    associação a deal (precisaria da API); ver bq.pipeline_activity()."""
    ctx = base_context()
    f = ctx["filters"]
    force = request.args.get("refresh") == "1"

    # ----- 1) Estado atual (now) -----
    df_now = bq.pipeline_activity(force_refresh=force)
    df_now = _apply_roster_current(df_now)
    df_now = df_now[df_now["leader"].isin(ALLOWED_LEADERS)]
    df_now = df_now[~df_now["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        df_now = df_now[df_now["region"] == f["region"]]
    if f.get("leader"):
        df_now = df_now[df_now["leader"].isin(expand_leader(f["leader"]))]

    # Sinal extra: retorno de contato via WhatsApp (integrador grava como NOTE).
    # Só quando o HubSpot está configurado (token via Private App); sem token é
    # no-op e a aba segue com email+call. Nunca deixa o HubSpot derrubar a página.
    n_wa_returns = 0
    wa_active = hubspot.is_configured()
    if wa_active and len(df_now):
        try:
            wa = hubspot.last_client_returns(
                df_now["deal_id"].astype(str).tolist(), force_refresh=force)
            if wa:
                wa_ts = (df_now["deal_id"].astype(str).map(wa)
                         .pipe(pd.to_datetime, errors="coerce", utc=True))
                # last_activity efetivo = mais recente entre email/call e WhatsApp
                prev = pd.to_datetime(df_now["last_activity_at"], errors="coerce", utc=True)
                combined = prev.where(prev >= wa_ts, wa_ts)  # NaT-safe: pega o maior
                combined = combined.fillna(prev).fillna(wa_ts)
                df_now["last_activity_at"] = combined
                df_now["has_activity"] = df_now["has_activity"] | wa_ts.notna()
                n_wa_returns = int(wa_ts.notna().sum())
                # Recalcula dias desde a atividade com a origem justa (entrada no stage)
                base = combined.fillna(
                    pd.to_datetime(df_now["stage_changed_at"], errors="coerce", utc=True))
                today = pd.Timestamp.utcnow().normalize()
                df_now["days_since_activity"] = (today - base.dt.normalize()).dt.days
        except Exception as e:  # noqa: BLE001 — HubSpot fora não pode quebrar a aba
            app.logger.warning("WhatsApp returns indisponível: %s", e)
            wa_active = False

    # "Inativo por atividade" = dias sem retorno > threshold do stage
    df_now["is_stalled"] = df_now.apply(
        lambda r: _STALLED_THRESHOLDS.get(r["stage_curr"], 999) < r["days_since_activity"], axis=1)
    stalled_now = df_now[df_now["is_stalled"]].copy()

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
        rows_now.sort(key=lambda r: r["total"], reverse=True)

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

    # Cobertura: quantos deals têm retorno real de contato (vs. fallback stage)
    n_deals_total   = int(len(df_now))
    n_with_activity = int(df_now["has_activity"].sum()) if len(df_now) else 0

    # ----- 2) WoW (saiu vs entrou da inatividade por atividade) -----
    last_sun, prev_sun = _last_two_sundays()
    t_curr_iso = f"{last_sun.isoformat()} 15:00:00 UTC"
    t_prev_iso = f"{prev_sun.isoformat()} 15:00:00 UTC"
    dw = bq.pipeline_activity_wow(t_curr_iso, t_prev_iso, force_refresh=force)
    dw = _apply_roster_current(dw)
    dw = dw[dw["leader"].isin(ALLOWED_LEADERS)]
    dw = dw[~dw["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        dw = dw[dw["region"] == f["region"]]
    if f.get("leader"):
        dw = dw[dw["leader"].isin(expand_leader(f["leader"]))]

    def _stalled_at(stage, days):
        th = _STALLED_THRESHOLDS.get(stage)
        return th is not None and days is not None and days > th

    dw["parado_curr"] = dw.apply(lambda r: _stalled_at(r["stage_curr"], r["days_curr"]), axis=1)
    dw["parado_prev"] = dw.apply(lambda r: _stalled_at(r["stage_prev"], r["days_prev"]), axis=1)
    dw_saiu   = dw[(dw["parado_prev"]) & (~dw["parado_curr"])]
    dw_entrou = dw[(~dw["parado_prev"]) & (dw["parado_curr"])]

    # Breakdown por estágio-âncora — mesmos chips/recompute da página Stale.
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
            r["net"] = int(r["saiu"]) - int(r["entrou"])
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
        rows_wow=rows_wow,
        wow_y_max=wow_y_max,
        wow_total_saiu=int(sum(r["saiu"]   for r in rows_wow)),
        wow_total_entrou=int(sum(r["entrou"] for r in rows_wow)),
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
    df_now = bq.pipeline_stalled(force_refresh=force)
    df_now = _apply_roster_current(df_now)   # roster Q3 + remove quem saiu (BQ traz líder cru)
    df_now = df_now[df_now["leader"].isin(ALLOWED_LEADERS)]
    df_now = df_now[~df_now["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        df_now = df_now[df_now["region"] == f["region"]]
    if f.get("leader"):
        df_now = df_now[df_now["leader"].isin(expand_leader(f["leader"]))]

    # Marca cada linha como "parado" segundo a threshold do stage
    df_now["is_stalled"] = df_now.apply(
        lambda r: _STALLED_THRESHOLDS.get(r["stage_curr"], 999) < r["days_stalled"], axis=1)
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
    dw = bq.pipeline_stalled_wow(t_curr_iso, t_prev_iso, force_refresh=force)
    dw = _apply_roster_current(dw)           # roster Q3 + remove quem saiu (BQ traz líder cru)
    dw = dw[dw["leader"].isin(ALLOWED_LEADERS)]
    dw = dw[~dw["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        dw = dw[dw["region"] == f["region"]]
    if f.get("leader"):
        dw = dw[dw["leader"].isin(expand_leader(f["leader"]))]

    def _stalled_at(stage, days):
        th = _STALLED_THRESHOLDS.get(stage)
        return th is not None and days is not None and days > th

    dw["parado_curr"] = dw.apply(lambda r: _stalled_at(r["stage_curr"], r["days_curr"]), axis=1)
    dw["parado_prev"] = dw.apply(lambda r: _stalled_at(r["stage_prev"], r["days_prev"]), axis=1)
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
        wow_total_saiu=int(sum(r["saiu"]   for r in rows_wow)),
        wow_total_entrou=int(sum(r["entrou"] for r in rows_wow)),
        snap_curr_label=_fmt_date_short(last_sun) + " · 12h",
        snap_prev_label=_fmt_date_short(prev_sun) + " · 12h",
        grain=grain,
    )


# ============================================================
# Pipeline Total — todo o dinheiro do funil, cortado por qualquer eixo
# ============================================================
# Eixo X = estágio do funil (onde o dinheiro ESTÁ). Os 3 cortes de negócio
# (deal type, source, forecast category) funcionam como filtro E como
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
    "fcat": {
        "col": "forecast_category", "label": "Forecast Category",
        # Ordem do mais comprometido pro menos. `OMIT` = "Not forecasted" e o
        # bucket vazio são a MAIORIA do pipe aberto (863 de 1349 sem categoria
        # nenhuma), então precisam existir como opção — se ficassem de fora o
        # gráfico empilhado não fecharia com o total e o número pareceria errado.
        "values": [
            ("commit",    "COMMIT",     "Commit",         "#047857", "#065F46"),
            ("bestcase",  "BEST_CASE",  "Best Case",      "#10B981", "#047857"),
            ("probable",  "PROBABLE",   "Probable",       "#2563EB", "#1D4ED8"),
            ("funnel",    "PIPELINE",   "Funnel",         "#93C5FD", "#1E40AF"),
            ("omit",      "OMIT",       "Not forecasted", "#F59E0B", "#B45309"),
            (_PT_DIM_NONE, None,        "Sem categoria",  "#94A3B8", "#475569"),
        ],
    },
}


def _pt_dim_meta(dim: str) -> list[dict]:
    """Valores de uma dimensão como dicts prontos pro template (chip + segmento).

    Em `fcat`, anexa o PESO do modelo de forecast (`FORECAST_CATEGORY_PCT`, a
    mesma fonte da aba Forecast: Funnel 25 · Probable 50 · Best Case 75 ·
    Commit 100). Vem de lá e não de uma cópia local justamente pra as duas abas
    nunca discordarem sobre quanto vale um Best Case."""
    out = []
    for k, raw, lbl, c, ink in _PT_DIMS[dim]["values"]:
        item = {"key": k, "raw": raw, "label": lbl, "color": c, "ink": ink}
        if dim == "fcat":
            # OMIT é 0% no modelo; "sem categoria" não tem peso definido (None) —
            # os dois são explicitamente NÃO ponderáveis, e a página diz isso em
            # vez de fingir que valem 0 ou 100.
            item["pct"] = FORECAST_CATEGORY_PCT.get(raw) if raw is not None else None
        out.append(item)
    return out


def _pt_apply_dim_filter(df: pd.DataFrame, dim: str, sel: str) -> pd.DataFrame:
    """Aplica o filtro de uma dimensão pela chave curta da URL. Chave
    desconhecida = filtro ignorado (URL editável não pode virar 500)."""
    if not sel:
        return df
    entry = next((v for v in _PT_DIMS[dim]["values"] if v[0] == sel), None)
    if entry is None:
        return df
    col, raw = _PT_DIMS[dim]["col"], entry[1]
    if raw is None:                      # bucket "vazio no HubSpot"
        return df[df[col].isna() | (df[col].astype(str).str.strip() == "")]
    return df[df[col] == raw]


def _pt_series_key(df: pd.DataFrame, dim: str) -> pd.Series:
    """Coluna crua → chave curta da dimensão, com tudo que não mapeia caindo no
    bucket vazio. Garante que a soma dos segmentos == total da barra."""
    col = _PT_DIMS[dim]["col"]
    lookup = {raw: k for k, raw, *_ in _PT_DIMS[dim]["values"] if raw is not None}
    return df[col].map(lookup).fillna(_PT_DIM_NONE)


def _pt_filtered_frame(f: dict, force: bool = False):
    """Universo do Pipeline Total já filtrado: deals ABERTOS do Sales BR com
    roster aplicado + todos os cortes da página (região, líder, AE, close date,
    deal type, source, forecast category).

    Compartilhado pelo gráfico e pelo drill-down DE PROPÓSITO: se cada um
    montasse seu próprio filtro, a lista explodida passaria a discordar da barra
    no primeiro ajuste — é o bug clássico de drill. Devolve (df, n_sem_close),
    onde n_sem_close = deals descartados por não ter close date quando há janela.
    """
    df = bq.pipeline_total_raw(force_refresh=force)
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]

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

    # ---- Close date (opcional) ----
    lo, hi = _parse_filter_date(f.get("date_from")), _parse_filter_date(f.get("date_to"))
    n_sem_close = 0
    if lo or hi:
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

    title_bits = []
    if len(df):
        df["_sid"] = _pt_stage_ids(df)
        if sid:
            df = df[df["_sid"] == sid]
            title_bits.append(_pt_stage_label(sid))
        if by in _PT_DIMS and seg:
            df = df[_pt_series_key(df, by) == seg]
            lbl = next((v["label"] for v in _pt_dim_meta(by) if v["key"] == seg), seg)
            title_bits.append(lbl)

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
                # Rótulo já com o peso ("Commit · 100%") — evita o leitor ter que
                # lembrar a tabela do modelo pra interpretar o Ponderado.
                "fcat":      (fcat_label.get(raw, "—")
                              + (f" · {int(pct*100)}%" if pct is not None else "")),
                "weighted":  (amt * pct) if pct is not None else None,
                "deal_type": dtype_label.get(r.get("deal_type"), r.get("deal_type") or "—"),
                "source":    r.get("deal_source_agg") or "—",
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
    forecast category, líder, AE e close date.

    Universo = deals ABERTOS (nem won nem lost) do Sales BR: a pergunta é "quanto
    dinheiro tem no meu funil hoje", então deal fechado não entra.

    Sobre o close date: é OPCIONAL aqui (ver PIPELINE_TOTAL_PATHS). Metade dos
    deals abertos não tem a data preenchida no HubSpot; a página conta quantos
    ficaram de fora quando há janela, em vez de encolher o total sem explicar."""
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

    # Filtros ativos, sem as chaves vazias — é a base das URLs do switcher de
    # empilhamento. Montado aqui (e não no template) porque url_for com valor ""
    # emite `?dtype=&source=` na URL, que suja o link e vira chip fantasma.
    keep = {k: v for k, v in f.items() if v and k != "role"}

    return render_template(
        "pipeline_total.html", ctx=ctx, grain=grain,
        rows=rows, segs=segs, by=by, seg_totals=seg_totals,
        total_amount=total_amount, total_deals=total_deals, max_amount=max_amount,
        ticket=(total_amount / total_deals if total_deals else 0.0),
        n_sem_close=n_sem_close, keep=keep, weighted_total=weighted_total,
        weighted_cov=weighted_cov,
        dims=[{"key": k, "label": v["label"], "opts": _pt_dim_meta(k)}
              for k, v in _PT_DIMS.items()],
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

    df = bq.pipeline_wow_snapshot(t_curr_iso, t_prev_iso, force_refresh=force)
    df = _apply_roster_current(df)           # roster Q3 + remove quem saiu (BQ traz líder cru)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        df = df[df["region"] == f["region"]]
    if f.get("leader"):
        df = df[df["leader"].isin(expand_leader(f["leader"]))]

    # Movimentação por TRANSIÇÃO (histórico completo na janela, não diff de
    # snapshots): captura o caminho INTRA-semana — deal criado e movido de
    # Scheduled a Scoping na mesma semana conta em cada etapa por onde passou
    # (o diff de pontas perdia esses). Uma linha por mudança de stage.
    tr = bq.funnel_transitions(t_curr_iso, t_prev_iso, force_refresh=force)
    tr = _apply_roster_current(tr)
    tr = tr[tr["leader"].isin(ALLOWED_LEADERS)]
    tr = tr[~tr["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    if f.get("region"):
        tr = tr[tr["region"] == f["region"]]
    if f.get("leader"):
        tr = tr[tr["leader"].isin(expand_leader(f["leader"]))]
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


def _wow_snapshot_filtered():
    """Snapshot WoW dos 2 domingos comparados (padrão: os 2 últimos), com
    roster/allowed/hidden aplicados. Só as barras de ESTADO (prev/curr) usam
    isto — o movimento vem de `_wow_events_filtered`."""
    last_sun, prev_sun = _wow_window(get_filters())
    tc = f"{last_sun.isoformat()} 15:00:00 UTC"
    tp = f"{prev_sun.isoformat()} 15:00:00 UTC"
    df = bq.pipeline_wow_snapshot(tc, tp)
    df = _apply_roster_current(df)
    df = df[df["leader"].isin(ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    return df


def _wow_events_filtered(force=False):
    """Eventos da janela WoW com o MESMO recorte do snapshot. Janela FIXA
    (`_wow_fixed_pair`), a mesma da página — o drill não pode cair num par de
    domingos diferente do gráfico que foi clicado."""
    last_sun, prev_sun = _wow_fixed_pair()
    tc = f"{last_sun.isoformat()} 15:00:00 UTC"
    tp = f"{prev_sun.isoformat()} 15:00:00 UTC"
    ev = bq.wow_events(tc, tp, force_refresh=force)
    ev = _apply_roster_current(ev)
    ev = ev[ev["leader"].isin(ALLOWED_LEADERS)]
    ev = ev[~ev["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
    return ev


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

    def _lbl(sid):
        if not sid:
            return "—"
        return stage_display(sid)

    def _quando(ts):
        """Timestamp do evento em BRT — a página fala em "domingo 12h BRT", então
        mostrar UTC aqui confundiria a leitura. UTC−3 fixo, a mesma convenção que
        o resto do WoW já usa ao montar os snapshots como 15:00 UTC."""
        try:
            t = pd.Timestamp(ts)
            if t.tzinfo is not None:
                t = t.tz_convert(None)
            return (t - timedelta(hours=3)).strftime("%d/%m %H:%M")
        except (ValueError, TypeError, AttributeError):
            return "—"

    # Uma linha por EVENTO (não por deal): o mesmo deal pode aparecer 2× se
    # cresceu e depois avançou na mesma semana — é justamente o que o diff de
    # pontas escondia.
    rows = []
    for _, r in ev.iterrows():
        # `_c`, NÃO `cat`: reusar o nome sobrescrevia o `cat` do filtro (que vale
        # "" quando o drill traz várias categorias) com a categoria da ÚLTIMA
        # linha, e aí o título ganhava um sufixo aleatório ("· Novo") e o
        # subtítulo caía no ramo de categoria única, somando tudo como se fosse
        # uma só. Bug que já existia antes da migração.
        _c = r["category"]
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
            "categoria": _WOW_CAT_LABEL.get(_c, _c),
            "movimento": mov,
            "quando":    _quando(r["ts"]),
            "delta":     float(r["delta"]),
            "amount":    val,
        })
    rows.sort(key=lambda x: abs(x["delta"]), reverse=True)
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
        subtitle = (f"{_d} won · R$ {round(won):,}".replace(",", ".")) + " que saiu do pipe"
    elif cat == "late":
        subtitle = (f"{_d} · R$ {round(won):,}".replace(",", ".")) + " movidos pra Late stage"
    elif cat:
        # uma categoria só (novo/cresceu/encolheu/lost) → soma dela, não "saldo"
        _v = sum(x["delta"] for x in rows)
        subtitle = (f"{_d} · "
                    f"{'+' if _v >= 0 else '−'}R$ {abs(round(_v)):,}".replace(",", "."))
    else:
        subtitle = (f"{_d} · saldo "
                    f"{'+' if net >= 0 else '−'}R$ {abs(round(net)):,}".replace(",", "."))
        if won:
            _w_lbl = "movido p/ Late" if focus == "early" else "won"
            subtitle += f" · {_w_lbl} R$ {round(won):,}".replace(",", ".")
    _escopo = name or f"PoP {focus} — total"
    _sufixo = _WOW_CAT_LABEL.get(cat, "") if cat else ("" if len(cats) == 5 else "Saldo")
    return jsonify({
        "title": (f"{_escopo} · {_sufixo}" if _sufixo else _escopo),
        "subtitle": subtitle,
        "columns": _with_ae_col([
            {"key": "deal_name", "label": "Deal", "wrap": True, "link": "deal_url"},
            {"key": "categoria", "label": "Movimento"},
            {"key": "movimento", "label": "Estágio (de → para)"},
            {"key": "quando",    "label": "Quando", "align": "right"},
            {"key": "delta",     "label": "Δ (R$)", "type": "money_signed"},
            {"key": "amount",    "label": "Amount no evento", "type": "money"},
        ], "leader" if (grain == "leader" or not name) else "ae"),
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
                               f"{prev_sun.isoformat()} 15:00:00 UTC")
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
        return (url_for("pipeline_funnel_move_deals") + "?" +
                urllib.parse.urlencode({**_carry, "stage": stage, "dir": direction}))

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
        x = x[~x["account_executive"].isin(HIDDEN_FROM_AE_VIEWS)]
        if f.get("region"):
            x = x[x["region"] == f["region"]]
        if f.get("leader"):
            x = x[x["leader"].isin(expand_leader(f["leader"]))]
        return x

    # DUAS fontes, de propósito: o snapshot dos 2 domingos só responde "quanto
    # havia em cada ponta" (barras de ESTADO); todo o MOVIMENTO vem dos eventos
    # da janela, que enxergam o caminho intra-semana. Ver _wow_events_categorize.
    df = _scope(bq.pipeline_wow_snapshot(t_curr_iso, t_prev_iso, force_refresh=force))
    df["gp_prev"] = df["stage_prev"].apply(_hs_stage_group)
    df["gp_curr"] = df["stage_curr"].apply(_hs_stage_group)
    ev = _wow_events_categorize(
        _scope(bq.wow_events(t_curr_iso, t_prev_iso, force_refresh=force)), focus)

    # 5ª categoria do relatório: no EARLY é o que AVANÇOU pra Late (o conceito de
    # won saiu da tela, a pedido); no LATE segue sendo won. O slot visual (cards,
    # chart, legenda) é o mesmo — muda a categoria que o alimenta.
    fifth = "late" if focus == "early" else "won"

    # Per-AE (ou por líder, explodindo p/ ancestrais). Totais sempre do frame CRU
    # pra não duplicar diretor+gerente.
    ev_ent = _explode_leaders(ev, f.get("leader")) if grain == "leader" else ev
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


@app.route("/forecast")
def forecast():
    return _forecast_page("ae")


@app.route("/leaders/forecast")
def leaders_forecast():
    return _forecast_page("leader")


def _forecast_page(grain="ae"):
    """Forecast ponderado: deals com close date no quarter, nos estágios 1-6
    (Scheduled→Procurement), ponderados pela % da forecast category do HubSpot
    (Funnel 25 · Probable 50 · Best Case 75 · Commit 100 · Not forecasted 0).
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

    lo, hi = pd.Timestamp(from_d), pd.Timestamp(to_d)
    # Pipeline aberto (estágios 1-6): recorte pelo CLOSE DATE (quando esperam fechar).
    pipe = dd[(~dd["is_won"]) & dd["close_date"].notna()
              & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)]
    # Realizado (PO Won): quarter definido pela DATA DE VALIDAÇÃO (po_emission,
    # fallback close date) — priorizada sobre o close date do HubSpot. É isso que
    # joga os deals fechados em 1-3/abr (validados em março) pro Q1, não pro Q2.
    won = dd[dd["is_won"] & dd["valid_date"].notna()
             & (dd["valid_date"] >= lo) & (dd["valid_date"] <= hi)]

    cats = [{"key": _fc_key(c), "raw": c, "label": FORECAST_CATEGORY_LABEL[c],
             "pct": FORECAST_CATEGORY_PCT[c], "color": FORECAST_CATEGORY_COLOR[c],
             "ink": FORECAST_CATEGORY_INK[c]}
            for c in FORECAST_CATEGORY_ORDER]

    # ----- on-hold (MRR) por AE — total_mrr_on_hold_brl da curated (escolha do
    # usuário pro hold). Inclui gestores. Unidade = MRR → mostrado à parte. -----
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
    hold_by_ae = (aep.groupby("account_executive")["total_mrr_on_hold_brl"].sum()
                  if len(aep) else pd.Series(dtype=float))

    # ----- agrega por AE: pipeline ponderado + realizado (amount) + hold (MRR) -----
    rows = {}
    def _row(ae_name, leader, region):
        r = rows.get(ae_name)
        if r is None:
            r = {"account_executive": ae_name, "leader": leader, "region": region,
                 "is_manager": ae_name in NON_AE_USERS,
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
    by_ae.sort(key=lambda r: r["projection"], reverse=True)

    # Gráfico = Realizado (Won) + forecast ponderado por AE/líder (ambos deal amount).
    # Gestores ficam fora das barras; inclui quem tem realizado mesmo sem pipeline.
    chart_rows = [r for r in by_ae if not r["is_manager"] and (r["weighted"] > 0 or r["realized"] > 0)]
    max_projection = max((r["projection"] for r in chart_rows), default=0.0)

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

    range_label = f"{from_d.strftime('%d/%m/%Y')} → {to_d.strftime('%d/%m/%Y')}"
    return render_template(
        "forecast.html", ctx=ctx, cats=cats,
        by_ae=by_ae, chart_rows=chart_rows, by_cat=by_cat, totals=totals,
        not_forecasted=not_forecasted, max_projection=max_projection,
        range_label=range_label, grain=grain)


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
    won = dd[dd["is_won"] & dd["valid_date"].notna()
             & (dd["valid_date"] >= lo) & (dd["valid_date"] <= hi)]
    done_by_ae = won.groupby("account_executive")["amount"].sum()

    # Forecast por conversão = deals ABERTOS nos 3 estágios modelados, com close
    # date no período, ponderados pela probabilidade do segmento×estágio×tipo.
    openm = dd[(~dd["is_won"]) & dd["dealstage"].isin(MODEL_STAGE.keys())
               & dd["close_date"].notna()
               & (dd["close_date"] >= lo) & (dd["close_date"] <= hi)].copy()
    if len(openm):
        openm["prob"] = openm.apply(
            lambda r: _model_prob(r["leader"], r["dealstage"], r["deal_type"]), axis=1)
        openm["weighted_model"] = openm["amount"] * openm["prob"]
        fc_by_ae = openm.groupby("account_executive")["weighted_model"].sum()
    else:
        fc_by_ae = pd.Series(dtype=float)

    # Quota por AE — MESMA fonte do overview/pipeline (Q3_ROSTER no Q3, senão curated).
    period_is_q3 = (from_d >= Q3_START and to_d <= Q3_END)
    if period_is_q3:
        scope = expand_leader(f["leader"]) if f["leader"] else None
        quota_by_ae = {}
        for name, v in Q3_ROSTER.items():
            if f["region"] and Q3_ROSTER_REGION != f["region"]:
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
            meta.setdefault(name, {"leader": Q3_ROSTER[name]["leader"], "region": Q3_ROSTER_REGION})

    ae_names = sorted(set(done_by_ae.index) | set(fc_by_ae.index)
                      | {a for a, q in quota_by_ae.items() if q > 0})
    by_ae = []
    for a in ae_names:
        done = float(done_by_ae.get(a, 0.0)); fc = float(fc_by_ae.get(a, 0.0))
        m = meta.get(a, {}); lead = m.get("leader")
        by_ae.append({
            "account_executive": a, "leader": lead, "region": m.get("region"),
            "is_manager": a in NON_AE_USERS,
            "segment": SEGMENT_LABEL.get(LEADER_SEGMENT.get(lead), "—"),
            "done": done, "forecast": fc, "projection": done + fc,
            "quota": float(quota_by_ae.get(a, 0.0) or 0.0),
        })

    if grain == "leader":
        rows = _leader_rollup(by_ae, f["leader"], ["done", "forecast", "projection", "quota"])
        # Meta do líder = Board Target (planilha), não a soma das quotas do time.
        for r in rows:
            r["quota"] = LEADER_BOARD_TARGET.get(r["leader"], r["quota"])
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
        "quota": (_leader_meta_total([r["leader"] for r in rows]) if grain == "leader"
                  else sum(r["quota"] for r in by_ae)),
    }
    totals["attainment"] = (totals["projection"] / totals["quota"]) if totals["quota"] else None

    range_label = f"{from_d.strftime('%d/%m/%Y')} → {to_d.strftime('%d/%m/%Y')}"
    return render_template("forecast_model.html", ctx=ctx, grain=grain, unit=unit,
                           chart=chart, table=table, totals=totals, range_label=range_label,
                           prob=FORECAST_MODEL_PROB, seg_label=SEGMENT_LABEL,
                           stage_order=MODEL_STAGE_ORDER, stage_label=MODEL_STAGE_LABEL,
                           safety=MODEL_SAFETY_FACTOR)


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


def _refresh_loop(interval: int = 2700):
    """Reaquece os caches principais a cada `interval`s (45min, < TTL de 1h),
    ANTES de vencerem. Junto com o stale-while-revalidate do bq.query, garante que
    nenhuma visita — nem a primeira do dia — pague o round-trip do BigQuery."""
    while True:
        time.sleep(interval)
        _warm_all(force=True)
        print("pepeuhacker · caches reaquecidos (bg)", flush=True)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    _prewarm()
    threading.Thread(target=_refresh_loop, name="cache-refresher", daemon=True).start()
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
