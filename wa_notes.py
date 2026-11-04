"""Parser das notas do integrador de WhatsApp (Sniper → NOTE no HubSpot).

Formato real produzido pelo integrador (validado em 08/10/2026 contra as notas
do portal; amostras em test_wa_notes.py):

    <p><strong>WhatsApp — <rótulo da conversa> — HH:MM–HH:MM UTC</strong></p>
    <p>[HH:MM] <Remetente>: <mensagem><br>[HH:MM] <Remetente>: <mensagem>...</p>

Três fatos do formato que o parser antigo não sabia:

  1. O RÓTULO é o contato como está salvo no celular do vendedor ("Sofia -
     Acme", "+5511900000000"), e as mensagens do cliente vêm com esse MESMO
     nome de remetente. É o sinal mais forte de "isto é o cliente falando".
  2. O vendedor aparece com o nome do PERFIL DO WHATSAPP dele, que pode não
     ser o nome do HubSpot: o owner "Gabriel Carvalho" escreve como "Gabriel
     Oliveira", o "Armando Ruiz" como "Armando Neto". Comparar com o nome do
     owner (a regra antiga) contava a fala do próprio vendedor como retorno do
     cliente. Por isso o vendedor é reconhecido por APELIDO aprendido: o nome
     que aparece no lado do vendedor em várias conversas do mesmo owner.
  3. Uma nota cobre uma janela de até ~24h e atravessa a meia-noite UTC
     ("12:47–00:46 UTC"); as linhas só têm HH:MM. A data de cada mensagem é
     reconstruída a partir do `hs_createdate` da nota, virando o dia quando o
     relógio volta.

O dono da nota (hubspot_owner_id) é o vendedor. Notas em prosa, escritas à mão,
não têm o cabeçalho "WhatsApp —" e são ignoradas.
"""
from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

PARSER_VERSION = "2026-10-08d"

# Cabeçalho do integrador: rótulo entre o 1º travessão e o "— HH:MM–HH:MM UTC".
_HDR_RE = re.compile(
    r"whats\s*app\s*[—\-]\s*(?P<label>.*?)\s*[—\-]\s*"
    r"(?P<h0>\d{1,2}):(?P<m0>\d{2})\s*[–\-]\s*(?P<h1>\d{1,2}):(?P<m1>\d{2})\s*UTC",
    re.IGNORECASE | re.S)
# Só a marca do integrador, pra `is_whatsapp_note` (compatível com a versão antiga).
_HDR_MARK_RE = re.compile(r"whats\s*app\s*[—\-]", re.IGNORECASE)
# Linha de mensagem no INÍCIO de uma linha: "[12:34] Fulano de Tal: ..."
_LINE_RE = re.compile(r"(?m)^\s*\[\s*(\d{1,2}):(\d{2})\s*\]\s*([^:\n]{1,120}?)\s*:")


def _norm(s: str) -> str:
    """Minúsculas, sem acento, sem emoji/pontuação exótica, espaços colapsados.
    Mantém | - ( ) / . + porque fazem parte de rótulo ("Renato - Alfa Racoes", "+55...")."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = "".join(c for c in s if c.isalnum() or c.isspace() or c in "|-()/.+")
    return re.sub(r"\s+", " ", s).strip().lower()


def _plain(body: str) -> str:
    """HTML do HubSpot → texto, com <br> e </p> virando quebra de linha."""
    if not body:
        return ""
    t = re.sub(r"(?i)<br\s*/?>|</p>", "\n", body)
    return html.unescape(re.sub(r"<[^>]+>", "", t))


def _to_dt(v) -> datetime | None:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    s = str(v).replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def label_match(sender_norm: str, label_norm: str) -> bool:
    """O remetente É o contato do rótulo? Igual, ou um é prefixo do outro na
    fronteira de palavra — o contato renomeado no meio da conversa aparece como
    "carlos lima" e "carlos lima | beta papel" na MESMA nota."""
    if not sender_norm or not label_norm:
        return False
    if sender_norm == label_norm:
        return True
    a, b = sorted((sender_norm, label_norm), key=len)
    return len(a) >= 5 and b.startswith(a + " ")


def _toks(s: str) -> list:
    """Palavras com letra/número (descarta "|", "-", "." soltos)."""
    return [t for t in re.split(r"[\s|/()\-.]+", s or "") if any(c.isalnum() for c in t)]


def _contains_seq(big: list, small: list) -> bool:
    n = len(small)
    return n > 0 and any(big[i:i + n] == small for i in range(len(big) - n + 1))


def name_match(sender_norm: str, person_norm: str) -> bool:
    """O remetente é essa pessoa pelo nome? Por PALAVRA inteira, nunca por
    substring: um nome contém o outro como sequência de palavras ("gabriel
    martins" ⊂ "gabriel martins silva"), ou mesmo primeiro + último nome.
    Até a versão 2026-10-08 era substring crua, e o cliente que assina "."
    no WhatsApp casava com o owner "Vitor H. Barbosa" e virava funcionário."""
    s, p = _toks(sender_norm), _toks(person_norm)
    if not s or not p or sum(len(t) for t in s) < 3:
        return False
    if _contains_seq(s, p) or _contains_seq(p, s):
        return True
    return len(s) >= 2 and len(p) >= 2 and s[0] == p[0] and s[-1] == p[-1]


# Palavras de cargo/bot que acompanham "Tractian" num rótulo interno
# ("Tractian SE", "Corp Tractian", "Tractian IA").
_ROLE_WORDS = {"ia", "ai", "bot", "assistente", "suporte", "se", "sdr", "bdr", "ae",
               "cs", "csm", "corp", "vendas", "sales", "presales", "pre"}
# Separadores de segmento: | e [ ], e travessão/hífen com espaço de pelo menos
# um lado ("Freio-Sul" não separa; "Tractian- Rodrigo" separa).
_SEG_RE = re.compile(r"\s*\|\s*|\s*[\[\]]\s*|\s+[-–—]\s*|\s*[-–—]\s+")


def internal_label(label_norm: str, internal_names=()) -> bool:
    """A conversa é INTERNA (o contato do rótulo é da própria Tractian)?

    Achado da auditoria de 08/10/2026: notas com rótulo de colega ("Joao
    Martins | Tractian SE", "Guilherme | Tractian", "Armando TRACTIAN") ou do
    bot ("Tractian IA") contavam como retorno de cliente, porque a regra do
    rótulo vem antes de tudo. Interna quando o rótulo tem "tractian" e:
      · tirando "tractian" e palavras de cargo/bot não sobra nada;
      · são 2 segmentos, um deles é só "Tractian" (+cargo) e o outro é um
        nome curto (≤3 palavras): "Pedro Mendes - Tractian", "[Tractian] João";
      · é 1 segmento só e sobram ≤2 palavras: "Armando TRACTIAN", "Gui Tractian";
      · ou o resto contém o nome completo de um owner.
    Sem "tractian": o rótulo inteiro É exatamente o nome completo (2+ palavras)
    de um owner ("Leonardo Scopel"), sem segmento de empresa ao lado.
    Fica CLIENTE o rótulo "Nome Empresa Tractian" com 3+ palavras além de
    Tractian ("Fabiano Freio-Sul Tractian") e o de 3+ segmentos ("Tractian-
    Rodrigo- Gama Eng Mec") — há vendedor que salva cliente assim."""
    # "Trct" é abreviação de Tractian em alguns contatos ("Julio Cortez | Trct").
    label_norm = re.sub(r"\btrct\b", "tractian", label_norm or "")
    toks = _toks(label_norm)
    if not toks:
        return False
    segs = [x for x in _SEG_RE.split(label_norm) if _toks(x)]
    names = [t for t in (_toks(n) for n in internal_names) if len(t) >= 2]
    if "tractian" in toks:
        rest = [t for t in toks if t != "tractian" and t not in _ROLE_WORDS]
        if not rest:
            return True
        if len(segs) == 2:
            only_tr = [sg for sg in segs if set(_toks(sg)) - _ROLE_WORDS == {"tractian"}]
            if only_tr:
                other = [sg for sg in segs if sg not in only_tr]
                if other and len(_toks(other[0])) <= 3:
                    return True
        if len(segs) == 1 and len(rest) <= 2:
            return True
        return any(_contains_seq(rest, n) for n in names)
    # Só o nome, sem empresa ao lado: "Lucas Silva | Omega Fertilizantes" é
    # um cliente homônimo do owner Lucas Silva, não o colega.
    return len(segs) == 1 and any(toks == n for n in names)


@dataclass
class Sender:
    name: str            # como aparece na nota
    key: str             # normalizado
    n: int = 0
    first_at: datetime | None = None
    last_at: datetime | None = None
    label_match: bool = False
    dates: set = field(default_factory=set)   # datas UTC em que falou


@dataclass
class ParsedNote:
    label: str
    label_key: str
    senders: dict          # key -> Sender, na ordem de 1ª aparição
    n_lines: int


def parse(body: str, created_at=None, last_msg_at=None) -> ParsedNote | None:
    """Nota do integrador → rótulo + remetentes com data/hora de cada fala.
    None se não for nota do integrador.

    Datas: âncora no `created_at` (≈ 1ª mensagem); cada vez que o HH:MM volta,
    vira o dia. Sem `created_at`, a âncora é o `last_msg_at` (hs_timestamp ≈
    última mensagem) contando os dias de trás pra frente."""
    text = _plain(body)
    m = _HDR_RE.search(text)
    if not m:
        return None
    label = re.sub(r"\s+", " ", m.group("label")).strip()
    lines = [(int(x.group(1)), int(x.group(2)), re.sub(r"\s+", " ", x.group(3)).strip())
             for x in _LINE_RE.finditer(text[m.end():])]
    # Dia relativo de cada linha (0 = dia da 1ª mensagem).
    rel, day, prev = [], 0, None
    for h, mi, _ in lines:
        t = h * 60 + mi
        if prev is not None and t < prev:
            day += 1
        rel.append(day)
        prev = t
    c0, l0 = _to_dt(created_at), _to_dt(last_msg_at)
    if c0 is not None:
        base = c0.astimezone(timezone.utc).date()
        # Nota criada com atraso: se a 1ª linha é "depois" do createdate no
        # relógio, ela foi no dia anterior (ex.: criada 00:05 pra fala das 23:58).
        if lines and (lines[0][0] * 60 + lines[0][1]) > (c0.hour * 60 + c0.minute) + 5:
            base -= timedelta(days=1)
    elif l0 is not None:
        base = l0.astimezone(timezone.utc).date() - timedelta(days=rel[-1] if rel else 0)
    else:
        base = None
    senders: dict = {}
    lkey = _norm(label)
    for (h, mi, name), d in zip(lines, rel):
        k = _norm(name)
        if not k:
            continue
        s = senders.get(k)
        if s is None:
            s = senders[k] = Sender(name=name, key=k, label_match=label_match(k, lkey))
        s.n += 1
        if base is not None:
            dt = datetime(base.year, base.month, base.day, h, mi, tzinfo=timezone.utc) + timedelta(days=d)
            s.first_at = s.first_at or dt
            s.last_at = dt
            s.dates.add(dt.date())
    return ParsedNote(label=label, label_key=lkey, senders=senders, n_lines=len(lines))


def classify(p: ParsedNote, owner_name: str = "", owner_aliases=(), staff=(),
             internal_names=()) -> dict:
    """Papel de cada remetente: 'client' | 'vendor' | 'staff'.

    `internal_names` = nomes completos dos owners da Tractian (pra reconhecer
    conversa interna pelo rótulo).

    Ordem das regras (a primeira que casa decide):
      0. conversa interna (rótulo é colega ou bot) → quem bate com o rótulo é staff
      1. bate com o rótulo da conversa            → client
      2. é o owner pelo nome, ou apelido dele      → vendor
      3. nota sem fala do rótulo e com 1 remetente → vendor  (o cliente,
         quando fala sozinho, fala com o nome do rótulo; quem fala sozinho
         sem ser o rótulo é o vendedor puxando conversa)
      4. nome ou apelido de outro owner da Tractian → staff (gestor, SDR)
      5. qualquer outro                            → client (colega do cliente,
         participante de grupo)
    """
    own = _norm(owner_name)
    aliases = {_norm(a) for a in owner_aliases}
    staff_keys = {_norm(x) for x in staff}
    any_label = any(s.label_match for s in p.senders.values())
    internal = internal_label(p.label_key, internal_names)
    roles = {}
    for k, s in p.senders.items():
        if s.label_match:
            roles[k] = "staff" if internal else "client"
        elif k in aliases or name_match(k, own):
            roles[k] = "vendor"
        elif not any_label and len(p.senders) == 1:
            roles[k] = "vendor"
        elif k in staff_keys or any(name_match(k, x) for x in staff_keys if len(_toks(x)) >= 2):
            roles[k] = "staff"
        elif internal:
            roles[k] = "staff"      # colega do colega, numa conversa interna
        else:
            roles[k] = "client"
    return roles


def client_summary(p: ParsedNote, roles: dict) -> dict:
    """Agregado do lado do cliente numa nota."""
    cs = [s for k, s in p.senders.items() if roles.get(k) == "client"]
    vs = [s for k, s in p.senders.items() if roles.get(k) == "vendor"]
    dates = sorted({d for s in cs for d in s.dates})
    lasts = [s.last_at for s in cs if s.last_at]
    return {
        "has_client_return": bool(cs),
        "client_dates": dates,
        "last_client_at": max(lasts) if lasts else None,
        "n_client_msgs": sum(s.n for s in cs),
        "n_vendor_msgs": sum(s.n for s in vs),
    }


# ---- API antiga (mantida: hubspot.last_client_returns ainda a usa) ----------

def is_whatsapp_note(body: str) -> bool:
    """True se a nota foi criada pelo integrador de WhatsApp (tem o cabeçalho)."""
    return bool(_HDR_MARK_RE.search(_plain(body)))


def senders(body: str) -> list[str]:
    """Remetentes distintos das linhas de mensagem, na ordem de 1ª aparição."""
    p = parse(body)
    return [s.name for s in p.senders.values()] if p else []


def has_client_return(body: str, salesperson_name: str, aliases=()) -> bool:
    """True se a nota é de WhatsApp e tem ≥1 fala do lado do cliente."""
    p = parse(body)
    if p is None:
        return False
    return client_summary(p, classify(p, salesperson_name, aliases))["has_client_return"]
