"""Parser das notas do integrador de WhatsApp (aparecem como NOTE no HubSpot).

Formato real produzido pelo integrador:

    WhatsApp — <rótulo da thread/contato> — HH:MM–HH:MM UTC
    [HH:MM] <Nome do remetente>: <mensagem>
    [HH:MM] <Nome do remetente>: <mensagem>

O dono da nota (hubspot_owner_id) é o VENDEDOR (AE). Uma linha cujo remetente
não é o vendedor = RETORNO DE UM CONTATO (cliente). Notas em prosa, escritas à
mão, não têm o cabeçalho "WhatsApp —" e são ignoradas.

Validado contra notas reais do portal (ver test_wa_notes.py)."""
from __future__ import annotations
import re
import unicodedata
import html

# Linha de mensagem: "[12:34] Fulano de Tal: ..." — captura o nome do remetente.
_LINE_RE = re.compile(r"\[\s*\d{1,2}:\d{2}\s*\]\s*([^:]{1,80}?)\s*:", re.UNICODE)
# Cabeçalho do integrador (— em dash ou hífen).
_HDR_RE = re.compile(r"whats\s*app\s*[—\-]", re.IGNORECASE)


def _norm(s: str) -> str:
    """minúsculas + sem acento + espaços colapsados, para comparar nomes."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip().lower()


def _plain(body: str) -> str:
    """Tira HTML e desescapa entidades — o corpo vem como HTML do HubSpot."""
    if not body:
        return ""
    return html.unescape(re.sub(r"<[^>]+>", " ", body))


def is_whatsapp_note(body: str) -> bool:
    """True se a nota foi criada pelo integrador de WhatsApp (tem o cabeçalho)."""
    return bool(_HDR_RE.search(_plain(body)))


def senders(body: str) -> list[str]:
    """Remetentes distintos das linhas de mensagem, na ordem de 1ª aparição."""
    text = _plain(body)
    seen, out = set(), []
    for m in _LINE_RE.finditer(text):
        name = re.sub(r"\s+", " ", m.group(1)).strip()
        k = _norm(name)
        if k and k not in seen:
            seen.add(k)
            out.append(name)
    return out


def has_client_return(body: str, salesperson_name: str) -> bool:
    """True se a nota é de WhatsApp e tem ≥1 mensagem de alguém além do vendedor.
    O rótulo do cliente costuma trazer empresa/cargo (ex.: 'Paulo | OZ AN'), então
    tratamos um remetente como o vendedor só quando o nome do AE está contido nele
    (ou vice-versa)."""
    if not is_whatsapp_note(body):
        return False
    ae = _norm(salesperson_name)
    for snd in senders(body):
        s = _norm(snd)
        if not ae or (ae not in s and s not in ae):
            return True
    return False
