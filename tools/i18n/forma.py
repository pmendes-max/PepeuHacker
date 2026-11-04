"""Regra de FORMA do tradutor da interface — espelho de static/i18n.js.

Texto → (forma, valores): números, dinheiro, datas, %, trimestres e nomes de
pessoas viram marcadores ({n}, {$}, {data}, {hora}, {%}, {tri}, {fy}, {nome}),
na ordem em que aparecem. O dicionário é indexado pela forma. Mexeu aqui,
mexer no JS (e vice-versa): se as duas divergirem, a forma gerada aqui não é a
que o navegador procura e o texto simplesmente não traduz.
"""
import re

MES = r"(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)"
_PARTES = [
    # Sufixo (M, k, mi, mil) só quando NÃO vem letra depois: com a regex sem
    # caixa, o "m" de "R$ 123 movidos" virava sufixo. Lookahead explícito (e não
    # \b) porque \b difere entre Python e JS perto de letra acentuada.
    ("$", r"(?:R\$|US\$)\s?[+\-−]?[\d.,]+(?:\s?(?:M|k|mi|mil)(?![A-Za-zÀ-ÿ]))?"),
    ("data", r"\b\d{1,2}/\d{1,2}(?:/\d{2,4})?\b"),
    ("data", r"\b\d{4}-\d{2}-\d{2}\b"),
    ("data", r"\b\d{1,2} " + MES + r"(?: \d{4})?\b"),
    ("hora", r"\b\d{1,2}h\d{0,2}\b"),
    ("%", r"[+\-−]?\d[\d.,]*\s?%"),
    ("tri", r"\bQ[1-4]/\d{2}\b"),
    ("fy", r"\bFY\d{2}\b"),
    ("n", r"[+\-−]?\d[\d.,]*"),
]


def compilar(nomes):
    nomes = sorted({n for n in nomes if n and len(n) > 3}, key=len, reverse=True)
    partes = [("nome", "|".join(re.escape(n) for n in nomes) or r"(?!x)x")] + _PARTES
    rx = re.compile("|".join("(" + p + ")" for _, p in partes), re.I)
    return rx, [t for t, _ in partes]


def forma(texto, rx, tipos):
    vals = []

    def sub(m):
        for i, g in enumerate(m.groups()):
            if g is not None:
                vals.append(g)
                return "{" + tipos[i] + "}"
        return m.group(0)

    k = rx.sub(sub, re.sub(r"\s+", " ", texto).strip())
    return k, vals


MARCADOR = re.compile(r"\{(\$|data|hora|%|tri|fy|n|nome|\d+)\}")


def marcadores(s):
    return [m.group(1) for m in MARCADOR.finditer(s)]
