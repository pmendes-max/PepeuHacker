"""Gera static/i18n/<lang>.js a partir de tools/i18n/<lang>.json.

<lang>.json = {"months": {"jan": "ene", ...}, "map": {"<forma PT>": "<tradução>"}}
A forma PT segue tools/i18n/forma.py (números, R$, datas, nomes → marcadores).
A tradução tem que ter os MESMOS marcadores do original — na mesma ordem, ou
numerados ({1}, {2}…) quando a frase precisa trocar a ordem. O script recusa
entrada que perca ou invente marcador (um {n} perdido apagaria um número da tela).

uso: python tools/i18n/gerar.py            (gera es e en)
"""
import json, os, sys
from collections import Counter

AQUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AQUI)
from forma import marcadores  # noqa: E402

RAIZ = os.path.dirname(os.path.dirname(AQUI))


def validar(pt, tr):
    a, b = marcadores(pt), marcadores(tr)
    if any(x.isdigit() for x in b):
        idx = sorted(int(x) for x in b if x.isdigit())
        return all(1 <= i <= len(a) for i in idx) and not [x for x in b if not x.isdigit()]
    return Counter(a) == Counter(b)


def gerar(lang):
    src = json.load(open(os.path.join(AQUI, f"{lang}.json"), encoding="utf-8"))
    mapa, ruins = {}, []
    for pt, tr in src["map"].items():
        if not isinstance(tr, str) or not tr.strip():
            continue
        if not validar(pt, tr):
            ruins.append((pt, tr))
            continue
        if tr != pt:
            mapa[pt] = tr
    out = {"lang": lang, "months": src.get("months", {}), "map": mapa}
    os.makedirs(os.path.join(RAIZ, "static", "i18n"), exist_ok=True)
    with open(os.path.join(RAIZ, "static", "i18n", f"{lang}.js"), "w", encoding="utf-8") as fh:
        fh.write("/* gerado por tools/i18n/gerar.py — não editar à mão */\n")
        fh.write("window.__I18N__ = " + json.dumps(out, ensure_ascii=False, separators=(",", ":")) + ";\n")
    print(f"{lang}: {len(mapa)} frases" + (f" · {len(ruins)} recusadas (marcadores não batem)" if ruins else ""))
    for pt, tr in ruins[:20]:
        print("   ✗", pt, "→", tr)
    return not ruins


if __name__ == "__main__":
    ok = all([gerar(l) for l in (sys.argv[1:] or ["es", "en"])])
    sys.exit(0 if ok else 1)
