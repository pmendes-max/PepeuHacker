"""Varre o pepeuhacker e coleta todo texto visível (nós de texto, atributos de
tooltip, literais de string dos <script> e o JSON dos drills), normalizado em
"formas" — números, dinheiro, datas e nomes de pessoas viram marcadores.

uso: PEPEU_CACHE_DIR=<copia do cache> python tools/i18n/extrair.py <repo> <saida.json>
"""
import os, sys, re, json, html, random
from collections import Counter, defaultdict
from html.parser import HTMLParser
from urllib.parse import urlsplit

repo, out = sys.argv[1], sys.argv[2]
sys.path.insert(0, repo); os.chdir(repo)
import bq
bq._spawn_refresh = lambda *a, **k: None
import app as A
A.app.config["TESTING"] = True
c = A.app.test_client()
random.seed(5)

# ---- nomes de gente e regra de forma: as MESMAS do app/navegador ----
sys.path.insert(0, os.path.join(repo, "tools", "i18n"))
import forma as FM
with A.app.test_request_context("/"):
    NOMES = A._i18n_nomes()
RX, TIPOS = FM.compilar(NOMES)


def forma(t):
    return FM.forma(t, RX, TIPOS)[0]


TEM_LETRA = re.compile(r"[A-Za-zÀ-ÿ]{2,}")
ATTRS = ("title", "placeholder", "aria-label", "data-drill-title", "alt", "data-of-label")


class Coletor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.pilha = []; self.textos = []; self.attrs = []; self.scripts = []

    def handle_starttag(self, tag, attrs):
        if tag not in ("br", "img", "input", "hr", "meta", "link", "path", "rect", "line", "circle"):
            self.pilha.append(tag)
        for k, v in attrs:
            if k in ATTRS and v:
                self.attrs.append(v)

    def handle_startendtag(self, tag, attrs):
        for k, v in attrs:
            if k in ATTRS and v:
                self.attrs.append(v)

    def handle_endtag(self, tag):
        if tag in self.pilha:
            while self.pilha and self.pilha.pop() != tag:
                pass

    def handle_data(self, data):
        dentro = self.pilha[-1] if self.pilha else ""
        if dentro == "script":
            self.scripts.append(data)
        elif dentro == "style":
            return
        elif data.strip():
            self.textos.append(data)


LIT = re.compile(r"'((?:[^'\\\n]|\\.){2,200})'|\"((?:[^\"\\\n]|\\.){2,200})\"|`((?:[^`\\]|\\.){2,300})`")
PT_HINT = re.compile(r"[ãõçáéíóúâêô]|\b(?:de|do|da|dos|das|em|no|na|sem|com|por|para|deals?|meta|total|semana|realizado|carregando|erro)\b", re.I)


def literais_js(src):
    out = []
    for m in LIT.finditer(src):
        s = next(g for g in m.groups() if g is not None)
        s = re.sub(r"\$\{[^}]*\}", "{x}", s)
        if TEM_LETRA.search(s) and PT_HINT.search(s) and not re.search(r"[{};=<>]|^\s*[\.#\[]|querySelector|data-", s.replace("{x}", "")):
            out.append(s)
    return out


rules = sorted({r.rule for r in A.app.url_map.iter_rules() if "GET" in r.methods and "<" not in r.rule})
PAGINAS = [r for r in rules if not r.endswith("deals") and not r.startswith(("/static", "/api", "/mcp", "/link"))
           and r not in ("/overview/funnel-stages", "/pipeline/funnel/moves", "/pipeline/wow/quarter/cobertura",
                         "/leaders/pipeline/wow/quarter/cobertura")]
VARIANTES = ["", "?ae=Bruno+Brunelli", "?leader=Jo%C3%A3o+Tavares", "?money=arr",
             "?date_from=2026-07-01&date_to=2026-09-30", "?dtype=exp", "?base=created", "?focus=late",
             "?weeks=4", "?by=dtype", "?by=fcat", "?env=any&coh=all"]

formas = Counter()
exemplos = defaultdict(set)
origem = defaultdict(set)
drills = []


def add(t, onde):
    if not t or not TEM_LETRA.search(t):
        return
    t = html.unescape(t)
    if t.strip().startswith(("/", "http")) or re.search(r"[?&][a-z_]+=", t):
        return
    f = forma(t)
    if not TEM_LETRA.search(f.replace("{nome}", "").replace("{n}", "")):
        return
    formas[f] += 1
    if len(exemplos[f]) < 2:
        exemplos[f].add(re.sub(r"\s+", " ", t).strip()[:200])
    origem[f].add(onde)


for p in PAGINAS:
    for v in VARIANTES:
        r = c.get(p + v)
        if r.status_code != 200 or "html" not in (r.content_type or ""):
            continue
        k = Coletor(); k.feed(r.get_data(as_text=True))
        for t in k.textos:
            add(t, p)
        for t in k.attrs:
            add(t, p + " @attr")
        for s in k.scripts:
            for t in literais_js(s):
                add(t, p + " @js")
        for u in re.findall(r'data-drill="(/[^"]+)"', r.get_data(as_text=True)):
            drills.append(html.unescape(u))

# drills: título, subtítulo, colunas e valores categóricos
random.shuffle(drills)
vistos = set()
for u in drills:
    chave = (urlsplit(u).path, tuple(sorted(k.split("=")[0] for k in urlsplit(u).query.split("&"))))
    if sum(1 for x in vistos if x == chave) >= 1 and random.random() > 0.15:
        continue
    vistos.add(chave)
    j = c.get(u + ("&" if "?" in u else "?") + "money=mrr").get_json(silent=True) or {}
    for k in ("title", "subtitle"):
        add(j.get(k) or "", "drill " + urlsplit(u).path)
    for col in j.get("columns") or []:
        add(col.get("label") or "", "drill-col")
    nao = {c_["key"] for c_ in (j.get("columns") or []) if c_.get("link")} | {"ae", "deal_name", "name", "leader", "qual_owner"}
    for row in (j.get("rows") or [])[:40]:
        for k, v in row.items():
            if k in nao or not isinstance(v, str) or v.startswith("http"):
                continue
            add(v, "drill-val")

# JS de templates incluídos que não renderizam sozinhos
for fn in os.listdir("templates"):
    src = open(os.path.join("templates", fn)).read()
    for s in re.findall(r"<script>(.*?)</script>", src, re.S):
        for t in literais_js(s):
            add(t, "tpl " + fn + " @js")

res = [{"forma": f, "n": n, "ex": sorted(exemplos[f]), "onde": sorted(origem[f])[:3]} for f, n in formas.most_common()]
json.dump({"nomes": sorted(NOMES), "formas": res}, open(out, "w"), ensure_ascii=False, indent=1)
print("formas:", len(res), "drills vistos:", len(vistos))
