/* ============================================================================
   Tradução da interface (PT-BR → ES / EN), no navegador.

   Por que aqui e não nos templates: o texto do dash nasce em três lugares
   (templates, rótulos montados em Python e tooltips/modais montados em JS), e
   um tradutor só no DOM pega os três sem reescrever tela nenhuma.

   Como funciona: cada texto vira uma FORMA — números, dinheiro, datas, %,
   trimestres e nomes de pessoas viram marcadores ({n}, {$}, {data}, {nome}…) —
   e a forma é procurada no dicionário do idioma (static/i18n/<lang>.js, gerado
   por tools/i18n/gerar.py). Achou: devolve a tradução com os valores originais
   de volta no lugar. Não achou: o texto fica como está (nome de deal, empresa).
   A MESMA regra de forma existe em Python (tools/i18n/forma.py); mexeu numa,
   mexer na outra.
   ========================================================================== */
(function () {
  "use strict";
  var D = window.__I18N__;
  var root = document.documentElement;
  function liberar() { root.classList.remove("i18n-pend"); }
  if (!D || !D.map) { liberar(); return; }

  var NOMES = (window.__I18N_NOMES__ || []).filter(function (n) { return n && n.length > 3; })
    .sort(function (a, b) { return b.length - a.length; });
  function esc(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }
  var MES = "(?:jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)";
  // Ordem = prioridade (mesma de forma.py). Cada alternativa é um grupo, pra
  // saber qual marcador vira.
  var PARTES = [
    ["nome", NOMES.length ? NOMES.map(esc).join("|") : "(?!x)x"],
    // Sufixo só sem letra depois (o "m" de "R$ 123 movidos" não é sufixo).
    ["$", "(?:R\\$|US\\$)\\s?[+\\-−]?[\\d.,]+(?:\\s?(?:M|k|mi|mil)(?![A-Za-zÀ-ÿ]))?"],
    ["data", "\\b\\d{1,2}/\\d{1,2}(?:/\\d{2,4})?\\b"],
    ["data", "\\b\\d{4}-\\d{2}-\\d{2}\\b"],
    ["data", "\\b\\d{1,2} " + MES + "(?: \\d{4})?\\b"],
    ["hora", "\\b\\d{1,2}h\\d{0,2}\\b"],
    ["%", "[+\\-−]?\\d[\\d.,]*\\s?%"],
    ["tri", "\\bQ[1-4]/\\d{2}\\b"],
    ["fy", "\\bFY\\d{2}\\b"],
    ["n", "[+\\-−]?\\d[\\d.,]*"]
  ];
  var RX = new RegExp(PARTES.map(function (p) { return "(" + p[1] + ")"; }).join("|"), "gi");

  function forma(t) {
    var vals = [];
    var k = t.replace(RX, function () {
      for (var i = 1; i <= PARTES.length; i++) {
        if (arguments[i] !== undefined) { vals.push(arguments[i]); return "{" + PARTES[i - 1][0] + "}"; }
      }
      return arguments[0];
    });
    return { k: k, v: vals };
  }

  var MESES = D.months || {};
  function valor(v) {
    // Data "18 out 2026": o mês também traduz.
    return v.replace(new RegExp("\\b" + MES + "\\b", "i"), function (m) { return MESES[m.toLowerCase()] || m; });
  }
  function preencher(tpl, vals) {
    var i = 0;
    return tpl.replace(/\{(\$|data|hora|%|tri|fy|n|nome|\d+)\}/g, function (m, g) {
      if (/^\d+$/.test(g)) { var x = vals[+g - 1]; return x === undefined ? m : valor(x); }
      var y = vals[i++]; return y === undefined ? m : valor(y);
    });
  }

  var LETRA = /[A-Za-zÀ-ÿ]{2}/;
  function umaFrase(t) {
    var f = forma(t);
    var tt = D.map[f.k];
    return tt === undefined ? null : preencher(tt, f.v);
  }
  // ": " também separa: os tooltips montados em JS são "rótulo: valor".
  var SEP = /( · | — | → |: )/;
  function traduzir(s) {
    if (!s || !LETRA.test(s)) return null;
    var limpo = s.replace(/\s+/g, " ").trim();
    if (!limpo) return null;
    var r = umaFrase(limpo);
    if (r === null && /:$/.test(limpo)) {           // "Projeção:" antes de um <b>
      var sem = umaFrase(limpo.slice(0, -1));
      if (sem !== null) r = sem + ":";
    }
    if (r === null) {
      // Pedaço que começa no separador (" · −3 pra meta" depois de um <b>):
      // tira o separador da frente, traduz o resto e devolve o separador.
      var mp = limpo.match(/^([·—→]\s+)(.+)$/);
      if (mp) { var rr = traduzir(mp[2]); if (rr !== null) return s.match(/^\s*/)[0] + mp[1] + rr.trim() + s.match(/\s*$/)[0]; }
    }
    if (r === null && SEP.test(limpo)) {
      // Plano B: a frase inteira não está no dicionário (ex.: traz o nome de
      // alguém fora da lista de nomes) — traduz pedaço a pedaço entre os
      // separadores; o que não casar (o nome) fica como está.
      var mudou = false;
      r = limpo.split(SEP).map(function (p, i) {
        if (i % 2) return p;
        var x = umaFrase(p);
        if (x === null) return p;
        mudou = true; return x;
      }).join("");
      if (!mudou) r = null;
    }
    if (r === null) return null;
    var lead = s.match(/^\s*/)[0], trail = s.match(/\s*$/)[0];
    return lead + r + trail;
  }
  window.__t = function (s) { var r = traduzir(s); return r === null ? s : r; };
  window.__i18nForma = forma;   // teste de paridade com tools/i18n/forma.py

  var PULA = { SCRIPT: 1, STYLE: 1, TEXTAREA: 1, CODE: 1, PRE: 1, NOSCRIPT: 1 };
  var ATTRS = ["title", "placeholder", "aria-label", "data-drill-title", "alt"];
  var feito = new WeakMap();   // nó → valor que NÓS escrevemos (não retraduz)

  function bloqueado(el) {
    for (var x = el; x && x.nodeType === 1; x = x.parentNode) {
      if (PULA[x.nodeName] || x.hasAttribute("data-no-i18n") || x.getAttribute("translate") === "no") return true;
    }
    return false;
  }
  function texto(n) {
    if (feito.get(n) === n.nodeValue) return;
    var r = traduzir(n.nodeValue);
    if (r !== null && r !== n.nodeValue) { n.nodeValue = r; }
    feito.set(n, n.nodeValue);
  }
  function atributos(el) {
    for (var i = 0; i < ATTRS.length; i++) {
      var a = ATTRS[i], v = el.getAttribute(a);
      if (!v) continue;
      var r = traduzir(v);
      if (r !== null && r !== v) el.setAttribute(a, r);
    }
  }
  function varrer(no) {
    if (!no) return;
    if (no.nodeType === 3) { if (!bloqueado(no.parentNode)) texto(no); return; }
    if (no.nodeType !== 1 || bloqueado(no)) return;
    atributos(no);
    var w = document.createTreeWalker(no, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT, {
      acceptNode: function (n) {
        if (n.nodeType === 1) {
          if (PULA[n.nodeName] || n.hasAttribute("data-no-i18n") || n.getAttribute("translate") === "no")
            return NodeFilter.FILTER_REJECT;
          return NodeFilter.FILTER_ACCEPT;
        }
        return NodeFilter.FILTER_ACCEPT;
      }
    });
    var n;
    while ((n = w.nextNode())) {
      if (n.nodeType === 3) texto(n); else atributos(n);
    }
  }

  function inicio() {
    try {
      var t = traduzir(document.title); if (t) document.title = t;
      varrer(document.body);
    } finally { liberar(); }
    new MutationObserver(function (muts) {
      for (var i = 0; i < muts.length; i++) {
        var m = muts[i];
        if (m.type === "characterData") { if (!bloqueado(m.target.parentNode)) texto(m.target); }
        else if (m.type === "attributes") { if (!bloqueado(m.target)) atributos(m.target); }
        else for (var j = 0; j < m.addedNodes.length; j++) varrer(m.addedNodes[j]);
      }
    }).observe(document.body, { childList: true, subtree: true, characterData: true,
                                attributes: true, attributeFilter: ATTRS });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", inicio);
  else inicio();
  // Rede de segurança: nada de tela em branco se algo acima falhar.
  setTimeout(liberar, 1500);
})();
