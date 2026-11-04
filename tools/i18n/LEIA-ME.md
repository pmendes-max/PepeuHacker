# Idiomas da interface (PT-BR · ES · EN)

O seletor do cabeçalho grava o cookie `lang`. Em ES/EN a página carrega
`static/i18n/<lang>.js` (dicionário) e `static/i18n.js` (tradutor), que troca o
texto no navegador — telas, tooltips e o modal de explosão. PT não carrega nada.

Cada texto vira uma FORMA (números, R$, datas, %, trimestres e nomes de pessoas
viram marcadores) e a forma é procurada no dicionário. A regra mora em dois
lugares que precisam ficar iguais: `tools/i18n/forma.py` e `static/i18n.js`.
Texto sem tradução simplesmente fica em português.

## Tela nova / texto novo
1. `PEPEU_CACHE_DIR=<cópia do cache> python tools/i18n/extrair.py . /tmp/formas.json`
   — varre todas as telas e drills e lista as formas.
2. Traduza as formas que faltam seguindo `GLOSSARIO.md` e acrescente em
   `tools/i18n/es.json` e `tools/i18n/en.json` (`map`: forma PT → tradução,
   mesmos marcadores).
3. `python tools/i18n/gerar.py` — valida os marcadores e gera `static/i18n/*.js`.
