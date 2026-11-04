# Tradução da interface do dashboard de vendas (PT-BR → ES e EN)

Contexto: dashboard interno de vendas B2B (Tractian). Público: gestores e vendedores
(ES = México; EN = gestão global). Tom: rótulos curtos de BI, sem floreio.

## Regras obrigatórias
1. MARCADORES: `{n}` `{$}` `{data}` `{hora}` `{%}` `{tri}` `{fy}` `{nome}` são valores
   (número, dinheiro, data, hora, porcentagem, trimestre, ano fiscal, nome de pessoa).
   A tradução tem que conter EXATAMENTE os mesmos marcadores, na MESMA ordem.
   Se a frase traduzida precisar mudar a ordem, numere TODOS: `{1}` `{2}` … (1 = o
   primeiro marcador do original). Nunca traduza, remova ou invente marcador.
2. Preserve pontuação e símbolos do original: `·` `→` `—` `−` `+` `×` `÷` `≥` `(` `)` `:`,
   espaços nas pontas não importam. Mantenha MAIÚSCULAS de ênfase (ex.: "AVANÇOU" →
   "AVANZÓ"/"ADVANCED").
3. NÃO traduzir (copie igual): nomes de estágio do HubSpot (Scheduled, Qualifying,
   Scoping, Negotiation, Decision, Procurement), Won, Lost, Pipeline (como estágio),
   Commit, Best Case, Not forecasted, Early, Late, PoP, WoW, DoD, MRR, ARR, R$, US$,
   AE, SDR, RSM, VP, CMMS, TracOS, Smart Trac (e demais produtos "… Trac"), HubSpot,
   Gong, WhatsApp, Sniper, Tractian, pepeuhacker, Comp. Target, Board Target, new
   logo(s), deal(s), pipe, forecast (como nome da aba), Overview, Stale, Funil II
   pode virar "Embudo II"/"Funnel II". Nomes de colunas/variáveis técnicas
   (ex.: `new_arr_in_usd`, `motion`, `deal_type`) ficam iguais.
4. Se o texto NÃO for interface (nome de empresa, nome de deal, nome de pessoa,
   cidade/UF, código), devolva o texto IGUAL ao original.
5. Abreviações de mês em `{data}` não aparecem no texto (já são marcador).

## Glossário (PT → ES | EN)
- Realizado → Realizado | Closed (como substantivo de receita fechada: "Closed won")
  · use "Realizado" (ES) e "Closed" (EN); "realizado no período" → "realizado en el período" | "closed in period"
- Meta → Meta | Target · Quota → Cuota | Quota · Atingimento → Cumplimiento | Attainment
- Cobertura → Cobertura | Coverage · Projeção → Proyección | Projection
- Ponderado → Ponderado | Weighted · Bruto → Bruto | Gross
- Geração de Demanda → Generación de Demanda | Demand Generation
- Inatividade → Inactividad | Inactivity · parado → estancado | stalled
- Explodir (abrir o detalhe de deals) → Ver detalle de | Drill into (ex.: "Explodir os deals" → "Ver detalle de los deals" | "Drill into deals")
- Aquisição → Adquisición | Acquisition · Expansão → Expansión | Expansion
- Venda → Venta | Sale · Vendedor → Vendedor | Rep · Líder → Líder | Leader
- Diretor → Director | Director · Gerente → Gerente | Manager
- Trimestre → Trimestre | Quarter · Semana → Semana | Week · Domingo → Domingo | Sunday
- Meia-noite → Medianoche | Midnight · Ontem → Ayer | Yesterday · Hoje → Hoy | Today
- Período → Período | Period · De / Até (filtro de datas) → Desde / Hasta | From / To
- Todos (opção de filtro) → Todos | All · Limpar → Limpiar | Clear
- Ativos (filtros ativos) → Activos | Active
- Estágio / Etapa → Etapa | Stage · Funil → Embudo | Funnel
- Entrou → Entró | Entered · Saiu → Salió | Exited · Avançou → Avanzó | Advanced
- Voltou → Retrocedió | Moved back · Cresceu → Creció | Grew · Encolheu → Se redujo | Shrank
- Novo deal → Deal nuevo | New deal · Ganho → Ganado | Won (como adjetivo) · Perdido → Perdido | Lost
- Reprecificação / reprecificado → Reprecificación / reprecificado | Repricing / repriced
- Escorregou (de trimestre) → Se deslizó | Slipped · Puxado (pro trimestre) → Adelantado | Pulled in
- Close date → Close date (igual) · PO → PO (igual) · On-hold / em aprovação → En aprobación | Pending approval
- Retorno de contato → Respuesta del contacto | Contact reply
- Jurídico → Jurídico | Legal · Visitas → Visitas | Visits · Ticket médio → Ticket promedio | Avg. ticket
- Sales Overtime → Sales Overtime (igual) · Conversão → Conversión | Conversion · Coorte → Cohorte | Cohort
- Dado de X min atrás → Dato de hace X min | Data from X min ago
- Moeda de exibição → Moneda | Display currency

## Formato de saída
Um JSON: {"es": {"<forma original>": "<tradução ES>", ...}, "en": {...}} com TODAS
as formas do lote (as que ficam iguais também podem ir iguais).
