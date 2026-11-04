#!/usr/bin/env python3
"""Amostra pra conferência de ponta a ponta do WhatsApp na Inatividade.

Mesma conta da tela (bq.pipeline_activity + roster), com e sem o canal
WhatsApp. Imprime JSON com duas listas:
  saiu   — deals que estariam inativos só com e-mail/ligação e deixam de
           estar por causa do WhatsApp (os maiores em valor + sorteados);
  ficou  — deals que continuam inativos, cujo dono teve conversa de WhatsApp
           no período (candidatos a falso negativo).
Cada item: deal_id, deal_name, owner_id (dono no HubSpot), ae, estágio, SLA,
dias com/sem WhatsApp e a data do último retorno por WhatsApp que a tela usa.
Uso: wa_verify_sample.py [n_por_lista]"""
import json, sys, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
import pandas as pd
import app, bq

n = int(sys.argv[1]) if len(sys.argv) > 1 else 15
with app.app.test_request_context("/pipeline/stalled-activity"):
    df = bq.pipeline_activity(force_refresh=True)
    df = app._apply_roster_current(df)
    df = df[df["leader"].isin(app.ALLOWED_LEADERS)]
    df = df[~df["account_executive"].isin(app.HIDDEN_FROM_AE_VIEWS)]
today = pd.Timestamp.utcnow().normalize()
t = lambda c: pd.to_datetime(df[c], errors="coerce", utc=True)
sem = pd.concat([t("last_email_at"), t("last_call_at")], axis=1).max(axis=1).fillna(t("stage_changed_at"))
df["dias_sem_wa"] = (today - sem.dt.normalize()).dt.days
df["sla"] = df["stage_curr"].map(app._ACTIVITY_THRESHOLDS)
df["wa"] = t("last_whatsapp_at").dt.strftime("%Y-%m-%d")
own = bq.client().query("""SELECT CAST(deal_id AS STRING) deal_id, CAST(ANY_VALUE(account_executive_id) AS STRING) owner_id
  FROM `tractian-bi.dbt_analytics_dev.int_adding_names_to_deals` GROUP BY 1""").to_dataframe()
df["deal_id"] = df["deal_id"].astype(str)
df = df.merge(own, on="deal_id", how="left")
saiu = df[(df.dias_sem_wa > df.sla) & (df.days_since_activity <= df.sla)].sort_values("amount", ascending=False)
ficou = df[df.days_since_activity > df.sla]
pick = lambda d, k: pd.concat([d.head(k // 2), d.iloc[k // 2:].sample(min(k - k // 2, max(len(d) - k // 2, 0)), random_state=7)])
cols = ["deal_id", "deal_name", "owner_id", "account_executive", "stage_curr", "sla", "dias_sem_wa",
        "days_since_activity", "wa"]
out = {"hoje_utc": str(today.date()), "n_universo": len(df),
       "inativos_sem_wa": int((df.dias_sem_wa > df.sla).sum()), "inativos_com_wa": int((df.days_since_activity > df.sla).sum()),
       "n_saiu": len(saiu),
       "saiu": pick(saiu, n)[cols].assign(stage=lambda x: x.stage_curr.map(app._STALLED_STAGE_LABELS)).to_dict("records"),
       "ficou": ficou.sample(min(n, len(ficou)), random_state=11)[cols].assign(stage=lambda x: x.stage_curr.map(app._STALLED_STAGE_LABELS)).to_dict("records")}
print(json.dumps(out, ensure_ascii=False, default=str))
