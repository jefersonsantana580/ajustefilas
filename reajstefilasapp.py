# -*- coding: utf-8 -*-
"""
Nivelamento de Filas - Cenários 1 e 2
Cenário 2 reutiliza o cálculo de balanceamento diário do app.py
Autor: M365 Copilot p/ Jeferson Santana
"""

import io
import base64
from datetime import timedelta
import pandas as pd
import streamlit as st

# =======================================
# Configuração da página
# =======================================

st.set_page_config(page_title="Nivelamento de Filas - Balanceado", layout="wide")
st.title("📊 Nivelamento de Filas (Cenários 1 e 2 - Balanceamento Unificado)")

st.markdown(
"""**Como funciona (atualizado)**
- Lê o Excel (aba **Planilha1**) com: NR_FILA, MÊS OFFLINE, DATA PLANEJADA, MERCADO, COD PRODUTO, MODELO
- Considera dias úteis (Seg–Sex) e feriados
- Capacidade diária configurável
- **Cenário 2 usa exatamente o mesmo cálculo diário do app.py** (FIFO + capacidade)
- Não cruza meses
"""
)

# =======================================
# Upload
# =======================================

uploaded = st.file_uploader("📥 Envie o arquivo Excel base (Planilha1)", type=["xlsx"])
DEFAULT_DAILY = 18

# =======================================
# Utilidades de calendário (mesmo conceito do app.py)
# =======================================

def parse_holidays(text):
    hs = set()
    if not text:
        return hs
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            hs.add(pd.to_datetime(line).normalize())
        except Exception:
            pass
    return hs


def business_days_month(eom: pd.Timestamp, holidays: set):
    first_day = eom.replace(day=1)
    days = pd.date_range(first_day, eom, freq='D')
    bdays = [d.normalize() for d in days if d.weekday() < 5]
    if holidays:
        bdays = [d for d in bdays if d not in holidays]
    return bdays

# =======================================
# Cálculo reutilizado do app.py
# =======================================

def distribuir_fifo(indices, dias_uteis, capacidade_por_dia):
    """Distribuição FIFO respeitando capacidade diária (núcleo do app.py)"""
    out = {}
    p = 0
    total = len(indices)

    for d in dias_uteis:
        for _ in range(capacidade_por_dia):
            if p >= total:
                break
            out[indices[p]] = d
            p += 1
        if p >= total:
            break
    return out

# =======================================
# Cenários
# =======================================

def cenario2_balanceado(df_month, days, capacidade_por_dia):
    """
    Cenário 2: FIFO global em cascata
    usando o mesmo cálculo diário do app.py
    """
    order = (
        df_month
        .sort_values(['DATA PLANEJADA', 'NR_FILA'])
        .index.tolist()
    )

    return distribuir_fifo(order, days, capacidade_por_dia)

# =======================================
# Execução
# =======================================

if uploaded is not None and st.button("🚀 Gerar Nivelamento"):
    xls = pd.ExcelFile(uploaded)
    df = pd.read_excel(xls, sheet_name="Planilha1")

    feriados_text = st.text_area(
        "📅 Feriados (AAAA-MM-DD)",
        value="",
        height=120
    )
    feriados = parse_holidays(feriados_text)

    capacidade_por_dia = st.number_input(
        "⚙️ Capacidade por dia",
        min_value=1,
        max_value=500,
        value=DEFAULT_DAILY
    )

    df['DATA PLANEJADA'] = pd.to_datetime(df['DATA PLANEJADA']).dt.normalize()

    resultados = {}

    for mes, df_mes in df.groupby(df['DATA PLANEJADA'].dt.to_period('M')):
        eom = df_mes['DATA PLANEJADA'].max()
        days = business_days_month(eom, feriados)

        aloc = cenario2_balanceado(df_mes, days, capacidade_por_dia)
        resultados.update(aloc)

    df['NV DATA CENARIO 2'] = df.index.map(resultados)

    st.success("✅ Cenário 2 gerado usando cálculo do app.py")
    st.dataframe(df.head(50))

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name="RESULTADO")

    st.download_button(
        "📥 Baixar Excel",
        data=output.getvalue(),
        file_name="nivelamento_balanceado.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
