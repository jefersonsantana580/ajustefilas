# -*- coding: utf-8 -*-
import io
import base64
from datetime import timedelta
import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------
# Configuração da página
# ---------------------------------------
st.set_page_config(page_title="Nivelamento de Filas - Mês a Mês", layout="wide")
st.title("📊 Nivelamento de Filas (Cenários 1 e 2)")


st.markdown(
    """
**Como funciona**
- Lê o Excel (aba **Planilha1**) com: **NR_FILA, MÊS OFFLINE, DATA PLANEJADA, MERCADO, COD PRODUTO, MODELO**.
- Considera **dias úteis** (Seg–Sex) e remove **feriados** informados abaixo.
- **Daily Rate** (capacidade/dia) configurável.
- Gera **Cenário 1** (FIFO por **MODELO**, antecipação mínima) e **Cenário 2** (cascata **FIFO global** por mês), sem cruzar meses.
- Exporta Excel com **NV DATA CENARIO 1, NV DATA CENARIO 2, DIF G - C, DIF H - C** e aba **CONFIG**.
    """
)

# ---------------------------------------
# Upload do arquivo
# ---------------------------------------
uploaded = st.file_uploader("📥 Envie o arquivo Excel base (com a aba Planilha1)", type=["xlsx"])

DEFAULT_DAILY = 18

# ---------------------------------------
# Utilidades
# ---------------------------------------

def try_read_config(xls):
    """Lê aba CONFIG se existir e retorna (capacidade_por_dia, lista_feriados)"""
    capacidade = DEFAULT_DAILY
    feriados = []
    if "CONFIG" in xls.sheet_names:
        cfg = pd.read_excel(xls, sheet_name="CONFIG")
        cols = {str(c).strip().upper(): c for c in cfg.columns}
        if "CAPACIDADE_POR_DIA" in cols and pd.notna(cfg[cols["CAPACIDADE_POR_DIA"]].iloc[0]):
            try:
                capacidade = int(cfg[cols["CAPACIDADE_POR_DIA"]].iloc[0])
            except Exception:
                capacidade = DEFAULT_DAILY
        fer_cols = [cols[k] for k in cols if k in ("FERIADOS","FERIADO","HOLIDAYS","HOLIDAY")]
        if fer_cols:
            for v in cfg[fer_cols[0]].dropna():
                try:
                    feriados.append(pd.to_datetime(v).normalize())
                except Exception:
                    pass
    return capacidade, sorted(set(feriados))


def normalize_dates(series):
    s = pd.to_datetime(series)
    return pd.to_datetime(s.dt.date)  # normaliza para 00:00


def business_days(eom: pd.Timestamp, holidays: set):
    first_day = eom.replace(day=1)
    days = pd.date_range(first_day, eom, freq='D')
    bdays = [d.normalize() for d in days if d.weekday() < 5]
    if holidays:
        bdays = [d for d in bdays if d not in holidays]
    return bdays


def ensure_columns(df):
    required = ['NR_FILA','MÊS OFFLINE','DATA PLANEJADA','MERCADO','COD PRODUTO','MODELO']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes em Planilha1: {missing}")


# ---------------------------------------
# Parâmetros (GUI)
# ---------------------------------------

default_rate = DEFAULT_DAILY
default_holidays_text = ""

if uploaded is not None:
    xls_tmp = pd.ExcelFile(uploaded)
    default_rate, fer_list = try_read_config(xls_tmp)
    if fer_list:
        default_holidays_text = "\n".join([d.strftime("%Y-%m-%d") for d in fer_list])

col_a, col_b = st.columns([1,2], gap="large")
with col_a:
    capacidade_por_dia = st.number_input("⚙️ Daily Rate (capacidade por dia)", min_value=1, max_value=500,
                                         value=int(default_rate), step=1)
with col_b:
    holidays_text = st.text_area(
        "📅 Feriados (um por linha, formato AAAA-MM-DD). Ex.: 2026-04-21",
        value=default_holidays_text, height=120, placeholder="2026-04-21\n2026-05-01"
    )


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
            st.warning(f"Não consegui interpretar este feriado: {line}")
    return hs


# ---------------------------------------
# Algoritmos dos cenários
# ---------------------------------------

def cenario2_cascata_fifo(df_month: pd.DataFrame, days: list, capacidade_por_dia: int) -> dict:
    """
    Cenário 2: CASCATA FIFO GLOBAL (por DATA_PLANEJADA, depois NR_FILA),
    preenchendo até 'capacidade_por_dia' por dia útil, antecipando o mínimo possível,
    sem cruzar de mês.
    Retorna dict {idx: data_alocada}
    """
    out = {}
    order = df_month.sort_values(['DATA PLANEJADA','NR_FILA']).index.tolist()
    p = 0
    total = len(order)
    for d in days:
        for _ in range(capacidade_por_dia):
            if p >= total:
                break
            idx = order[p]
            out[idx] = d
            p += 1
        if p >= total:
            break
    return out


def cenario1_fifo_model_min_antec(df_month: pd.DataFrame, days: list, capacidade_por_dia: int) -> dict:
    """
    Cenário 1: Por MODELO, FIFO, com antecipação mínima (somente quando sobra capacidade),
    mantendo o mês, dias úteis e capacidade/dia.
    """
    out = {}
    planned = df_month['DATA PLANEJADA']
    model_q = {m: g.sort_values(['DATA PLANEJADA','NR_FILA']).index.tolist()
               for m, g in df_month.groupby('MODELO')}
    models = list(model_q.keys())
    if not models:
        return out

    for d in days:
        filled = 0
        # Passo 1: sem antecipar (planned <= d)
        visited = 0
        ptr = 0
        m_count = len(models)
        while filled < capacidade_por_dia and visited < m_count:
            m = models[ptr]
            q = model_q[m]
            placed = False
            while q:
                idx = q[0]
                if planned[idx].normalize() <= d:
                    out[idx] = d
                    q.pop(0)
                    filled += 1
                    placed = True
                    break
                else:
                    break
            if not placed:
                visited += 1
            else:
                visited = 0
            ptr = (ptr + 1) % m_count

        # Passo 2: antecipação mínima (menor planned futura entre cabeças)
        while filled < capacidade_por_dia:
            candidates = []
            for m in models:
                q = model_q[m]
                if q:
                    idx = q[0]
                    candidates.append((idx, planned[idx].normalize(), m))
            if not candidates:
                break
            candidates.sort(key=lambda x: (x[1], x[0]))
            idx_pick, _, m_pick = candidates[0]
            out[idx_pick] = d
            model_q[m_pick].pop(0)
            filled += 1

        if all(len(q)==0 for q in model_q.values()):
            break

    return out


# ---------------------------------------
# Botão principal
# ---------------------------------------
if uploaded is not None and st.button("🚀 Gerar Nivelamento"):
    try:
        xls = pd.ExcelFile(uploaded)
        df = pd.read_excel(xls, sheet_name="Planilha1")
        ensure_columns(df)

        # Normaliza datas
        df['MÊS OFFLINE'] = normalize_dates(df['MÊS OFFLINE'])
        df['DATA PLANEJADA'] = normalize_dates(df['DATA PLANEJADA'])

        # Garante colunas de saída
        for col in ['NV DATA CENARIO 1','NV DATA CENARIO 2']:
            if col not in df.columns:
                df[col] = pd.NaT

        # Lê feriados
        holidays = parse_holidays(holidays_text)

        # Processa mês a mês
        nv1_map = {}
        nv2_map = {}
        for eom, g_month in df.groupby('MÊS OFFLINE'):
            days = business_days(eom, holidays)
            if not days:
                continue

            # Cenário 2 - CASCATA FIFO GLOBAL
            c2 = cenario2_cascata_fifo(g_month, days, int(capacidade_por_dia))
            nv2_map.update(c2)

            # Cenário 1 - FIFO por MODELO, antecipação mínima
            c1 = cenario1_fifo_model_min_antec(g_month, days, int(capacidade_por_dia))
            nv1_map.update(c1)

        # Aplica resultados
        df['NV DATA CENARIO 1'] = pd.to_datetime(df.index.map(nv1_map).fillna(pd.NaT))
        df['NV DATA CENARIO 2'] = pd.to_datetime(df.index.map(nv2_map).fillna(pd.NaT))

        # Diferenças em dias
        df['DIF G - C'] = (df['NV DATA CENARIO 1'] - df['DATA PLANEJADA']).dt.days
        df['DIF H - C'] = (df['NV DATA CENARIO 2'] - df['DATA PLANEJADA']).dt.days

        st.success("Nivelamento concluído! Veja prévias e faça o download abaixo.")

        # Pivot de conferência — Cenário 2
        with st.expander("📈 Pivot de conferência — Cenário 2 (Modelo x Data)"):
            if df['NV DATA CENARIO 2'].notna().any():
                piv2 = pd.pivot_table(
                    df.dropna(subset=['NV DATA CENARIO 2']),
                    index='MODELO',
                    columns='NV DATA CENARIO 2',
                    values='COD PRODUTO',
                    aggfunc='count',
                    fill_value=0
                ).sort_index(axis=1)
                st.dataframe(piv2, use_container_width=True)
            else:
                st.info("Sem dados alocados no Cenário 2.")

        # Prévia
        st.subheader("Prévia da Planilha1 (primeiras 200 linhas)")
        st.dataframe(df.head(200), use_container_width=True)

        # Excel para download
        config_out = pd.DataFrame({
            'CAPACIDADE_POR_DIA': [int(capacidade_por_dia)],
            'FERIADOS': [pd.NaT] if not holidays else sorted(list(holidays))
        })

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Planilha1')
            config_out.to_excel(writer, index=False, sheet_name='CONFIG')

        st.download_button(
            label="💾 Baixar Excel Nivelado",
            data=buffer.getvalue(),
            file_name="RESULTADO_NIVELAMENTO.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"Erro ao processar: {e}")
else:
    st.info("Envie o Excel (Planilha1) para habilitar o processamento.")
