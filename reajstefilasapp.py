
# -*- coding: utf-8 -*-
"""
Nivelamento de Filas - Cenários 1 e 2 (VERSÃO FINAL)
Opção B:
- Cenário 1: FIFO por MODELO com antecipação mínima
- Cenário 2: FIFO global com balanceamento diário por MODELO
Autor: M365 Copilot p/ Jeferson Santana
"""

import io
import pandas as pd
import streamlit as st

# =====================================================
# Configuração da página
# =====================================================

st.set_page_config(page_title="Nivelamento de Filas", layout="wide")
st.title("📊 Nivelamento de Filas – Cenários 1 e 2 (Balanceados)")

# =====================================================
# Sidebar – Parâmetros
# =====================================================

st.sidebar.header("⚙️ Parâmetros")

capacidade_por_dia = st.sidebar.number_input(
    "Daily Rate (capacidade por dia)",
    min_value=1,
    max_value=500,
    value=18,
    step=1
)

feriados_text = st.sidebar.text_area(
    "Feriados (um por linha, formato AAAA-MM-DD)",
    value="",
    placeholder="Ex:\n2026-04-21\n2026-05-01",
    height=120
)

uploaded = st.file_uploader("📥 Envie o Excel (aba Planilha1)", type=["xlsx"])

# =====================================================
# Utilidades
# =====================================================

def parse_feriados(text):
    out = set()
    for ln in (text or '').splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.add(pd.to_datetime(ln).normalize())
        except Exception:
            pass
    return out


def dias_uteis_mes(datas, feriados):
    first = datas.min().replace(day=1)
    last = datas.max()
    dias = pd.date_range(first, last, freq='D')
    dias = [d.normalize() for d in dias if d.weekday() < 5 and d.normalize() not in feriados]
    return dias


def encontrar_coluna_mes(df):
    """
    Tenta localizar a coluna MES OFFLINE mesmo que o nome esteja um pouco diferente.
    """
    candidatas = [
        "MES OFFLINE",
        "MÊS OFFLINE",
        "MES OFF LINE",
        "MÊS OFF LINE",
        "MÊS OFF-LINE",
        "MES OFF-LINE",
    ]
    for c in candidatas:
        if c in df.columns:
            return c
    return None

# =====================================================
# Núcleo de balanceamento diário por MODELO
# =====================================================

def balancear_dia_por_modelo(df_pend, capacidade_dia):
    escolhidos = []

    saldo = df_pend.groupby('MODELO').size().to_dict()
    total = sum(saldo.values())
    if total == 0:
        return escolhidos

    cotas = {m: int((q / total) * capacidade_dia) for m, q in saldo.items()}
    usados = sum(cotas.values())
    resto = capacidade_dia - usados

    restos_ord = sorted(
        saldo.keys(),
        key=lambda m: ((saldo[m] / total) * capacidade_dia) - cotas[m],
        reverse=True
    )

    for m in restos_ord:
        if resto <= 0:
            break
        cotas[m] += 1
        resto -= 1

    for modelo, qtd in cotas.items():
        idxs = (
            df_pend[df_pend['MODELO'] == modelo]
            .sort_values(['DATA PLANEJADA', 'NR_FILA'])
            .index.tolist()
        )
        escolhidos.extend(idxs[:qtd])

    return escolhidos[:capacidade_dia]

# =====================================================
# Cenário 1 – FIFO por MODELO (antecipação mínima)
# =====================================================

def aplicar_cenario1(df_mes, dias, capacidade):
    """
    Cenário 1 correto:
    - FIFO por MODELO
    - Antecipação mínima
    - Nivelamento apenas quando necessário
    """
    resultado = {}

    ocupacao_dia = {d: 0 for d in dias}

    for modelo, grupo in df_mes.groupby('MODELO'):
        filas = grupo.sort_values(['DATA PLANEJADA', 'NR_FILA'])

        for idx, row in filas.iterrows():
            d = row['DATA PLANEJADA']

            if d not in ocupacao_dia:
                dias_validos = [x for x in dias if x <= d]
                if dias_validos:
                    d = max(dias_validos)
                else:
                    d = dias[0]

            while ocupacao_dia[d] >= capacidade:
                prev_days = [x for x in dias if x < d]
                if not prev_days:
                    break
                d = prev_days[-1]

            resultado[idx] = d
            ocupacao_dia[d] += 1

    return resultado

# =====================================================
# Cenário 2 – FIFO global + balanceamento por MODELO
# =====================================================

def aplicar_cenario2(df_mes, dias, capacidade):
    resultado = {}
    pend = df_mes.sort_values(['DATA PLANEJADA', 'NR_FILA']).copy()

    for d in dias:
        if pend.empty:
            break
        escolhidos = balancear_dia_por_modelo(pend, capacidade)
        for idx in escolhidos:
            resultado[idx] = d
        pend = pend.drop(index=escolhidos)

    return resultado

# =====================================================
# Execução
# =====================================================

if uploaded and st.button("🚀 Gerar Nivelamento"):
    df = pd.read_excel(uploaded, sheet_name='Planilha1', engine='openpyxl')

    # Garantir DATA PLANEJADA como datetime sem hora
    df['DATA PLANEJADA'] = pd.to_datetime(df['DATA PLANEJADA'], errors='coerce').dt.normalize()

    # Identificar coluna MES OFFLINE
    col_mes_offline = encontrar_coluna_mes(df)

    if col_mes_offline is None:
        st.error("❌ Não encontrei a coluna 'MES OFFLINE' no arquivo. Verifique o nome da coluna.")
        st.stop()

    # Garantir MES OFFLINE como datetime
    df[col_mes_offline] = pd.to_datetime(df[col_mes_offline], errors='coerce')

    feriados = parse_feriados(feriados_text)

    res_c1, res_c2 = {}, {}

    for mes, df_mes in df.groupby(df['DATA PLANEJADA'].dt.to_period('M')):
        dias = dias_uteis_mes(df_mes['DATA PLANEJADA'], feriados)
        res_c1.update(aplicar_cenario1(df_mes, dias, capacidade_por_dia))
        res_c2.update(aplicar_cenario2(df_mes, dias, capacidade_por_dia))

    df['NV DATA CENARIO 1'] = pd.to_datetime(df.index.map(res_c1), errors='coerce')
    df['NV DATA CENARIO 2'] = pd.to_datetime(df.index.map(res_c2), errors='coerce')

    # Salva no session_state para manter ao trocar o filtro
    st.session_state["df_resultado"] = df.copy()
    st.session_state["col_mes_offline"] = col_mes_offline

    st.success("✅ Cenário 1 e Cenário 2 gerados corretamente")

# =====================================================
# Exibição com filtro por MES OFFLINE
# =====================================================

if "df_resultado" in st.session_state:
    df_resultado = st.session_state["df_resultado"].copy()
    col_mes_offline = st.session_state["col_mes_offline"]

    st.subheader("🔎 Filtro por mês")

    # Garantir datetime
    df_resultado[col_mes_offline] = pd.to_datetime(df_resultado[col_mes_offline], errors='coerce')

    meses_disponiveis = (
        df_resultado[col_mes_offline]
        .dropna()
        .dt.to_period("M")
        .drop_duplicates()
        .sort_values()
    )

    opcoes_mes = ["Todos"] + [m.strftime("%m/%Y") for m in meses_disponiveis]

    mes_selecionado = st.selectbox(
        f"Filtrar por mês ({col_mes_offline})",
        options=opcoes_mes,
        index=0
    )

    df_filtrado = df_resultado.copy()

    if mes_selecionado != "Todos":
        periodo = pd.Period(
            pd.to_datetime("01/" + mes_selecionado, dayfirst=True),
            freq="M"
        )
        df_filtrado = df_filtrado[
            df_filtrado[col_mes_offline].dt.to_period("M") == periodo
        ].copy()

    # -----------------------------
    # Formatação só para exibição
    # -----------------------------
    df_view = df_filtrado.copy()

    # MES OFFLINE como MM/AAAA
    if col_mes_offline in df_view.columns:
        df_view[col_mes_offline] = pd.to_datetime(
            df_view[col_mes_offline], errors='coerce'
        ).dt.strftime("%m/%Y")

    # DATA PLANEJADA e cenários sem hora
    colunas_data = ['DATA PLANEJADA', 'NV DATA CENARIO 1', 'NV DATA CENARIO 2']
    for col in colunas_data:
        if col in df_view.columns:
            df_view[col] = pd.to_datetime(df_view[col], errors='coerce').dt.strftime("%d/%m/%Y")

    df_view = df_view.fillna("")

    st.dataframe(df_view, use_container_width=True, hide_index=True)

    # =====================================================
    # Download Excel
    # =====================================================
    # Aqui você escolhe se quer baixar o filtrado ou o completo:
    # - para baixar o filtrado: use df_filtrado
    # - para baixar o completo: use df_resultado

    df_download = df_filtrado.copy()   # <- troque para df_resultado se quiser baixar tudo

    # Se quiser também sem hora no Excel:
    if col_mes_offline in df_download.columns:
        df_download[col_mes_offline] = pd.to_datetime(df_download[col_mes_offline], errors='coerce').dt.date

    for col in ['DATA PLANEJADA', 'NV DATA CENARIO 1', 'NV DATA CENARIO 2']:
        if col in df_download.columns:
            df_download[col] = pd.to_datetime(df_download[col], errors='coerce').dt.date

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_download.to_excel(writer, index=False, sheet_name='RESULTADO')

    st.download_button(
        "📥 Baixar Excel",
        data=output.getvalue(),
        file_name='nivelamento_final_opcaoB.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
