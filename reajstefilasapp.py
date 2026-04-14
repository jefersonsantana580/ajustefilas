
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
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.add(pd.to_datetime(ln).normalize())
        except Exception:
            pass
    return out


def dias_uteis_mes(datas, feriados):
    datas_validas = pd.to_datetime(datas, errors="coerce").dropna()
    if datas_validas.empty:
        return []

    first = datas_validas.min().replace(day=1)
    last = datas_validas.max()

    dias = pd.date_range(first, last, freq="D")
    dias = [
        d.normalize()
        for d in dias
        if d.weekday() < 5 and d.normalize() not in feriados
    ]
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

    saldo = df_pend.groupby("MODELO").size().to_dict()
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
            df_pend[df_pend["MODELO"] == modelo]
            .sort_values(["DATA PLANEJADA", "NR_FILA"])
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

    if not dias:
        return resultado

    ocupacao_dia = {d: 0 for d in dias}

    for modelo, grupo in df_mes.groupby("MODELO"):
        filas = grupo.sort_values(["DATA PLANEJADA", "NR_FILA"])

        for idx, row in filas.iterrows():
            d = row["DATA PLANEJADA"]

            # Se a data não estiver dentro do range de dias úteis, ajusta
            if d not in ocupacao_dia:
                dias_validos = [x for x in dias if x <= d]
                if dias_validos:
                    d = max(dias_validos)
                else:
                    d = dias[0]

            # Enquanto estiver cheio, volta para o dia útil anterior
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

    if not dias:
        return resultado

    pend = df_mes.sort_values(["DATA PLANEJADA", "NR_FILA"]).copy()

    for d in dias:
        if pend.empty:
            break

        escolhidos = balancear_dia_por_modelo(pend, capacidade)

        for idx in escolhidos:
            resultado[idx] = d

        pend = pend.drop(index=escolhidos)

    return resultado

# =====================================================
# Botão de geração
# =====================================================

if uploaded and st.button("🚀 Gerar Nivelamento"):
    df = pd.read_excel(uploaded, sheet_name="Planilha1", engine="openpyxl")

    # Identifica a coluna de mês offline
    col_mes_offline = encontrar_coluna_mes(df)
    if col_mes_offline is None:
        st.error("❌ Não encontrei a coluna 'MES OFFLINE' no arquivo. Verifique o nome da coluna.")
        st.stop()

    # Converte colunas de data
    df["DATA PLANEJADA"] = pd.to_datetime(df["DATA PLANEJADA"], errors="coerce").dt.normalize()
    df[col_mes_offline] = pd.to_datetime(df[col_mes_offline], errors="coerce")

    # Validação mínima
    if df["DATA PLANEJADA"].isna().all():
        st.error("❌ A coluna 'DATA PLANEJADA' não possui datas válidas.")
        st.stop()

    feriados = parse_feriados(feriados_text)

    res_c1, res_c2 = {}, {}

    # Continua nivelando por mês da DATA PLANEJADA
    for mes, df_mes in df.groupby(df["DATA PLANEJADA"].dt.to_period("M")):
        dias = dias_uteis_mes(df_mes["DATA PLANEJADA"], feriados)

        if not dias:
            continue

        res_c1.update(aplicar_cenario1(df_mes, dias, capacidade_por_dia))
        res_c2.update(aplicar_cenario2(df_mes, dias, capacidade_por_dia))

    df["NV DATA CENARIO 1"] = pd.to_datetime(df.index.map(res_c1), errors="coerce")
    df["NV DATA CENARIO 2"] = pd.to_datetime(df.index.map(res_c2), errors="coerce")

    # Salva o resultado para não perder ao alterar o filtro
    st.session_state["df_resultado"] = df.copy()
    st.session_state["col_mes_offline"] = col_mes_offline

    st.success("✅ Cenário 1 e Cenário 2 gerados corretamente")

# =====================================================
# Exibição com filtro por MES OFFLINE
# =====================================================

if "df_resultado" in st.session_state:
    df_resultado = st.session_state["df_resultado"].copy()
    col_mes_offline = st.session_state["col_mes_offline"]

    # Garante datetime
    df_resultado[col_mes_offline] = pd.to_datetime(df_resultado[col_mes_offline], errors="coerce")
    df_resultado["DATA PLANEJADA"] = pd.to_datetime(df_resultado["DATA PLANEJADA"], errors="coerce")
    df_resultado["NV DATA CENARIO 1"] = pd.to_datetime(df_resultado["NV DATA CENARIO 1"], errors="coerce")
    df_resultado["NV DATA CENARIO 2"] = pd.to_datetime(df_resultado["NV DATA CENARIO 2"], errors="coerce")

    st.subheader("🔎 Filtro por mês")

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

    # =====================================================
    # Indicadores rápidos
    # =====================================================

    c1, c2, c3 = st.columns(3)
    c1.metric("Total de filas", len(df_filtrado))
    c2.metric("Modelos únicos", df_filtrado["MODELO"].nunique() if "MODELO" in df_filtrado.columns else 0)
    c3.metric("Capacidade por dia", capacidade_por_dia)

    # =====================================================
    # Formatação somente para exibição
    # =====================================================

    df_view = df_filtrado.copy()

    # MES OFFLINE como MM/YYYY
    if col_mes_offline in df_view.columns:
        df_view[col_mes_offline] = pd.to_datetime(
            df_view[col_mes_offline], errors="coerce"
        ).dt.strftime("%m/%Y")

    # Demais datas sem hora
    colunas_data = ["DATA PLANEJADA", "NV DATA CENARIO 1", "NV DATA CENARIO 2"]
    for col in colunas_data:
        if col in df_view.columns:
            df_view[col] = pd.to_datetime(
                df_view[col], errors="coerce"
            ).dt.strftime("%d/%m/%Y")

    df_view = df_view.fillna("")

    st.dataframe(df_view, use_container_width=True, hide_index=True)

    # =====================================================
    # Download do Excel
    # =====================================================

    # Se quiser baixar apenas o filtrado, mantenha df_filtrado.
    # Se quiser baixar o resultado completo sempre, troque por df_resultado.
    df_download = df_filtrado.copy()

    if col_mes_offline in df_download.columns:
        df_download[col_mes_offline] = pd.to_datetime(
            df_download[col_mes_offline], errors="coerce"
        ).dt.date

    for col in ["DATA PLANEJADA", "NV DATA CENARIO 1", "NV DATA CENARIO 2"]:
        if col in df_download.columns:
            df_download[col] = pd.to_datetime(
                df_download[col], errors="coerce"
            ).dt.date

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_download.to_excel(writer, index=False, sheet_name="RESULTADO")

    st.download_button(
        "📥 Baixar Excel",
        data=output.getvalue(),
        file_name="nivelamento_final_opcaoB.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

