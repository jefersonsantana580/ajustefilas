# -*- coding: utf-8 -*-
"""
Nivelamento de Filas - Cenários 1, 2 e 3
- Cenário 1: FIFO por MODELO com antecipação mínima
- Cenário 2: FIFO global com balanceamento diário por MODELO
- Cenário 3: Modelos leves ficam próximos da DATA PLANEJADA,
             espalhando quando houver acúmulo no mesmo dia
Autor: M365 Copilot p/ Jeferson Santana
"""

import io
import pandas as pd
import streamlit as st

# =====================================================
# Configuração da página
# =====================================================

st.set_page_config(page_title="Nivelamento de Filas", layout="wide")
st.title("📊 Nivelamento de Filas – Cenários 1, 2 e 3")

# =====================================================
# Parâmetro interno fixo
# =====================================================

JANELA_ESPALHAMENTO_C3 = 2

# =====================================================
# Sidebar – Parâmetros visíveis
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

MESES_PT = {
    1: "janeiro",
    2: "fevereiro",
    3: "março",
    4: "abril",
    5: "maio",
    6: "junho",
    7: "julho",
    8: "agosto",
    9: "setembro",
    10: "outubro",
    11: "novembro",
    12: "dezembro"
}


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
    Tenta localizar a coluna MES OFFLINE mesmo que o nome esteja diferente.
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


def formatar_mes_portugues(data):
    if pd.isna(data):
        return ""
    return f"{MESES_PT[data.month]}/{data.year}"


def ordenar_periodos(periodos):
    return sorted(periodos)


def periodo_para_texto(periodo):
    return f"{MESES_PT[periodo.month]}/{periodo.year}"


def texto_para_periodo(texto):
    """
    Converte 'maio/2026' -> Period('2026-05', 'M')
    """
    nome_mes, ano = texto.split("/")
    mapa_reverso = {v: k for k, v in MESES_PT.items()}
    mes_num = mapa_reverso[nome_mes.lower()]
    return pd.Period(f"{int(ano)}-{mes_num:02d}", freq="M")


def distancia_em_dias_uteis(data_alvo, data_candidata, ordem_dias):
    """
    Mede distância em quantidade de posições na lista de dias úteis.
    """
    if data_candidata not in ordem_dias:
        return 9999

    if data_alvo in ordem_dias:
        return abs(ordem_dias[data_candidata] - ordem_dias[data_alvo])

    mais_proximo = min(ordem_dias.keys(), key=lambda d: abs((d - data_alvo).days))
    return abs(ordem_dias[data_candidata] - ordem_dias[mais_proximo])


def calcular_desvio_dias(data_nova, data_original):
    if pd.isna(data_nova) or pd.isna(data_original):
        return pd.NA
    return (pd.to_datetime(data_nova).normalize() - pd.to_datetime(data_original).normalize()).days


def garantir_colunas_resultado(df):
    """
    Garante que as colunas dos cenários existam,
    evitando erro quando houver session_state antigo.
    """
    colunas_data = [
        "NV DATA CENARIO 1",
        "NV DATA CENARIO 2",
        "NV DATA CENARIO 3",
    ]
    colunas_desvio = [
        "CENARIO 1 - DT PLANEJADA",
        "CENARIO 2 - DT PLANEJADA",
        "CENARIO 3 - DT PLANEJADA",
    ]

    for col in colunas_data:
        if col not in df.columns:
            df[col] = pd.NaT

    for col in colunas_desvio:
        if col not in df.columns:
            df[col] = pd.NA

    return df

# =====================================================
# Núcleo de balanceamento diário por MODELO
# =====================================================

def balancear_dia_por_modelo(df_pend, capacidade_dia):
    escolhidos = []

    saldo = df_pend.groupby("MODELO").size().to_dict()
    total = sum(saldo.values())

    if total == 0 or capacidade_dia <= 0:
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
    Cenário 1:
    - FIFO por MODELO
    - antecipação mínima
    - usa a capacidade diária como meta de preenchimento
    - preenche os dias desde o início do mês

    Regra:
    - Em cada dia, só pode ser escolhido o PRIMEIRO item ainda não alocado de cada MODELO
      (isso preserva o FIFO por MODELO)
    - Entre os candidatos elegíveis, escolhe primeiro quem tem DATA PLANEJADA mais próxima
      do dia atual (menor antecipação)
    """

    resultado = {}

    if not dias:
        return resultado

    dias = [pd.to_datetime(d).normalize() for d in dias]

    # Filas por modelo em FIFO
    filas_modelo = {}
    for modelo, grupo in df_mes.groupby("MODELO"):
        filas = grupo.sort_values(["DATA PLANEJADA", "NR_FILA"]).copy()
        filas_modelo[modelo] = filas.index.tolist()

    # Controle de ponteiro por modelo
    ponteiro_modelo = {modelo: 0 for modelo in filas_modelo.keys()}

    # Função para pegar o próximo item elegível do modelo
    def proximo_item_modelo(modelo):
        idxs = filas_modelo[modelo]
        pos = ponteiro_modelo[modelo]

        if pos >= len(idxs):
            return None

        idx = idxs[pos]
        row = df_mes.loc[idx]

        data_planejada = pd.to_datetime(row["DATA PLANEJADA"], errors="coerce")
        if pd.isna(data_planejada):
            return None

        return {
            "idx": idx,
            "modelo": modelo,
            "data_planejada": data_planejada.normalize(),
            "nr_fila": row["NR_FILA"]
        }

    # Preenche dia a dia
    for dia in dias:
        alocados_no_dia = 0

        while alocados_no_dia < capacidade:
            candidatos = []

            for modelo in filas_modelo.keys():
                item = proximo_item_modelo(modelo)
                if item is None:
                    continue

                # só pode antecipar ou manter: data planejada >= dia
                if item["data_planejada"] >= dia:
                    candidatos.append(item)

            # não há mais ninguém elegível para este dia
            if not candidatos:
                break

            # prioridade:
            # 1) menor antecipação possível -> menor DATA PLANEJADA >= dia
            # 2) menor NR_FILA
            # 3) nome do modelo como critério estável
            candidatos = sorted(
                candidatos,
                key=lambda x: (x["data_planejada"], x["nr_fila"], str(x["modelo"]))
            )

            escolhido = candidatos[0]

            resultado[escolhido["idx"]] = dia
            alocados_no_dia += 1

            # avança o ponteiro do modelo escolhido
            ponteiro_modelo[escolhido["modelo"]] += 1

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
# Cenário 3 – Modelos leves próximos da data e espalhados
# =====================================================

def escolher_dia_proximo_espalhado(
    data_alvo,
    modelo,
    dias,
    ocupacao,
    uso_modelo_dia,
    capacidade,
    janela=2
):
    """
    Escolhe o melhor dia para um modelo leve:
    - tenta espalhar o mesmo MODELO em dias diferentes
    - mantém a data o mais próximo possível da DATA PLANEJADA
    - respeita a capacidade diária
    - em empate, prefere o mesmo dia ou posterior
    """
    if pd.isna(data_alvo):
        return None

    data_alvo = pd.to_datetime(data_alvo).normalize()
    ordem_dias = {d: i for i, d in enumerate(dias)}

    dias_livres = [d for d in dias if ocupacao[d] < capacidade]
    if not dias_livres:
        return None

    candidatos_janela = [
        d for d in dias_livres
        if distancia_em_dias_uteis(data_alvo, d, ordem_dias) <= janela
    ]

    candidatos = candidatos_janela if candidatos_janela else dias_livres

    melhor_dia = min(
        candidatos,
        key=lambda d: (
            1 if uso_modelo_dia.get((modelo, d), 0) > 0 else 0,
            distancia_em_dias_uteis(data_alvo, d, ordem_dias),
            ocupacao[d],
            0 if d >= data_alvo else 1,
            abs((d - data_alvo).days)
        )
    )

    return melhor_dia


def aplicar_cenario3(df_mes, dias, capacidade, janela_espalhamento=2):
    """
    Cenário 3:
    - Modelos com volume no mês <= número de dias úteis do mês => modelos leves
    - Modelos leves:
        * ficam próximos da DATA PLANEJADA
        * se houver acúmulo no mesmo dia, espalha para dias úteis próximos
        * tenta evitar concentrar o mesmo MODELO no mesmo dia
    - Modelos pesados:
        * seguem no balanceamento padrão por MODELO
    - Sempre respeita a capacidade diária
    """
    resultado = {}

    if not dias:
        return resultado

    dias = [pd.to_datetime(d).normalize() for d in dias]
    ocupacao = {d: 0 for d in dias}
    uso_modelo_dia = {}

    volume_modelo = df_mes.groupby("MODELO").size()

    # Modelo leve = volume do mês <= quantidade de dias úteis do mês
    modelos_leves = set(volume_modelo[volume_modelo <= len(dias)].index)

    df_leves = df_mes[df_mes["MODELO"].isin(modelos_leves)].copy()
    df_pesados = df_mes[~df_mes["MODELO"].isin(modelos_leves)].copy()

    df_leves = df_leves.sort_values(["DATA PLANEJADA", "NR_FILA"])
    df_pesados = df_pesados.sort_values(["DATA PLANEJADA", "NR_FILA"])

    # 1) Aloca modelos leves próximos da data, espalhando se necessário
    for idx, row in df_leves.iterrows():
        modelo = row["MODELO"]
        data_original = pd.to_datetime(row["DATA PLANEJADA"], errors="coerce")

        melhor_dia = escolher_dia_proximo_espalhado(
            data_alvo=data_original,
            modelo=modelo,
            dias=dias,
            ocupacao=ocupacao,
            uso_modelo_dia=uso_modelo_dia,
            capacidade=capacidade,
            janela=janela_espalhamento
        )

        if melhor_dia is not None:
            resultado[idx] = melhor_dia
            ocupacao[melhor_dia] += 1
            uso_modelo_dia[(modelo, melhor_dia)] = uso_modelo_dia.get((modelo, melhor_dia), 0) + 1

    # 2) Aloca modelos pesados nas vagas restantes
    pend = df_pesados.copy()

    for d in dias:
        if pend.empty:
            break

        vagas = capacidade - ocupacao[d]
        if vagas <= 0:
            continue

        escolhidos = balancear_dia_por_modelo(pend, vagas)

        for idx in escolhidos:
            resultado[idx] = d
            ocupacao[d] += 1

            modelo = pend.loc[idx, "MODELO"]
            uso_modelo_dia[(modelo, d)] = uso_modelo_dia.get((modelo, d), 0) + 1

        pend = pend.drop(index=escolhidos)

    return resultado

# =====================================================
# Geração do nivelamento
# =====================================================

if uploaded and st.button("🚀 Gerar Nivelamento"):
    df = pd.read_excel(uploaded, sheet_name="Planilha1", engine="openpyxl")

    # Limpa resultados anteriores para evitar conflito de session_state antigo
    st.session_state.pop("df_resultado", None)
    st.session_state.pop("col_mes_offline", None)

    # Valida colunas mínimas
    colunas_obrigatorias = ["DATA PLANEJADA", "MODELO", "NR_FILA"]
    faltantes = [c for c in colunas_obrigatorias if c not in df.columns]

    if faltantes:
        st.error(f"❌ Colunas obrigatórias ausentes no Excel: {', '.join(faltantes)}")
        st.stop()

    # Identifica a coluna MES OFFLINE
    col_mes_offline = encontrar_coluna_mes(df)
    if col_mes_offline is None:
        st.error("❌ Não encontrei a coluna 'MES OFFLINE' no arquivo. Verifique o nome da coluna.")
        st.stop()

    # Converte datas
    df["DATA PLANEJADA"] = pd.to_datetime(df["DATA PLANEJADA"], errors="coerce").dt.normalize()
    df[col_mes_offline] = pd.to_datetime(df[col_mes_offline], errors="coerce")

    if df["DATA PLANEJADA"].isna().all():
        st.error("❌ A coluna 'DATA PLANEJADA' não possui datas válidas.")
        st.stop()

    feriados = parse_feriados(feriados_text)

    res_c1, res_c2, res_c3 = {}, {}, {}

    # Nivelamento por mês da DATA PLANEJADA
    for mes, df_mes in df.groupby(df["DATA PLANEJADA"].dt.to_period("M")):
        dias = dias_uteis_mes(df_mes["DATA PLANEJADA"], feriados)

        if not dias:
            continue

        res_c1.update(aplicar_cenario1(df_mes, dias, capacidade_por_dia))
        res_c2.update(aplicar_cenario2(df_mes, dias, capacidade_por_dia))
        res_c3.update(
            aplicar_cenario3(
                df_mes=df_mes,
                dias=dias,
                capacidade=capacidade_por_dia,
                janela_espalhamento=JANELA_ESPALHAMENTO_C3
            )
        )

    # Garante colunas antes de preencher
    df = garantir_colunas_resultado(df)

    # Colunas de resultado
    df["NV DATA CENARIO 1"] = pd.to_datetime(df.index.map(res_c1), errors="coerce")
    df["NV DATA CENARIO 2"] = pd.to_datetime(df.index.map(res_c2), errors="coerce")
    df["NV DATA CENARIO 3"] = pd.to_datetime(df.index.map(res_c3), errors="coerce")

    # Colunas de desvio
    df["CENARIO 1 - DT PLANEJADA"] = df.apply(
        lambda r: calcular_desvio_dias(r["NV DATA CENARIO 1"], r["DATA PLANEJADA"]),
        axis=1
    )
    df["CENARIO 2 - DT PLANEJADA"] = df.apply(
        lambda r: calcular_desvio_dias(r["NV DATA CENARIO 2"], r["DATA PLANEJADA"]),
        axis=1
    )
    df["CENARIO 3 - DT PLANEJADA"] = df.apply(
        lambda r: calcular_desvio_dias(r["NV DATA CENARIO 3"], r["DATA PLANEJADA"]),
        axis=1
    )

    # Salva no estado
    st.session_state["df_resultado"] = df.copy()
    st.session_state["col_mes_offline"] = col_mes_offline

    st.success("✅ Cenários 1, 2 e 3 gerados corretamente")

# =====================================================
# Exibição com filtro por MÊS OFFLINE
# =====================================================

if "df_resultado" in st.session_state:
    df_resultado = st.session_state["df_resultado"].copy()
    col_mes_offline = st.session_state.get("col_mes_offline")

    if col_mes_offline is None or col_mes_offline not in df_resultado.columns:
        st.warning("⚠️ Resultado disponível, mas a coluna de mês offline não foi encontrada.")
        st.stop()

    # Garante colunas de resultado para evitar KeyError
    df_resultado = garantir_colunas_resultado(df_resultado)

    # Converte datas com segurança
    df_resultado[col_mes_offline] = pd.to_datetime(df_resultado[col_mes_offline], errors="coerce")

    for col in ["DATA PLANEJADA", "NV DATA CENARIO 1", "NV DATA CENARIO 2", "NV DATA CENARIO 3"]:
        if col in df_resultado.columns:
            df_resultado[col] = pd.to_datetime(df_resultado[col], errors="coerce")

    st.subheader("🔎 Filtro por mês")

    meses_disponiveis = (
        df_resultado[col_mes_offline]
        .dropna()
        .dt.to_period("M")
        .drop_duplicates()
    )

    meses_disponiveis = ordenar_periodos(meses_disponiveis)
    opcoes_mes = ["Todos"] + [periodo_para_texto(m) for m in meses_disponiveis]

    mes_selecionado = st.selectbox(
        f"Filtrar por mês ({col_mes_offline})",
        options=opcoes_mes,
        index=0
    )

    df_filtrado = df_resultado.copy()

    if mes_selecionado != "Todos":
        periodo = texto_para_periodo(mes_selecionado)
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

    # MÊS OFFLINE como maio/2026
    if col_mes_offline in df_view.columns:
        datas_mes = pd.to_datetime(df_view[col_mes_offline], errors="coerce")
        df_view[col_mes_offline] = datas_mes.apply(formatar_mes_portugues)

    # Datas sem hora
    colunas_data = [
        "DATA PLANEJADA",
        "NV DATA CENARIO 1",
        "NV DATA CENARIO 2",
        "NV DATA CENARIO 3"
    ]

    for col in colunas_data:
        if col in df_view.columns:
            df_view[col] = pd.to_datetime(df_view[col], errors="coerce").dt.strftime("%d/%m/%Y")

    df_view = df_view.fillna("")

    st.dataframe(df_view, use_container_width=True, hide_index=True)

    # =====================================================
    # Download do resultado completo
    # =====================================================

    df_download = df_resultado.copy()

    if col_mes_offline in df_download.columns:
        df_download[col_mes_offline] = pd.to_datetime(
            df_download[col_mes_offline], errors="coerce"
        ).dt.date

    for col in ["DATA PLANEJADA", "NV DATA CENARIO 1", "NV DATA CENARIO 2", "NV DATA CENARIO 3"]:
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
        file_name="nivelamento_final_cenarios_1_2_3.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
