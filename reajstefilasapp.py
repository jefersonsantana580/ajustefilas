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

    # Controle de ocupação por dia
    ocupacao_dia = {d: 0 for d in dias}

    # Processa MODELO por MODELO
    for modelo, grupo in df_mes.groupby('MODELO'):
        filas = grupo.sort_values(['DATA PLANEJADA','NR_FILA'])

        for idx, row in filas.iterrows():
            d = row['DATA PLANEJADA']

            # Garante que a data está dentro do range do mês
            if d not in ocupacao_dia:
                d = max([x for x in dias if x <= d])

            # Volta no calendário até achar vaga
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
    pend = df_mes.sort_values(['DATA PLANEJADA','NR_FILA']).copy()

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
    df = pd.read_excel(uploaded, sheet_name='Planilha1')
    df['DATA PLANEJADA'] = pd.to_datetime(df['DATA PLANEJADA']).dt.normalize()
    feriados = parse_feriados(feriados_text)

    res_c1, res_c2 = {}, {}

    for mes, df_mes in df.groupby(df['DATA PLANEJADA'].dt.to_period('M')):
        dias = dias_uteis_mes(df_mes['DATA PLANEJADA'], feriados)
        res_c1.update(aplicar_cenario1(df_mes, dias, capacidade_por_dia))
        res_c2.update(aplicar_cenario2(df_mes, dias, capacidade_por_dia))

    df['NV DATA CENARIO 1'] = df.index.map(res_c1)
    df['NV DATA CENARIO 2'] = df.index.map(res_c2)

    
    st.success("✅ Cenário 1 e Cenário 2 gerados corretamente")
    st.dataframe(df.head(50))

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='RESULTADO')

    st.download_button(
        "📥 Baixar Excel",
        data=output.getvalue(),
        file_name='nivelamento_final_opcaoB.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
