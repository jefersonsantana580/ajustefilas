# Nivelamento de Filas (Streamlit)

App para nivelar filas de produção **mês a mês**, respeitando **dias úteis**, **feriados** e **capacidade diária**.

## 🧩 O que o app faz
- Lê o Excel base (aba **Planilha1**) com as colunas:
  - `NR_FILA`, `MÊS OFFLINE`, `DATA PLANEJADA`, `MERCADO`, `COD PRODUTO`, `MODELO`.
- Considera **dias úteis (Seg–Sex)** e remove **feriados** informados no app (um por linha).  
- Permite definir o **Daily Rate** (capacidade por dia).
- Gera **dois cenários** e acrescenta as colunas:
  - `NV DATA CENARIO 1`: por **MODELO**, **FIFO**, **antecipação mínima**.
  - `NV DATA CENARIO 2`: **cascata FIFO global** por mês, **antecipando o mínimo** e **nivelando** a capacidade por dia.
  - `DIF G - C` e `DIF H - C`: diferença (em dias) entre a nova data e a `DATA PLANEJADA`.
- Exporta um Excel com **Planilha1** resultante e uma aba **CONFIG** (com `CAPACIDADE_POR_DIA` e `FERIADOS`).

## 📦 Requisitos
```
pip install -r requirements.txt
```
> Arquivo `requirements.txt` já incluso.

## ▶️ Como rodar localmente
```bash
# (Opcional) criar ambiente virtual
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

# instalar dependências
pip install -r requirements.txt

# executar
a streamlit run app.py
```
Abra o link do Streamlit mostrado no terminal.

## 🗂️ Estrutura esperada do Excel
Aba **Planilha1** deve conter, no mínimo, as colunas:
```
NR_FILA | MÊS OFFLINE | DATA PLANEJADA | MERCADO | COD PRODUTO | MODELO
```
- `MÊS OFFLINE`: deve ser o **último dia do mês** da programação (ex.: `2026-04-30`).
- `DATA PLANEJADA`: data alvo do item (será normalizada para 00:00).  

> Se existir a aba **CONFIG** no arquivo enviado, o app lê valores padrão:  
> `CAPACIDADE_POR_DIA` (inteiro) e `FERIADOS` (uma data por linha).

## 🧠 Regras dos cenários
### Cenário 1 — Por MODELO, FIFO, antecipação mínima
1. Em cada **dia útil** do mês, agenda primeiro itens do modelo com `DATA PLANEJADA ≤ dia` (sem antecipar).  
2. Se sobrar capacidade no dia, antecipa **minimamente**: escolhe a **menor data planejada futura** entre as cabeças de fila dos modelos.  
3. Mantém **capacidade/dia**, **dias úteis** e **não cruza meses**.

### Cenário 2 — Cascata FIFO global
- Ordena **todo o mês** por `DATA PLANEJADA` e `NR_FILA` (**FIFO** global).  
- Percorre os **dias úteis** do mês na ordem e preenche até o **Daily Rate**.  
- Se faltar carga num dia, puxa o **próximo da ordem** (menor antecipação possível).  
- Se sobrar carga, o excedente **flui** para os dias seguintes (menor postergação possível).  
- **Não cruza meses** e respeita **capacidades** e **feriados**.

## 🧪 Conferência rápida (Pivot)
No Excel gerado, crie uma Tabela Dinâmica:
- **Filtro**: `MÊS OFFLINE`.
- **Colunas**: `NV DATA CENARIO 2` (ou `NV DATA CENARIO 1`).
- **Linhas**: `MODELO`.
- **Valores**: contagem de `COD PRODUTO` (ou `NR_FILA`).
- A linha **Total Geral** deve mostrar o **Daily Rate** por dia útil (ex.: 18).  

## 🛠️ Parametrizações úteis
- **Daily Rate**: ajustável no próprio app (campo “Daily Rate”).  
- **Feriados**: informe no campo de texto (um por linha, formato `AAAA-MM-DD`).  
- (Opcional) você pode enviar um Excel com a aba **CONFIG** para guardar defaults.

## 🚀 Deploy (opcional)
- **Streamlit Cloud**: faça deploy deste repositório, defina o arquivo principal como `app.py`.  
- **Container**: crie uma imagem Python 3.11+, copie `app.py`/`requirements.txt`, instale dependências e exponha a porta padrão do Streamlit (`8501`).

## ❗ Possíveis avisos/limitações
- Se `itens_do_mês > dias_úteis * Daily Rate`, não é possível encaixar tudo mantendo a capacidade/dia.  
  Se precisar, adapte a regra para **estourar último dia**, **carregar para o mês seguinte** ou **sinalizar excedente**.

## 📄 Licença
Uso interno.
