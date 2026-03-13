import io
import streamlit as st
import pandas as pd
import db
from zoneinfo import ZoneInfo

st.set_page_config(page_title="Base - Controle de Coletores", layout="wide")

# ✅ Só entra logado
if not st.session_state.get("logged_in", False):
    st.warning("Faça login para acessar a Base.")
    st.switch_page("site_coletor.py")


@st.cache_data
def carregar_usuarios_xlsx(path: str = "usuarios.xlsx") -> dict:
    df = pd.read_excel(path, dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]
    df["ID_USUARIO"] = df["ID_USUARIO"].astype(str).str.strip()
    df["NOME_COMPLETO"] = df["NOME_COMPLETO"].astype(str).str.strip()
    return dict(zip(df["ID_USUARIO"], df["NOME_COMPLETO"]))


def fmt_hhmmss(delta: pd.Timedelta) -> str:
    if pd.isna(delta):
        return ""
    total = int(delta.total_seconds())
    if total < 0:
        total = abs(total)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}"


st.title("Base - Controle de Coletores")

# ---- Cards (Totais) ----
totais = db.get_totais_coletores()
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("COLETORES DISPONÍVEIS", totais.get("DISPONIVEL", 0))
c2.metric("COLETORES EM OPERAÇÃO", totais.get("EM OPERACAO", 0))
c3.metric("EM CONSERTO", totais.get("EM CONSERTO", 0))
c4.metric("INATIVO", totais.get("INATIVO", 0))
c5.metric("EXTRAVIADO", totais.get("EXTRAVIADO", 0))

st.divider()

# ✅ Busca no banco (antes do filtro para popular o selectbox)
rows = db.get_base_coletores()
df = pd.DataFrame(rows)

if df.empty:
    st.info("Sem dados para exibir.")
    st.stop()

# =========================================================
# ✅ AJUSTE DE FUSO (Supabase UTC -> Brasil)
# =========================================================
tz_br = ZoneInfo("America/Sao_Paulo")

df["DATA_REGISTRO_DT"] = pd.to_datetime(df["DATA_REGISTRO"], errors="coerce")

if df["DATA_REGISTRO_DT"].dt.tz is not None:
    df["DATA_REGISTRO_BR"] = df["DATA_REGISTRO_DT"].dt.tz_convert(tz_br)
else:
    df["DATA_REGISTRO_BR"] = df["DATA_REGISTRO_DT"].dt.tz_localize("UTC").dt.tz_convert(tz_br)

df["DATA_REGISTRO"] = df["DATA_REGISTRO_BR"].dt.strftime("%d/%m/%Y")
df["HORA_REGISTRO"] = df["DATA_REGISTRO_BR"].dt.strftime("%I:%M %p")

mapa = carregar_usuarios_xlsx("usuarios.xlsx")
df["USUARIO"] = df["ID_COLABORADOR"].astype(str).fillna("").str.strip()
df["COLABORADOR"] = df["USUARIO"].map(mapa).fillna("")

now_br = pd.Timestamp.now(tz=tz_br)

def calc_lt(row) -> str:
    if row["STATUS_COLETOR"] != "EM OPERAÇÃO" or pd.isna(row["DATA_REGISTRO_BR"]):
        return ""
    delta = now_br - row["DATA_REGISTRO_BR"]
    return fmt_hhmmss(delta)

df["LEADTIME OPERAÇÃO"] = df.apply(calc_lt, axis=1)

# --------- VISÃO FINAL ----------
df_view = pd.DataFrame({
    "STATUS COLETOR": df["STATUS_COLETOR"],
    "DATA_REGISTRO": df["DATA_REGISTRO"],
    "HORA_REGISTRO": df["HORA_REGISTRO"],
    "ID_COLETOR": df["ID_COLETOR"],
    "N° SERIAL COLETOR": df["NUM_SERIAL_COLETOR"],
    "USUARIO": df["USUARIO"],
    "COLABORADOR": df["COLABORADOR"],
    "SETOR": df["SETOR"],
    "LEADTIME OPERAÇÃO": df["LEADTIME OPERAÇÃO"],
})

df_view = df_view.loc[:, ~df_view.columns.duplicated()]

# ---- Linha de controles: Filtro de Setor + (Logado / Voltar) ----
colL, colR = st.columns([3, 1])

with colL:
    # Limita a largura do selectbox usando sub-colunas
    sub1, sub2 = st.columns([1, 2])
    with sub1:
        setores = ["Todos"] + sorted(
            df_view["SETOR"].dropna().astype(str).str.strip().unique().tolist()
        )
        setor_escolhido = st.selectbox("Filtrar por Setor", setores, key="filtro_setor")

with colR:
    st.write(f"Logado: **{st.session_state.user}**")
    if st.button("Voltar para Site"):
        st.switch_page("site_coletor.py")

if st.button("Atualizar Base"):
    st.cache_data.clear()
    st.rerun()

# ---- Aplica filtro ----
if setor_escolhido != "Todos":
    df_view = df_view[df_view["SETOR"].astype(str).str.strip() == setor_escolhido]

# ---- Esconde botão CSV nativo do Streamlit ----
st.markdown("""
    <style>
    [data-testid="stDataFrameToolbar"] { display: none; }
    </style>
""", unsafe_allow_html=True)

st.dataframe(df_view, use_container_width=True, hide_index=True)

# ---- Botão de download em XLS ----
output = io.BytesIO()
with pd.ExcelWriter(output, engine="openpyxl") as writer:
    df_view.to_excel(writer, index=False, sheet_name="Base Coletores")

st.download_button(
    label="⬇️ Exportar Base",
    data=output.getvalue(),
    file_name="base_coletores.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
