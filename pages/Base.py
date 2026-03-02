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

colL, colR = st.columns([3, 1])
with colR:
    st.write(f"Logado: **{st.session_state.user}**")
    if st.button("Voltar para Site"):
        st.switch_page("site_coletor.py")

if st.button("Atualizar Base"):
    st.cache_data.clear()
    st.rerun()

# ✅ Busca no banco
rows = db.get_base_coletores()
df = pd.DataFrame(rows)

if df.empty:
    st.info("Sem dados para exibir.")
    st.stop()

# =========================================================
# ✅ AJUSTE DE FUSO (Supabase UTC -> Brasil)
# =========================================================
tz_br = ZoneInfo("America/Sao_Paulo")

# DATA_REGISTRO vem do db como datetime (geralmente timestamptz/UTC)
df["DATA_REGISTRO_DT"] = pd.to_datetime(df["DATA_REGISTRO"], errors="coerce")

# Converte para fuso BR
# - Se vier timezone-aware: tz_convert
# - Se vier naive: assume UTC e converte
if df["DATA_REGISTRO_DT"].dt.tz is not None:
    df["DATA_REGISTRO_BR"] = df["DATA_REGISTRO_DT"].dt.tz_convert(tz_br)
else:
    df["DATA_REGISTRO_BR"] = df["DATA_REGISTRO_DT"].dt.tz_localize("UTC").dt.tz_convert(tz_br)

# DATA e HORA (texto) já no horário local
df["DATA_REGISTRO"] = df["DATA_REGISTRO_BR"].dt.strftime("%d/%m/%Y")
df["HORA_REGISTRO"] = df["DATA_REGISTRO_BR"].dt.strftime("%I:%M %p")
# Se preferir 24h, use:
# df["HORA_REGISTRO"] = df["DATA_REGISTRO_BR"].dt.strftime("%H:%M")

# Excel: usuário/colaborador
mapa = carregar_usuarios_xlsx("usuarios.xlsx")
df["USUARIO"] = df["ID_COLABORADOR"].astype(str).fillna("").str.strip()
df["COLABORADOR"] = df["USUARIO"].map(mapa).fillna("")

# Leadtime operação (só para EM OPERAÇÃO) - usando fuso BR
now_br = pd.Timestamp.now(tz=tz_br)

def calc_lt(row) -> str:
    if row["STATUS_COLETOR"] != "EM OPERAÇÃO" or pd.isna(row["DATA_REGISTRO_BR"]):
        return ""
    delta = now_br - row["DATA_REGISTRO_BR"]
    return fmt_hhmmss(delta)

df["LEADTIME OPERAÇÃO"] = df.apply(calc_lt, axis=1)

# --------- VISÃO FINAL (nomes únicos) ----------
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

# ✅ segurança extra: remove duplicadas (se ocorrer por algum motivo)
df_view = df_view.loc[:, ~df_view.columns.duplicated()]

st.dataframe(df_view, use_container_width=True, hide_index=True)