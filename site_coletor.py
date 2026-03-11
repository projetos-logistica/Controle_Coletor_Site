import streamlit as st
from pathlib import Path

import db
from mov_validacoes import (
    processar_movimentacao,
    nome_coletor_ou_usuario,
    status_do_coletor,
    consultar_coletor_resumo,
)
import pandas as pd

@st.cache_data
def carregar_usuarios_xlsx(path: str = "usuarios.xlsx") -> dict:
    df = pd.read_excel(path, dtype=str)
    df.columns = [c.strip().upper() for c in df.columns]

    df["ID_USUARIO"] = df["ID_USUARIO"].astype(str).str.strip()
    df["NOME_COMPLETO"] = df["NOME_COMPLETO"].astype(str).str.strip()

    return dict(zip(df["ID_USUARIO"], df["NOME_COMPLETO"]))

def buscar_nome_responsavel(id_usuario: str) -> str | None:
    mapa = carregar_usuarios_xlsx("usuarios.xlsx")
    return mapa.get((id_usuario or "").strip())

st.set_page_config(page_title="Controle Coletores WMS", layout="wide")

st.markdown("""
<style>
/* reduz padding da página */
.block-container {padding-top: 1rem; padding-bottom: 1rem;}

/* centraliza e limita largura do conteúdo */
.login-wrap {
    max-width: 520px;
    margin: 0 auto;
    padding: 24px 22px;
    border: 1px solid rgba(49, 51, 63, 0.2);
    border-radius: 16px;
    background: white;
}

/* centraliza imagem */
.login-wrap img {display:block; margin: 0 auto 12px auto;}

/* reduz espaçamento entre widgets */
div[data-testid="stVerticalBlock"] > div {gap: 0.35rem;}
</style>
""", unsafe_allow_html=True)

# ----------------------------
# ESTADO
# ----------------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user = None


def gerar_id_usuario(nome_completo: str):
    nome_completo = (nome_completo or "").upper().strip()
    partes = nome_completo.split()
    if len(partes) < 2:
        return None
    primeiro = partes[0].lower()
    ultimo = partes[-1].lower()
    return f"{primeiro}.{ultimo}"


def logout():
    st.session_state.logged_in = False
    st.session_state.user = None


# ----------------------------
# UI: LOGIN / CADASTRO
# ----------------------------
def render_login():
    st.markdown('<div class="login-wrap">', unsafe_allow_html=True)

    # Logo menor e central
    logo_path = Path("assets/Logo Minimalista AZZAS.png")
    if logo_path.exists():
        st.image(str(logo_path), width=260)

    st.markdown("<h3 style='text-align:center;margin:0 0 10px 0;'>Sistema de Cadastro e Login</h3>",
                unsafe_allow_html=True)

    abas = st.tabs(["Login", "Cadastrar"])

    with abas[0]:
        with st.form(key="login_form_v1"):
            usuario = st.text_input("Usuário", key="login_usuario")
            senha = st.text_input("Senha", type="password", key="login_senha")
            entrar = st.form_submit_button("Login", use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Esqueci minha senha", use_container_width=True):
                st.info("Procure o administrador de sistemas para resetar sua senha.")
        with c2:
            st.caption("")

        if entrar:
            if not usuario or not senha:
                st.error("Preencha usuário e senha.")
            else:
                if db.verificar_login(usuario.strip(), senha.strip()):
                    st.session_state.logged_in = True
                    st.session_state.user = usuario.strip()
                    st.rerun()
                else:
                    st.error("Usuário ou senha inválidos.")

    with abas[1]:
        with st.form(key="cadastro_form_v1"):
            nome = st.text_input("Nome completo", key="cad_nome")
            senha_cad = st.text_input("Senha", type="password", key="cad_senha")
            cadastrar = st.form_submit_button("Cadastrar", use_container_width=True)

        if cadastrar:
            if not nome or not senha_cad:
                st.error("Preencha todos os campos!")
            else:
                id_usuario = gerar_id_usuario(nome)
                if not id_usuario:
                    st.error("Digite nome e sobrenome!")
                else:
                    try:
                        if db.usuario_existe(id_usuario):
                            st.error("Usuário já cadastrado!")
                        else:
                            db.inserir_usuario(id_usuario, nome.upper(), senha_cad)
                            st.success(f"Usuário cadastrado! ID: {id_usuario}")
                    except Exception as e:
                        st.error(f"Falha ao cadastrar: {e}")

        st.markdown("</div>", unsafe_allow_html=True)

# --------------------------
# UI: APP PRINCIPAL 
# ----------------------------
ACOES = [
    "Entrega Início operação",
    "Devolução término operação",
    "Envio Conserto",
    "Retorno Conserto",
    "Coletor Extraviado",
    "Coletor Inativo",
]


def render_app():

    if "msg_sucesso" not in st.session_state:
        st.session_state["msg_sucesso"] = None

    # --- estados para manter resultados entre cliques ---
    if "resp_consultado" not in st.session_state:
        st.session_state["resp_consultado"] = False
    if "nome_resp" not in st.session_state:
        st.session_state["nome_resp"] = None

    st.title("Controle Coletores WMS")

    top_left, top_right = st.columns([3, 1])
    with top_right:
        st.write(f"Logado: **{st.session_state.user}**")
        if st.button("Sair"):
            logout()
            st.rerun()

    # Totais
    totais = db.get_totais_coletores()
    colA, colB, colC, colBtn = st.columns([1, 1, 1, 1])
    colA.metric("Em operação", totais.get("EM OPERACAO", 0))
    colB.metric("Disponíveis", totais.get("DISPONIVEL", 0))
    colC.metric("Enviado para Conserto", totais.get("EM CONSERTO", 0))

    with colBtn:
        if st.button("Atualizar Totais"):
            st.rerun()

    st.divider()

    # Ação
    acao_ui = st.radio("Escolha uma ação", ACOES, horizontal=True)

    # Dados principais + consultas
    c1, c2 = st.columns(2)

    with c1:
        id_coletor = st.text_input("Coletor", key="id_coletor")
        if st.button("Consultar coletor"):
            if id_coletor.strip():
                serial, stt, last_colab = consultar_coletor_resumo(id_coletor.strip())
                if serial:
                    msg = f"{serial} | Status: {stt}"
                    if last_colab:
                        msg += f" (colab: {last_colab})"
                    st.info(msg)
                else:
                    st.warning("Coletor não encontrado.")
            else:
                st.warning("Informe um coletor.")

    with c2:
        id_resp = st.text_input("Responsável", key="id_resp")

        if st.button("Consultar responsável"):
            st.session_state["resp_consultado"] = True
            st.session_state["nome_resp"] = buscar_nome_responsavel(id_resp)

        # Só mostra resultado depois que clicar no botão
        if st.session_state["resp_consultado"]:
            nome = st.session_state["nome_resp"]
            if nome:
                st.success(nome if nome.startswith("1 -") else f"1 - {nome}")
            else:
                st.warning("Usuário não encontrado.")

    # Regras de exibição (igual Tkinter)
    mostrar_testes = acao_ui in ["Devolução término operação", "Envio Conserto"]
    mostrar_info = acao_ui in ["Envio Conserto", "Retorno Conserto", "Coletor Extraviado", "Coletor Inativo"]
    mostrar_datas = acao_ui in ["Envio Conserto", "Retorno Conserto"]

    realizado_teste = False
    detectado_defeito = False
    sinaliza_conserto = False
    defeitos_txt = ""
    consideracoes_txt = ""
    data_envio = None
    chamado = None
    data_retorno = None

    if mostrar_testes:
        st.subheader("Testes")
        realizado_teste = st.radio("Teste realizado?", ["SIM", "NÃO"], horizontal=True) == "SIM"
        detectado_defeito = st.radio("Detectado defeito?", ["SIM", "NÃO"], horizontal=True) == "SIM"
        sinaliza_conserto = st.radio("Sinaliza conserto?", ["SIM", "NÃO"], horizontal=True) == "SIM"

    if mostrar_info:
        st.subheader("Informações")
        i1, i2 = st.columns(2)
        with i1:
            defeitos_txt = st.text_area("Defeitos encontrados", height=120)
        with i2:
            consideracoes_txt = st.text_area("Considerações", height=120)

    if mostrar_datas:
        st.subheader("Datas / Chamado")
        d1, d2, d3 = st.columns(3)
        with d1:
            data_envio = st.text_input("Data Envio Conserto (YYYY-MM-DD)")
        with d2:
            chamado = st.text_input("Nº Chamado")
        with d3:
            data_retorno = st.text_input("Data Retorno Conserto (YYYY-MM-DD)")

    st.divider()  # ← fora do if mostrar_datas

    # ✅ Mensagem de sucesso ACIMA do botão Salvar
    if st.session_state.get("msg_sucesso"):
        st.success(st.session_state["msg_sucesso"])
        st.session_state["msg_sucesso"] = None

    b1, b2 = st.columns(2)  # ← fora do if msg_sucesso

    with b1:
        if st.button("Salvar", type="primary"):
            obs = (defeitos_txt or "").strip() or (consideracoes_txt or "").strip() or None

            ok, msg = processar_movimentacao(
                acao_ui=acao_ui,
                id_coletor=(id_coletor or "").strip(),
                id_resp=(id_resp or "").strip(),
                realizado_teste=realizado_teste,
                detectado_defeito=detectado_defeito,
                sinaliza_conserto=sinaliza_conserto,
                observacao=obs,
                resp_processo=st.session_state.user,
                data_envio_conserto=(data_envio.strip() if data_envio else None),
                chamado=(chamado.strip() if chamado else None),
                data_retorno_conserto=(data_retorno.strip() if data_retorno else None),
                lista_defeitos_escolhidos=[],
            )

            if ok:
                st.session_state["msg_sucesso"] = "Coletor retornado com sucesso."
                st.rerun()
            else:
                st.error(msg)

    with b2:
        if st.button("Cancelar"):
            st.rerun()


# ----------------------------
# ROUTER
# ----------------------------
if not st.session_state.logged_in:
    render_login()
else:
    render_app()
