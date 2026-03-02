# mov_validacoes.py
# ------------------------------------------------------------
# Validações e inserts de coletores (Supabase/Postgres)
# ------------------------------------------------------------
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict
from datetime import datetime
import psycopg

import db as _db


def get_conn():
    return _db.conectar()


ID_REGISTRO = {
    "ENTREGA": 1,
    "DEVOLUCAO": 2,
    "ENVIO": 3,
    "RETORNO": 4,
    "EXTRAVIO": 5,
    "INATIVO": 6,
}

STATUS_BY_IDREG = {
    1: "EM OPERACAO",
    2: "DISPONIVEL",
    3: "EM CONSERTO",
    4: "DISPONIVEL",
    5: "EXTRAVIADO",
    6: "INATIVO",
    None: "DISPONIVEL",
}


@dataclass
class MovDados:
    id_registro: int
    id_coletor: str
    id_colaborador: Optional[str]
    realizado_teste: bool
    detectado_defeito: bool
    sinaliza_conserto: bool
    observacao: Optional[str]
    resp_processo: str
    data_envio_conserto: Optional[str]   # YYYY-MM-DD
    chamado: Optional[str]
    data_retorno_conserto: Optional[str] # YYYY-MM-DD


@dataclass
class DefeitoItem:
    id_registro: int
    id_coletor: str
    id_defeito: str
    resp_processo: str


def yyyymmdd(date_iso: Optional[str]) -> Optional[str]:
    if not date_iso:
        return None
    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    return dt.strftime("%Y%m%d")


def norm_coletor_py(v: str) -> str:
    v = (v or "").strip()
    if v.isdigit():
        v2 = v.lstrip("0")
        return v2 if v2 != "" else "0"
    return v


def _coletor_norm(expr_sql: str) -> str:
    return f"""
    CASE
      WHEN trim({expr_sql}) ~ '^[0-9]+$'
        THEN COALESCE(NULLIF(regexp_replace(trim({expr_sql}), '^0+', ''), ''), '0')
      ELSE trim({expr_sql})
    END
    """


def _get_ultimo_mov_do_coletor(id_coletor: str):
    """
    Retorna (IDRegistro:int, IDColaborador) do ÚLTIMO movimento do coletor,
    unificando variações como '73' e '000073'.
    """
    sql = f"""
    WITH base AS (
      SELECT
        trim("IDColetor"::text) AS id_coletor_trim,
        trim("IDColaborador"::text) AS id_colaborador,
        "IDRegistro"::int AS id_registro,
        "DataRegistro" AS data_registro,
        {_coletor_norm('"IDColetor"::text')} AS id_coletor_norm
      FROM public."LG_ControleColetores"
    )
    SELECT id_registro, id_colaborador
    FROM base
    WHERE id_coletor_norm = %s
    ORDER BY data_registro DESC, id_registro DESC
    LIMIT 1;
    """
    p = norm_coletor_py(id_coletor)
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (p,))
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)


def _status_atual(id_coletor: str) -> Tuple[str, Optional[str]]:
    last_idreg, last_colab = _get_ultimo_mov_do_coletor(id_coletor)
    return STATUS_BY_IDREG.get(last_idreg, "DISPONIVEL"), last_colab


def validar_bipagem(acao: str, id_coletor: Optional[str], id_resp: Optional[str]) -> Tuple[bool, str]:
    id_coletor = (id_coletor or "").strip()
    id_resp = (id_resp or "").strip()
    if id_coletor == "" and id_resp != "":
        return False, "É necessário bipar o endereço do coletor para processar os dados."
    if id_coletor != "" and id_resp == "" and acao.upper() in ("DEVOLUCAO", "ENTREGA", "ENVIO", "RETORNO"):
        return False, "É necessário bipar o crachá do colaborador para processar os dados."
    return True, ""


def _colaborador_tem_coletor_em_operacao(id_resp: str) -> Optional[str]:
    sql = f"""
    WITH base AS (
      SELECT
        trim("IDColetor"::text) AS id_coletor_trim,
        trim("IDColaborador"::text) AS id_colaborador,
        "IDRegistro"::int AS id_registro,
        "DataRegistro" AS data_registro,
        {_coletor_norm('"IDColetor"::text')} AS id_coletor_norm
      FROM public."LG_ControleColetores"
    ),
    ultimo AS (
      SELECT DISTINCT ON (id_coletor_norm)
        id_coletor_trim, id_colaborador, id_registro, data_registro, id_coletor_norm
      FROM base
      ORDER BY id_coletor_norm, data_registro DESC, id_registro DESC
    )
    SELECT id_coletor_trim
    FROM ultimo
    WHERE id_registro = 1
      AND id_colaborador = trim(%s)
    LIMIT 1;
    """
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (id_resp.strip(),))
        row = cur.fetchone()
        return row[0] if row else None


def validar_regras_de_status(acao: str, id_coletor: str, id_resp: Optional[str]) -> Tuple[bool, str]:
    ac = acao.upper()
    status, last_colab = _status_atual(id_coletor)

    # ENTREGA
    if ac == "ENTREGA" and (id_resp or "").strip():
        atual = _colaborador_tem_coletor_em_operacao(id_resp)
        if atual:
            return False, f"{id_resp} já está com o coletor {atual}. EFETUE A DEVOLUÇÃO para prosseguir."

    # DEVOLUÇÃO
    if ac == "DEVOLUCAO":
        if status == "DISPONIVEL":
            return False, f"{id_coletor} não está em operação para ser devolvido. EFETUE ENTREGA para prosseguir."

        # ✅ Se estava EXTRAVIADO ou INATIVO, permitir devolução para voltar a DISPONÍVEL
        if status in ("EXTRAVIADO", "INATIVO"):
            return True, ""

        #if status == "INATIVO" and id_resp:
            #return False, f"Retorno de coletor que estava {status}."

        if status == "EM OPERACAO" and id_resp and last_colab and id_resp.strip() != last_colab.strip():
            return False, f"{last_colab} que estava com esse coletor. Verifique o USUÁRIO CORRETO para prosseguir."

    # ENVIO (conserto)
    if ac == "ENVIO":
        if status == "EM OPERACAO":
            return False, f"{id_coletor} está em operação. EFETUE DEVOLUÇÃO para prosseguir."
        if status == "EM CONSERTO":
            return False, f"{id_coletor} já está em conserto. EFETUE RETORNO para prosseguir."

    # RETORNO (conserto)
    if ac == "RETORNO" and status != "EM CONSERTO":
        return False, f"{id_coletor} não foi enviado para conserto. EFETUE O ENVIO para prosseguir."

    return True, ""


def inserir_mov_principal(d: MovDados) -> None:
    sql = """
        INSERT INTO public."LG_ControleColetores"
        ("DataRegistro", "IDRegistro", "IDColetor", "IDColaborador",
         "RealizadoTeste", "DetectadoDefeito", "Observacao")
        VALUES
        (now(), %s, %s, %s, %s, %s, %s);
    """
    params = (
        d.id_registro,
        d.id_coletor.strip(),
        (d.id_colaborador or "").strip() or None,
        1 if d.realizado_teste else 0,
        1 if d.detectado_defeito else 0,
        (d.observacao or "").strip() or None,
    )
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, params)
        cn.commit()


def processar_movimentacao(
    acao_ui: str,
    id_coletor: str,
    id_resp: Optional[str],
    realizado_teste: bool,
    detectado_defeito: bool,
    sinaliza_conserto: bool,
    observacao: Optional[str],
    resp_processo: str,
    data_envio_conserto: Optional[str],
    chamado: Optional[str],
    data_retorno_conserto: Optional[str],
    lista_defeitos_escolhidos: Optional[List[str]] = None,
) -> Tuple[bool, str]:

    acao_norm = acao_ui.strip().upper()
    if acao_norm.startswith("ENTREGA"):
        ac = "ENTREGA"
    elif acao_norm.startswith("DEVOLU"):
        ac = "DEVOLUCAO"
    elif acao_norm.startswith("ENVIO"):
        ac = "ENVIO"
    elif acao_norm.startswith("RETORNO"):
        ac = "RETORNO"
    elif "EXTRAVI" in acao_norm:
        ac = "EXTRAVIO"
    elif "INATIV" in acao_norm:
        ac = "INATIVO"
    else:
        return False, f"Ação não reconhecida: {acao_ui}"

    id_reg = ID_REGISTRO[ac]
    id_coletor = (id_coletor or "").strip()
    id_resp = (id_resp or "").strip()

    ok, msg = validar_bipagem(ac, id_coletor, id_resp)
    if not ok:
        return False, msg

    ok, msg = validar_regras_de_status(ac, id_coletor, id_resp)
    if not ok:
        return False, msg

    # ✅ usa o último movimento interno (normaliza 000001 vs 1)
    last_idreg, _last_colab = _get_ultimo_mov_do_coletor(id_coletor)

    # ✅ Regras adicionais SOMENTE para ENTREGA
    if ac == "ENTREGA":
        if last_idreg == 1:
            return False, "Coletor já está em uso. Aguarde a devolução/retorno desse coletor."
        if last_idreg in (3, 5, 6):
            mapa = {3: "em conserto", 5: "extraviado", 6: "inativo"}
            return False, f"Não é possível entregar: coletor está {mapa[last_idreg]}."

    mov = MovDados(
        id_registro=id_reg,
        id_coletor=id_coletor,
        id_colaborador=id_resp,
        realizado_teste=realizado_teste,
        detectado_defeito=detectado_defeito,
        sinaliza_conserto=sinaliza_conserto,
        observacao=observacao,
        resp_processo=resp_processo,
        data_envio_conserto=data_envio_conserto,
        chamado=chamado,
        data_retorno_conserto=data_retorno_conserto,
    )

    try:
        inserir_mov_principal(mov)
        return True, "Movimentação registrada com sucesso."
    except psycopg.Error as e:
        return False, f"Erro de banco: {e}"
    except Exception as e:
        return False, f"Falha ao processar movimentação: {e}"


def consultar_coletor_resumo(id_coletor: str) -> Tuple[Optional[str], str, Optional[str]]:
    sql = """
        WITH ultimo AS (
            SELECT
                "IDRegistro"::int AS id_registro,
                trim("IDColaborador"::text) AS id_colaborador,
                "DataRegistro" AS data_registro
            FROM public."LG_ControleColetores"
            WHERE trim("IDColetor"::text) = trim(%s)
            ORDER BY "DataRegistro" DESC, "IDRegistro" DESC
            LIMIT 1
        )
        SELECT
            trim(cc."NumSerie"::text) AS num_serie,
            COALESCE(lp."DescProcesso", 'DISPONIVEL') AS status,
            u.id_colaborador
        FROM public.coletores_cadastro cc
        LEFT JOIN ultimo u
            ON true
        LEFT JOIN public."LG_ProcessoColetor" lp
            ON lp."IDRegistro"::int = u.id_registro
        WHERE trim(cc."IDColetores"::text) = trim(%s)
        LIMIT 1;
    """
    p = (id_coletor or "").strip()
    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, (p, p))
        row = cur.fetchone()
        if not row:
            return None, "COLETOR NÃO ENCONTRADO", None
        return row[0], row[1], row[2]


def nome_coletor_ou_usuario(id_busca: str, modo: str) -> Optional[str]:
    if modo.upper() == "COLETOR":
        sql = """
            SELECT trim("NumSerie"::text)
            FROM public.coletores_cadastro
            WHERE trim("IDColetores"::text) = trim(%s)
              AND trim("NumSerie"::text) NOT ILIKE '%%COLETOR%%'
            ORDER BY "IDColetores"
            LIMIT 1;
        """
        params = (id_busca.strip(),)
    else:
        sql = """
            SELECT trim("NomeUsuario"::text)
            FROM public.usuario
            WHERE "Ativo" = true
              AND trim("IDUsuario"::text) = trim(%s)
            LIMIT 1;
        """
        params = (id_busca.strip(),)

    with get_conn() as cn, cn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def status_do_coletor(id_coletor: str) -> Tuple[str, Optional[str]]:
    return _status_atual(id_coletor)