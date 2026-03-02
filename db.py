import streamlit as st
import psycopg
from psycopg.rows import tuple_row
from typing import Dict, Optional, List, Any

# ======= Config do banco via st.secrets =======
# Agora o código vai falhar de propósito (e te avisar) se faltar algo no secrets.toml
try:
    PGHOST = st.secrets["PGHOST"]
    PGPORT = st.secrets.get("PGPORT", "6543")
    PGDATABASE = st.secrets.get("PGDATABASE", "postgres")
    PGUSER = st.secrets["PGUSER"]
    PGPASSWORD = st.secrets["PGPASSWORD"]
except KeyError as e:
    raise RuntimeError(
        f"Credencial ausente no .streamlit/secrets.toml: {e}. "
        "Verifique se o arquivo existe e contém todas as chaves."
    )

def conectar():
    """
    Conexão psycopg estrita via st.secrets.
    """
    return psycopg.connect(
        host=PGHOST,
        port=int(PGPORT),
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode="require",
        row_factory=tuple_row,
        prepare_threshold=None,  # ESSENCIAL com transaction pooler do Supabase
    )

# Atalho (compatibilidade com seu projeto)
get_conn = conectar
# ---------------------------
# LOGIN / USUÁRIO (public.usuario)
# colunas: IDUsuario, NomeUsuario, Senha, Ativo
# ---------------------------
def verificar_login(usuario: str, senha: str) -> bool:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.usuario
                WHERE "IDUsuario"=%s AND "Senha"=%s AND "Ativo"=true
                """,
                (usuario, senha),
            )
            return cur.fetchone()[0] > 0


def usuario_existe(id_usuario: str) -> bool:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.usuario
                WHERE "IDUsuario"=%s
                """,
                (id_usuario,),
            )
            return cur.fetchone()[0] > 0


def inserir_usuario(id_usuario: str, nome_usuario: str, senha: str) -> None:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.usuario ("IDUsuario", "NomeUsuario", "Senha", "Ativo")
                VALUES (%s, %s, %s, true)
                """,
                (id_usuario, nome_usuario, senha),
            )
        conn.commit()


# ---------------------------
# TOTAIS COLETORES
# ---------------------------
def get_totais_coletores() -> Dict[str, int]:
    totais = {
        "EM OPERACAO": 0,
        "DISPONIVEL": 0,
        "EM CONSERTO": 0,
        "INATIVO": 0,
        "EXTRAVIADO": 0,
    }

    sql_query = """
    WITH ultimo AS (
        SELECT DISTINCT ON (b."IDColetor")
            b."IDColetor"::text AS id_coletor_txt,
            b."IDRegistro"::int AS id_registro
        FROM public."LG_ControleColetores" b
        ORDER BY b."IDColetor", b."DataRegistro" DESC
    )
    SELECT
        COUNT(DISTINCT TRIM(a."IDColetores"::text)) AS qtd_coletores,
        CASE
            WHEN u.id_registro = 4 THEN 'DISPONIVEL'
            WHEN u.id_registro = 1 THEN 'EM OPERACAO'
            WHEN u.id_registro = 3 THEN 'EM CONSERTO'
            WHEN u.id_registro = 5 THEN 'EXTRAVIADO'
            WHEN u.id_registro = 6 THEN 'INATIVO'
            WHEN u.id_registro IS NULL OR u.id_registro = 2 THEN 'DISPONIVEL'
        END AS status_coletor
    FROM public.coletores_cadastro a
    LEFT JOIN ultimo u
      ON TRIM(a."IDColetores"::text) = TRIM(u.id_coletor_txt)
    GROUP BY 2;
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            for qtd, status in cur.fetchall():
                if status in totais:
                    totais[status] = qtd

    return totais


# ---------------------------
# BASE COLETORES (último registro por coletor)
# ---------------------------
def get_base_coletores() -> List[Dict[str, Any]]:
    sql = """
    WITH ult AS (
      SELECT DISTINCT ON (lc."IDColetor")
        lc."DataRegistro"   AS data_registro,
        lc."IDColetor"      AS id_coletor,
        lc."IDRegistro"     AS id_registro,
        lc."IDColaborador"  AS id_colaborador
      FROM public."LG_ControleColetores" lc
      WHERE lc."DataRegistro"::date >= DATE '2022-01-01'
      ORDER BY lc."IDColetor", lc."DataRegistro" DESC
    )
    SELECT
      TRIM(cc."IDColetores"::text) AS id_coletor,
      TRIM(cc."NumSerie"::text)    AS num_serial_coletor,
      TRIM(cc."Setor"::text)       AS setor,
      ult.data_registro            AS data_registro,
      TRIM(ult.id_colaborador::text) AS id_colaborador,
      CASE
        WHEN ult.id_registro IS NULL THEN 'DISPONÍVEL'
        WHEN ult.id_registro IN (2,4) THEN 'DISPONÍVEL'
        WHEN ult.id_registro = 3 THEN 'ENVIADO PARA CONSERTO'
        WHEN ult.id_registro = 1 THEN 'EM OPERAÇÃO'
        WHEN ult.id_registro = 6 THEN 'INATIVO'
        WHEN ult.id_registro = 5 THEN 'EXTRAVIADO'
        ELSE 'DISPONÍVEL'
      END AS status_coletor
    FROM public.coletores_cadastro cc
    LEFT JOIN ult
      ON TRIM(cc."IDColetores"::text) = TRIM(ult.id_coletor::text)
    ORDER BY ult.data_registro DESC NULLS LAST;
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

    result: List[Dict[str, Any]] = []
    for (id_coletor, num_serial, setor, data_registro, id_colaborador, status) in rows:
        result.append({
            "ID_COLETOR": id_coletor,
            "NUM_SERIAL_COLETOR": num_serial,
            "SETOR": setor,
            "DATA_REGISTRO": data_registro,
            "ID_COLABORADOR": id_colaborador,
            "STATUS_COLETOR": status,
        })

    return result


def get_ultimo_movimento_coletor(id_coletor: str):
    """
    Retorna o último movimento do coletor (IDRegistro, DataRegistro, IDColaborador).
    Se não houver registros, retorna None.
    """
    sql = """
    SELECT
      b."IDRegistro"::int,
      b."DataRegistro",
      b."IDColaborador"::text
    FROM public."LG_ControleColetores" b
    WHERE b."IDColetor"::text = %s
    ORDER BY b."DataRegistro" DESC
    LIMIT 1;
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (str(id_coletor).strip(),))
            row = cur.fetchone()
            if not row:
                return None
            id_registro, data_registro, id_colaborador = row
            return {
                "id_registro": id_registro,
                "data_registro": data_registro,
                "id_colaborador": (id_colaborador or "").strip(),
            }