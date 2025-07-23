import os
import streamlit as st
import zipfile
import pandas as pd
from time import time, sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from datetime import datetime

# --------- Funções -----------
def limpar_cache(pastas):
    for pasta in pastas:
        if os.path.exists(pasta):
            for root, dirs, files in os.walk(pasta):
                for f in files:
                    os.remove(os.path.join(root, f))
    st.success("🧹 Cache limpo com sucesso!")

def imprimir_log(texto, tipo="info"):
    cores = {
        "info": "white",
        "sucesso": "green",
        "aviso": "orange",
        "erro": "red"
    }
    cor = cores.get(tipo, "white")
    st.markdown(f"<span style='color:{cor}'>{texto}</span>", unsafe_allow_html=True)

def download_with_retry(url, pasta_destino, nome_arquivo_zip, max_retries=3, chunk_size=1024*1024):
    os.makedirs(pasta_destino, exist_ok=True)
    output_file = os.path.join(pasta_destino, nome_arquivo_zip)

    if os.path.exists(output_file):
        imprimir_log(f"📦 Arquivo já existe: {output_file}", "aviso")
        return output_file

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://justica-em-numeros.cnj.jus.br/"
    }

    retry_strategy = Retry(
        total=5,
        backoff_factor=5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    for attempt in range(1, max_retries + 1):
        imprimir_log(f"Tentativa {attempt} de {max_retries}...", "info")
        try:
            with requests.Session() as session:
                session.mount("https://", adapter)
                session.mount("http://", adapter)

                with session.get(url, headers=headers, stream=True, timeout=180) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get("content-length", 0))
                    downloaded = 0
                    start = time()

                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    with open(output_file, "wb") as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                elapsed = time() - start
                                speed = downloaded / elapsed if elapsed > 0 else 0
                                percent = (downloaded / total_size) * 100 if total_size else 0
                                eta = (total_size - downloaded) / speed if speed else 0

                                progress_bar.progress(min(percent / 100, 1.0))
                                status_text.text(
                                    f"⬇️ {downloaded / (1024**2):.2f} MB "
                                    f"({percent:.2f}%) | Velocidade: {speed / (1024**2):.2f} MB/s | ETA: {eta:.1f}s"
                                )

                    status_text.text("✅ Download concluído com sucesso.")
                    imprimir_log("✅ Download concluído com sucesso.", "sucesso")
                    return output_file

        except Exception as e:
            imprimir_log(f"Erro: {e}", "erro")
            sleep(5)

    imprimir_log("Todas as tentativas de download falharam.", "erro")
    return None

def descompactar_arquivos(zip_path, pasta_destino):
    imprimir_log(f"Iniciando extração de: {zip_path}", "info")
    os.makedirs(pasta_destino, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        lista_arquivos = zip_ref.namelist()
        total = len(lista_arquivos)

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, nome_arquivo in enumerate(lista_arquivos):
            zip_ref.extract(nome_arquivo, pasta_destino)
            progress_bar.progress((i + 1) / total)
            status_text.text(f"Extraindo: {nome_arquivo[-60:]}")

    status_text.text("Extração concluída.")
    imprimir_log(f"Extração concluída. {total} arquivos extraídos.", "sucesso")
    imprimir_log(f"Arquivos extraídos em: {pasta_destino}", "info")

def salvar_coluna_processo(pasta_csvs, destino_parquet, chunksize=500_000):
    imprimir_log(f"Iniciando agrupamento dos csv em: {pasta_csvs}", "info")
    os.makedirs(os.path.dirname(destino_parquet), exist_ok=True)
    lista_chunks = []
    arquivos = [f for f in os.listdir(pasta_csvs) if f.endswith(".csv")]

    progress_bar = st.progress(0)
    total_arquivos = len(arquivos)
    total_linhas = 0

    for i, nome_arquivo in enumerate(arquivos):
        caminho = os.path.join(pasta_csvs, nome_arquivo)
        try:
            for chunk in pd.read_csv(caminho, sep=';', chunksize=chunksize, usecols=["Processo"], low_memory=False):
                chunk["Processo"] = chunk["Processo"].astype(str).str.replace(r"[-.]", "", regex=True)
                chunk = chunk[~chunk["Processo"].str.contains("sigiloso", case=False, na=False)]
                lista_chunks.append(chunk)
                total_linhas += len(chunk)
        except Exception as e:
            imprimir_log(f"Erro ao ler {nome_arquivo}: {e}", "aviso")
        progress_bar.progress((i + 1) / total_arquivos)

    if not lista_chunks:
        st.error("❌ Nenhum dado válido encontrado.")
        return

    df_final = pd.concat(lista_chunks, ignore_index=True)
    df_final.drop_duplicates(subset="Processo", inplace=True)
    df_final["data_download"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_final.to_parquet(destino_parquet, index=False)
    imprimir_log(f"✅ Arquivo salvo em: {destino_parquet} | {total_linhas:,} linhas", "sucesso")

# --------- Interface -----------
st.title("Download Painel CNJ TJSP (v10)")

PASTA_DOWNLOAD = "data-raw"
NOME_ZIP = "TJSP.zip"
PASTA_EXTRAIDA = "app10/dados-extraidos"
PASTA_SAIDA = "app10/dados-processos"
URL = "https://api-csvr.stg.cloud.cnj.jus.br/download_csv?tribunal=TJSP&indicador=&oj=&grau=&municipio=&ambiente=csv_p"
ZIP_PATH = os.path.join(PASTA_DOWNLOAD, NOME_ZIP)
DESTINO_PARQUET = os.path.join(PASTA_SAIDA, "tjsp_processos.parquet")

# Estado
for etapa in ["download_ok", "extracao_ok", "processamento_ok"]:
    if etapa not in st.session_state:
        st.session_state[etapa] = False

if os.path.exists(ZIP_PATH):
    st.session_state.download_ok = True
if os.path.exists(PASTA_EXTRAIDA) and len(os.listdir(PASTA_EXTRAIDA)) > 0:
    st.session_state.extracao_ok = True
if os.path.exists(DESTINO_PARQUET):
    st.session_state.processamento_ok = True

st.subheader("✅ Etapas concluídas:")
st.checkbox("📥 ZIP baixado", value=st.session_state.download_ok, disabled=True)
st.checkbox("📂 Arquivos extraídos", value=st.session_state.extracao_ok, disabled=True)
st.checkbox("🗃️ Processos salvos", value=st.session_state.processamento_ok, disabled=True)

etapa = st.radio("📌 Escolha a etapa a executar:", [
    "Executar processo completo",
    "Somente download",
    "Somente extração",
    "Somente salvar coluna 'Processo'"
])

if st.button("▶️ Executar"):
    start = time()

    if etapa in ["Executar processo completo", "Somente download"]:
        download_with_retry(URL, PASTA_DOWNLOAD, NOME_ZIP)
        st.session_state.download_ok = os.path.exists(ZIP_PATH)

    if etapa in ["Executar processo completo", "Somente extração"]:
        if not os.path.exists(ZIP_PATH):
            st.error("ZIP não encontrado.")
            st.stop()
        descompactar_arquivos(ZIP_PATH, PASTA_EXTRAIDA)
        st.session_state.extracao_ok = True

    if etapa in ["Executar processo completo", "Somente salvar coluna 'Processo'"]:
        if not os.path.exists(PASTA_EXTRAIDA):
            st.error("Pasta extraída não encontrada.")
            st.stop()
        salvar_coluna_processo(PASTA_EXTRAIDA, DESTINO_PARQUET)
        st.session_state.processamento_ok = True

    st.success(f"✅ Etapa finalizada em {time() - start:.2f} segundos")

# Limpeza de cache
st.markdown("---")
st.subheader("🧹 Limpeza de cache")

opcoes_limpeza = {
    "- ZIP baixado": PASTA_DOWNLOAD,
    "- Arquivos extraídos": PASTA_EXTRAIDA,
    "- Processos salvos": PASTA_SAIDA
}

selecionadas = st.multiselect("Selecione as pastas para limpar:", options=list(opcoes_limpeza.keys()))

if st.button("Limpar selecionadas"):
    limpar_cache([opcoes_limpeza[nome] for nome in selecionadas])
    for nome in selecionadas:
        if nome == "- ZIP baixado":
            st.session_state.download_ok = False
        elif nome == "- Arquivos extraídos":
            st.session_state.extracao_ok = False
        elif nome == "- Processos salvos":
            st.session_state.processamento_ok = False
