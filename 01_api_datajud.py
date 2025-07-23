import streamlit as st
import pandas as pd
from time import time, sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import json
import os
from datetime import datetime

# ----------------------------
def consultar_processo(numero, url, headers, max_retries=3, delay=1.0):
    payload = json.dumps({"query": {"match": {"numeroProcesso": numero}}})
    for tentativa in range(1, max_retries + 1):
        try:
            response = requests.post(url, headers=headers, data=payload, timeout=20)
            response.raise_for_status()
            data = response.json()
            hits = data.get("hits", {}).get("hits", [])
            registros = []
            for hit in hits:
                registro = hit["_source"]
                registro["_id"] = hit.get("_id")
                registros.append(registro)
            sleep(delay)
            return (numero, registros, None)
        except Exception as e:
            if tentativa < max_retries:
                sleep(delay)
            else:
                return (numero, None, f"Tentativas esgotadas ({tentativa}): {e}")

# ----------------------------
def identificar_lotes_salvos(pasta):
    return [
        int(f.split("_")[1].split(".")[0])
        for f in os.listdir(pasta)
        if f.startswith("lote_") and f.endswith(".parquet")
    ]

# ----------------------------
def identificar_lotes_vazios(pasta):
    caminho = os.path.join(pasta, "lotes_vazios.jsonl")
    if not os.path.exists(caminho):
        return set()
    with open(caminho, "r", encoding="utf-8") as f:
        return {json.loads(linha)["lote"] for linha in f if linha.strip()}

# ----------------------------
def carregar_processos_parquet(caminho):
    return pd.read_parquet(caminho, columns=["Processo"])

# ----------------------------
st.title("Consulta API DATAJUD v13 - com Salvamento por Lotes")
st.subheader("Consulta de processos TJSP previamente extraídos")

pasta_entrada = "./app10/dados-processos"
arquivos_parquet = [f for f in os.listdir(pasta_entrada) if f.endswith(".parquet")]
if not arquivos_parquet:
    st.warning("Nenhum arquivo .parquet encontrado.")
    st.stop()

arquivo_selecionado = st.selectbox("- Selecione o arquivo de entrada", arquivos_parquet)
caminho_arquivo = os.path.join(pasta_entrada, arquivo_selecionado)

nome_pasta_saida = st.text_input("📁 Pasta final consolidada", value="app10/resultado-api/")
nome_arquivo_saida = st.text_input("Nome do arquivo final", value="resultado_api")
pasta_batches = st.text_input("📁 Pasta para salvar os batches", value="app10/resultado-api/lotes/")
os.makedirs(pasta_batches, exist_ok=True)

col1, col2 = st.columns([2, 1])
with col2:
    usar_todos = st.checkbox("Selecionar todos os processos (não recomendado)")
with col1:
    n_max = st.number_input("Número máximo de processos", min_value=1, value=100, disabled=usar_todos)

st.markdown("### Parâmetros de Processamento")
n_threads = st.slider("Threads", 1, 20, 5)
batch_size = 100
lote_inicio = st.number_input("Começar a partir do lote...", min_value=1, value=1)

# ----------------------------
if st.button("▶️ Iniciar consulta"):
    try:
        df_proc = carregar_processos_parquet(caminho_arquivo)
        lista_novos = df_proc["Processo"].tolist()
        qtde = len(lista_novos) if usar_todos else min(n_max, len(lista_novos))
        lista_novos = lista_novos[:qtde]

        url = "https://api-publica.datajud.cnj.jus.br/api_publica_tjsp/_search"
        headers = {
            'Authorization': 'ApiKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==',
            'Content-Type': 'application/json'
        }

        lotes_salvos = set(identificar_lotes_salvos(pasta_batches))
        lotes_vazios = identificar_lotes_vazios(pasta_batches)

        for i in range((lote_inicio - 1) * batch_size, qtde, batch_size):
            lote_idx = i // batch_size + 1
            if lote_idx in lotes_salvos or lote_idx in lotes_vazios:
                if lote_idx % 500 == 0:
                    st.info(f"📁 Lote {lote_idx} já registrado como processado ou vazio, pulando...")
                continue

            batch = lista_novos[i:i + batch_size]
            st.write(f"- Processando lote {lote_idx} com {len(batch)} processos...")
            resultados_lote = []
            start_lote = time()

            processos_sem_dados = []

            with ThreadPoolExecutor(max_workers=n_threads) as executor:
                futures = {executor.submit(consultar_processo, numero, url, headers): numero for numero in batch}
                for future in as_completed(futures):
                    numero = futures[future]
                    try:
                        _, registros, erro = future.result()
                        if erro:
                            st.warning(f"- Erro no processo {numero}: {erro}")
                        elif registros:
                            resultados_lote.extend(registros)
                        else:
                            processos_sem_dados.append(numero)
                    except Exception as e:
                        st.error(f"- Erro inesperado no processo {numero}: {e}")

            if resultados_lote:
                df_lote = pd.DataFrame(resultados_lote)
                df_lote["data_download"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                for coluna in ["classe", "assuntos", "orgaoJulgador", "vara", "grau"]:
                    if coluna in df_lote.columns:
                        df_lote[coluna] = df_lote[coluna].apply(lambda x: str(x))


                caminho_lote = os.path.join(pasta_batches, f"lote_{lote_idx:03}.parquet")
                df_lote.to_parquet(caminho_lote, index=False, compression="zstd")
                st.success(f"✅ Lote {lote_idx} salvo em {time() - start_lote:.2f}s.")
            else:
                # Registra lote vazio
                with open(os.path.join(pasta_batches, "lotes_vazios.jsonl"), "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "lote": lote_idx,
                        "quantidade_processos": len(batch),
                        "timestamp": datetime.now().isoformat()
                    }) + "\n")

            if processos_sem_dados:
                with open(os.path.join(pasta_batches, "processos_sem_dados.jsonl"), "a", encoding="utf-8") as log_proc:
                    for numero in processos_sem_dados:
                        log_proc.write(json.dumps({
                            "processo": numero,
                            "lote": lote_idx,
                            "timestamp": datetime.now().isoformat()
                        }) + "\n")

    except Exception as e:
        st.error(f"Erro: {e}")

# ----------------------------
if st.button("📦 Consolidar todos os batches"):
    try:
        arquivos = sorted([
            os.path.join(pasta_batches, f)
            for f in os.listdir(pasta_batches) if f.endswith(".parquet")
        ])
        dfs = [pd.read_parquet(f) for f in arquivos]
        df_total = pd.concat(dfs, ignore_index=True)
        os.makedirs(nome_pasta_saida, exist_ok=True)
        caminho_final = os.path.join(nome_pasta_saida, f"{nome_arquivo_saida}_completo.parquet")
        df_total.to_parquet(caminho_final, index=False, compression="zstd")
        st.success(f"✅ Arquivo consolidado salvo em: {caminho_final}")
    except Exception as e:
        st.error(f"Erro ao consolidar: {e}")
