# API DataJud 

O objetivo deste projeto é **extrair e estruturar dados de processos do Tribunal de Justiça de São Paulo (TJSP)** disponibilizados por meio da API pública do CNJ DataJud.  

O pipeline possui duas etapas principais:  
1. **Download do Painel de Estatísticas do CNJ** → onde são obtidos os números CNJ dos processos.  
2. **Consulta à API DataJud** → acessa os dados detalhados desses processos em lotes de 100, salvando os resultados em arquivos Parquet.  

Dessa forma, temos um fluxo completo de **coleta, parsing e armazenamento** de dados processuais, pronto para análises posteriores.  

📚 A documentação oficial da API pode ser encontrada [aqui](https://datajud-wiki.cnj.jus.br/).  
📊 O Painel de Estatísticas está disponível [neste link](https://justica-em-numeros.cnj.jus.br/painel-estatisticas/).  

---

## 📂 Estrutura do projeto

```bash

├── app/
│   ├── dados-extraidos/  # CSVs extraídos do ZIP
│   ├── dados-processos/  # .parquet com números de processo
│   └── resultado-api/
│       ├── lotes/  # Arquivos Parquet por lote com dados da API
│       └── resultado_api_completo.parquet  # Arquivo consolidado final
│       
├── requirements.txt        # Dependências do projeto

```
---

## ⚙️ Modo de utilização

### 0. Instalação  
Crie o ambiente virtual e instale as dependências:

```bash
python -m venv venv
venv\Scripts\activate   # (Windows)
# ou source venv/bin/activate (Linux/Mac)

pip install -r requirements.txt

```

### 1. App – Download & Parsing CNJ

O primeiro app realiza:

- Download do .zip do CNJ (TJSP);
- Extração dos CSVs;
- Parsing da coluna Processo, com limpeza e deduplicação;
- Geração do arquivo tjsp_processos.parquet.

Para rodar:

```bash
streamlit run 00_download_painel_CNJ.py
```

### 2. App – Consulta API DataJud

O segundo app realiza:

- Leitura dos processos em tjsp_processos.parquet;
- Consulta à API DataJud em lotes de 100 processos;
- Execução paralela com múltiplas threads;
- Salvamento incremental de arquivos Parquet por lote;
- Registro de lotes vazios e processos sem dados;
- Consolidação final em resultado_api_completo.parquet.

Para rodar:

```bash
streamlit run 01_api_datajud.py
```

### Funcionalidades implementadas

- Retry e robustez no download;
- Consulta paralela com threads ajustáveis;
- Persistência incremental (retoma de onde parou);
- Logs de erros, lotes vazios e processos sem dados.

### Limitações atuais

- Apenas o TJSP está contemplado, mas a inclusão de outros tribunais é simples;
- Armazenamento ainda local, sem centralização em Lakehouse/DB.

### Próximos passos

- Integração com banco de dados;
- Criação de uma tabela só para os movimentos;
- Deploy como pipeline agendado (orquestração).
- Controle de versionamento via execucao_id e data_execucao.



🔗 [LinkedIn](https://www.linkedin.com/in/matheus-oscar/) | 💻 [GitHub](https://github.com/matheus-oscar)