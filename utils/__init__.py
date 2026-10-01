"""
utils - código comum aos robôs de consulta pública (TJAL, TJBA, TJMA, TJRJ, TJRN).

    log.py      log padrão: mesmo formato no terminal e em <TRIBUNAL>/saida/logs/<script>_AAAAMMDD.log
    banco.py    conexão com o Postgres (.env da raiz) e consultas que mais de um robô usa
    texto.py    dígitos, número CNJ e CPF/CNPJ
    arquivos.py CSV no padrão do projeto (';' e UTF-8 com BOM, abre direto no Excel)

Os robôs rodam como script (python TJAL/fetch_TJAL.py), então cada um põe a raiz do projeto no sys.path
antes de importar daqui.
"""
