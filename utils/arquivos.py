"""
arquivos.py - CSV no padrão do projeto: ';' e UTF-8 com BOM (o Excel em pt-BR abre direto, com os acentos certos).
"""
import csv
from pathlib import Path


def gravar_csv(caminho, linhas, colunas=None):
    """Escreve o CSV inteiro (sobrescreve). Sem colunas, usa as chaves das linhas na ordem em que aparecem."""
    if colunas is None:
        colunas = []
        for linha in linhas:
            colunas += [k for k in linha if k not in colunas]
    Path(caminho).parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8-sig", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=colunas or ["vazio"], delimiter=";", extrasaction="ignore")
        escritor.writeheader()
        escritor.writerows(linhas)


def anexar_csv(caminho, colunas, linhas):
    """Acrescenta as linhas ao CSV, escrevendo o cabeçalho (e o BOM) só quando o arquivo é novo. Arquivo que já
    existe segue o cabeçalho dele (as colunas podem ter mudado entre versões do robô): coluna que saiu fica vazia e
    coluna nova fica de fora, para as linhas nunca desalinharem do cabeçalho."""
    novo = not Path(caminho).exists() or Path(caminho).stat().st_size == 0
    if not novo:
        with open(caminho, encoding="utf-8-sig", newline="") as f:
            colunas = next(csv.reader(f, delimiter=";"), None) or colunas
    with open(caminho, "a", encoding="utf-8-sig", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=colunas, delimiter=";", extrasaction="ignore")
        if novo:
            escritor.writeheader()
        escritor.writerows(linhas)
