"""
reabrir_sociedade_advogados.py - devolve à fila os créditos do TJMA que o fetch_TJMA.py gravou como FALHA
'SEM_CPF_CREDOR: credor é sociedade de advogados (honorários)', para o robô gravar a regra de honorários nova.

Desde 09/10/2026 o escritório de advocacia não é mais FALHA (equivalia a descartar): vira SUCESSO_INCOMPLETO
'SUCESSO_SEM_CPF: regra=HONORARIOS_ADVOGADO beneficiario=<sociedade> ...' (como no TJPI/TJDF/TJMT), com o nome em
metadata.honorarios e a sociedade como ADVOGADO quando vier com CNPJ. Os FALHA antigos só mudam passando pelo robô.

Mesmo funcionamento do reabrir_nao_encontrado.py: só os sem credor ligado, ainda na lista e sem lease; status
PENDENTE, disponivel_em = agora e o prefixo 'REABERTO_SOCIEDADE_ADV: ' no motivo; desfazer em
TJMA/saida/desfazer_reabrir_sociedade_advogados_<data>.sql.
Os que vieram da rota das iniciais ('rota das iniciais' no motivo) voltam pela fila normal como SEM_BENEFICIARIO
(sem a marca) e o próximo --iniciais os refaz.

Uso:
    python TJMA/reabrir_sociedade_advogados.py              # só mostra o que faria
    python TJMA/reabrir_sociedade_advogados.py --aplicar    # grava (COMMIT) e escreve o SQL de desfazer
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from reabrir_nao_encontrado import reabrir  # noqa: E402

if __name__ == "__main__":
    reabrir(prefixo="REABERTO_SOCIEDADE_ADV: ", motivo_like="SEM\\_CPF\\_CREDOR: credor é sociedade de advogados%",
            descricao="FALHA SEM_CPF_CREDOR de sociedade de advogados", nome="reabrir_sociedade_advogados")
