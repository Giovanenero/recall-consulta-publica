"""
texto.py - dígitos, número CNJ e CPF/CNPJ (iguais em todos os robôs).

A normalização de nomes (normal, chave...) fica em cada robô: cada tribunal compara nomes de um jeito.
"""
import re


def so_digitos(texto):
    """Só os dígitos do texto ('' para None)."""
    return re.sub(r"\D", "", texto or "")


def formatar_cnj(numero):
    """20 dígitos -> NNNNNNN-DD.AAAA.J.TR.OOOO; outro tamanho volta como veio."""
    d = so_digitos(numero)
    if len(d) != 20:
        return numero
    return f"{d[:7]}-{d[7:9]}.{d[9:13]}.{d[13]}.{d[14:16]}.{d[16:20]}"


def documento_valido(documento):
    """CPF (11 dígitos) ou CNPJ (14) com dígito verificador certo. Documento mascarado (***) não passa."""
    d = so_digitos(documento)
    if len(d) == 11 and d != d[0] * 11:
        for n in (9, 10):
            if (sum(int(d[i]) * (n + 1 - i) for i in range(n)) * 10) % 11 % 10 != int(d[n]):
                return False
        return True
    if len(d) == 14 and d != d[0] * 14:
        for n, pesos in ((12, (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)), (13, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2))):
            resto = sum(int(d[i]) * pesos[i] for i in range(n)) % 11
            if (0 if resto < 2 else 11 - resto) != int(d[n]):
                return False
        return True
    return False
