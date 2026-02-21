import re

texto = "O CPF é 123.456.789asdacasc    -00, Telefone: (11) 98765-4321!"
# Substitui tudo que não é número (\D) por uma string vazia ('')
apenas_numeros = re.sub(r'\D', '', texto)
print(apenas_numeros)