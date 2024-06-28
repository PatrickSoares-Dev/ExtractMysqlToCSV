import mysql.connector
import csv

# Configurações de conexão com o banco de dados
config = {
    'user': '',
    'password': '',
    'host': '',
    'database': ''
}

# Conectando ao banco de dados
print("Iniciando o script...")

# Conectando ao banco de dados
try:
    print("Tentando conectar ao banco de dados...")
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("Conexão estabelecida com sucesso.")

    # Consulta SQL
    query = """
    SELECT TERMINALNUMBER, VERSIONID, RECIPIENTSP, RECIPIENTEOT, ACTIVATIONTIME, CREATED, ROUTE, CNL, LNP, VERSION, LINE, ''
        FROM ported_number
        WHERE ACTIVATIONTIME >= '2024-06-05 00:00:00' AND ACTIVATIONTIME < '2024-06-06 00:00:00';
    """
    print("Consulta SQL preparada.")

    # Executando a consulta
    print("Executando a consulta SQL...")
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"Consulta executada com sucesso. {len(results)} registros encontrados.")

    # Nome das colunas
    column_names = [i[0] for i in cursor.description]
    print("Nomes das colunas obtidos.")

    # Criando o arquivo CSV
    print("Criando o arquivo CSV...")
    with open('Output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        
        # Escrevendo o cabeçalho
        csvwriter.writerow(column_names)
        print("Cabeçalho escrito no arquivo CSV.")
        
        # Escrevendo os dados
        csvwriter.writerows(results)
        print("Dados escritos no arquivo CSV.")

    print("Dados exportados com sucesso para 'Output.csv'.")

except mysql.connector.Error as err:
    print(f"Erro: {err}")

finally:
    # Fechando a conexão
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")