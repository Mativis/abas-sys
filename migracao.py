import sqlite3

def adicionar_coluna_se_nao_existir(cursor, tabela, coluna, tipo):
    """Função auxiliar para adicionar uma coluna a uma tabela se ela não existir."""
    try:
        print(f"A verificar a coluna '{coluna}' na tabela '{tabela}'...")
        cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")
        print(f"SUCESSO: Coluna '{coluna}' adicionada.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print(f"INFO: A coluna '{coluna}' já existe. Nenhuma alteração necessária.")
        else:
            raise e

def migrar_base_de_dados():
    """
    Este script verifica e adiciona todas as colunas necessárias às tabelas existentes.
    É seguro executar este script várias vezes.
    """
    db_file = 'abastecimentos.db'
    print(f"A iniciar a migração da base de dados: {db_file}")

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Garante a criação da tabela de requisições se ela não existir
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS requisicoes_abastecimento (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_solicitacao DATE NOT NULL,
                solicitado_por_id INTEGER NOT NULL,
                placa TEXT NOT NULL,
                combustivel TEXT,
                quantidade_estimada REAL,
                status TEXT NOT NULL,
                abastecimento_id INTEGER,
                FOREIGN KEY (solicitado_por_id) REFERENCES users(id),
                FOREIGN KEY (abastecimento_id) REFERENCES abastecimentos(id)
            )
        ''')

        # Adiciona as novas colunas à tabela 'requisicoes_abastecimento'
        adicionar_coluna_se_nao_existir(cursor, 'requisicoes_abastecimento', 'motorista', 'TEXT')
        adicionar_coluna_se_nao_existir(cursor, 'requisicoes_abastecimento', 'centro_custo', 'TEXT')

        # Adiciona colunas à tabela 'pedidos_compra' para garantir consistência
        adicionar_coluna_se_nao_existir(cursor, 'pedidos_compra', 'orcamento_id', 'INTEGER DEFAULT 0')
        adicionar_coluna_se_nao_existir(cursor, 'pedidos_compra', 'aprovado_por_id', 'INTEGER DEFAULT 0')

        conn.commit()
        conn.close()
        
        print("\nMigração concluída com sucesso!")
        print("A sua base de dados foi atualizada e todos os seus dados foram preservados.")

    except Exception as e:
        print(f"\nOcorreu um erro durante a migração: {e}")

if __name__ == "__main__":
    migrar_base_de_dados()