import sqlite3
import pandas as pd

def migrar_dados_seguro():
    """
    Executa uma migração de banco de dados robusta, que pode ser executada novamente em caso de falha.
    """
    db_file = 'abastecimentos.db'
    
    print("Iniciando a migração segura do banco de dados...")
    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print("Passo 0: Limpando tentativas de migração anteriores (se houver)...")
        # Remove tabelas que podem ter sido criadas em uma tentativa anterior que falhou
        cursor.execute("DROP TABLE IF EXISTS orcamentos")
        cursor.execute("DROP TABLE IF EXISTS cotacoes")
        cursor.execute("DROP TABLE IF EXISTS pedidos_compra")
        # Renomeia as tabelas antigas de volta se o script falhou após renomear
        try:
            cursor.execute("ALTER TABLE cotacoes_old RENAME TO cotacoes")
        except sqlite3.OperationalError:
            pass # Ignora o erro se cotacoes_old não existir
        try:
            cursor.execute("ALTER TABLE pedidos_compra_old RENAME TO pedidos_compra")
        except sqlite3.OperationalError:
            pass # Ignora o erro se pedidos_compra_old não existir

        print("Passo 1: Renomeando tabelas antigas para backup...")
        cursor.execute("ALTER TABLE cotacoes RENAME TO cotacoes_old")
        cursor.execute("ALTER TABLE pedidos_compra RENAME TO pedidos_compra_old")
        
        print("Passo 2: Criando novas tabelas com a estrutura correta...")
        # Cria a nova tabela de cotações (simplificada)
        cursor.execute('''
        CREATE TABLE cotacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, item TEXT NOT NULL,
            quantidade REAL NOT NULL, data_limite TEXT NOT NULL, observacoes TEXT,
            status TEXT DEFAULT 'Aberta', data_aprovacao TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
        ''')
        # Cria a nova tabela de orçamentos
        cursor.execute('''
        CREATE TABLE orcamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT, cotacao_id INTEGER NOT NULL, fornecedor_id INTEGER NOT NULL,
            valor REAL NOT NULL, prazo_pagamento TEXT, faturamento TEXT,
            data_registro TEXT DEFAULT CURRENT_TIMESTAMP, aprovado BOOLEAN DEFAULT 0,
            FOREIGN KEY (cotacao_id) REFERENCES cotacoes (id),
            FOREIGN KEY (fornecedor_id) REFERENCES fornecedores (id)
        )
        ''')
        # Cria a nova tabela de pedidos de compra com a coluna 'orcamento_id'
        cursor.execute('''
        CREATE TABLE pedidos_compra (
            id INTEGER PRIMARY KEY AUTOINCREMENT, orcamento_id INTEGER, user_id INTEGER NOT NULL,
            item TEXT NOT NULL, quantidade REAL NOT NULL, valor REAL NOT NULL, fornecedor_cnpj TEXT,
            status TEXT DEFAULT 'Aberto', nf_e_chave TEXT, nfs_pdf_path TEXT, data_abertura TEXT NOT NULL,
            data_finalizacao TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (orcamento_id) REFERENCES orcamentos (id)
        )
        ''')

        print("Passo 3: Migrando dados das cotações antigas...")
        df_cotacoes_old = pd.read_sql_query("SELECT * FROM cotacoes_old", conn)
        for _, row in df_cotacoes_old.iterrows():
            cursor.execute(
                'INSERT INTO cotacoes (id, user_id, item, quantidade, data_limite, observacoes, status, data_aprovacao, data_registro) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['id'], row['user_id'], row['item'], row['quantidade'], row['data_limite'], row['observacoes'], row['status'], row['data_aprovacao'], row['data_registro'])
            )
            if pd.notna(row['fornecedor_id']) and pd.notna(row['valor_fechado']):
                aprovado_status = 1 if row['status'] == 'Aprovada' else 0
                cursor.execute(
                    'INSERT INTO orcamentos (cotacao_id, fornecedor_id, valor, prazo_pagamento, faturamento, aprovado) VALUES (?, ?, ?, ?, ?, ?)',
                    (row['id'], row['fornecedor_id'], row['valor_fechado'], row['prazo_pagamento'], row['faturamento'], aprovado_status)
                )

        print("Passo 4: Migrando dados dos pedidos de compra antigos...")
        df_pedidos_old = pd.read_sql_query("SELECT * FROM pedidos_compra_old", conn)
        for _, row in df_pedidos_old.iterrows():
            orcamento_id_novo = None
            if pd.notna(row.get('quote_id')):
                cursor.execute("SELECT id FROM orcamentos WHERE cotacao_id = ?", (row['quote_id'],))
                res = cursor.fetchone()
                if res:
                    orcamento_id_novo = res[0]
            
            cursor.execute(
                'INSERT INTO pedidos_compra (id, orcamento_id, user_id, item, quantidade, valor, fornecedor_cnpj, status, nf_e_chave, nfs_pdf_path, data_abertura, data_finalizacao, data_registro) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['id'], orcamento_id_novo, row['user_id'], row['item'], row['quantidade'], row['valor'], row['fornecedor_cnpj'], row['status'], row['nf_e_chave'], row['nfs_pdf_path'], row['data_abertura'], row['data_finalizacao'], row['data_registro'])
            )

        print("Passo 5: Removendo tabelas de backup...")
        cursor.execute("DROP TABLE cotacoes_old")
        cursor.execute("DROP TABLE pedidos_compra_old")
        
        conn.commit()
        print("\nMigração concluída com sucesso! Seus dados foram preservados e a estrutura do banco de dados foi atualizada.")

    except Exception as e:
        print(f"\nOcorreu um erro durante a migração: {e}")
        print("A operação foi revertida (rollback).")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # Lembre-se: Faça um backup do seu arquivo 'abastecimentos.db' antes de rodar!
    migrar_dados_seguro()