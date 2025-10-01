import sqlite3
import pandas as pd

def migrar_para_multi_item():
    db_file = 'abastecimentos.db'
    print("Iniciando migração para múltiplos itens...")
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # -- PASSO 1: LIMPEZA DE TENTATIVAS ANTERIORES --
        print("Limpando tentativas anteriores...")
        cursor.execute("DROP TABLE IF EXISTS cotacoes_new")
        cursor.execute("DROP TABLE IF EXISTS cotacao_itens")
        cursor.execute("DROP TABLE IF EXISTS orcamentos_new")
        cursor.execute("DROP TABLE IF EXISTS pedidos_compra_new")
        cursor.execute("DROP TABLE IF EXISTS pedido_itens")

        # -- PASSO 2: RENOMEAR TABELAS ANTIGAS --
        print("Renomeando tabelas antigas...")
        cursor.execute("ALTER TABLE cotacoes RENAME TO cotacoes_old_single_item")
        cursor.execute("ALTER TABLE orcamentos RENAME TO orcamentos_old_single_item")
        cursor.execute("ALTER TABLE pedidos_compra RENAME TO pedidos_compra_old_single_item")

        # -- PASSO 3: CRIAR NOVAS TABELAS --
        print("Criando novas tabelas...")
        cursor.execute('''
            CREATE TABLE cotacoes (
                id INTEGER PRIMARY KEY, user_id INTEGER, titulo TEXT, data_limite TEXT,
                observacoes TEXT, status TEXT, data_aprovacao TEXT, data_registro TEXT
            )''')
        cursor.execute('''
            CREATE TABLE cotacao_itens (
                id INTEGER PRIMARY KEY, cotacao_id INTEGER, descricao TEXT, quantidade REAL
            )''')
        cursor.execute('''
            CREATE TABLE orcamentos (
                id INTEGER PRIMARY KEY, cotacao_id INTEGER, fornecedor_id INTEGER, valor REAL,
                prazo_pagamento TEXT, faturamento TEXT, data_registro TEXT, aprovado BOOLEAN
            )''')
        cursor.execute('''
            CREATE TABLE pedidos_compra (
                id INTEGER PRIMARY KEY, cotacao_id INTEGER, user_id INTEGER, fornecedor_id INTEGER,
                valor_total REAL, data_abertura TEXT, status TEXT, data_finalizacao TEXT,
                nf_e_chave TEXT, nfs_pdf_path TEXT, data_registro TEXT
            )''')
        cursor.execute('''
            CREATE TABLE pedido_itens (
                id INTEGER PRIMARY KEY, pedido_id INTEGER, descricao TEXT, quantidade REAL
            )''')
        
        # -- PASSO 4: MIGRAR DADOS --
        print("Migrando dados...")
        
        # Migrar Cotações e criar itens
        df_cotacoes_old = pd.read_sql("SELECT * FROM cotacoes_old_single_item", conn)
        for _, row in df_cotacoes_old.iterrows():
            cursor.execute(
                'INSERT INTO cotacoes (id, user_id, titulo, data_limite, observacoes, status, data_aprovacao, data_registro) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (row['id'], row['user_id'], row['item'], row['data_limite'], row['observacoes'], row['status'], row['data_aprovacao'], row['data_registro'])
            )
            cursor.execute(
                'INSERT INTO cotacao_itens (cotacao_id, descricao, quantidade) VALUES (?, ?, ?)',
                (row['id'], row['item'], row['quantidade'])
            )

        # Migrar Orçamentos
        df_orcamentos_old = pd.read_sql("SELECT * FROM orcamentos_old_single_item", conn)
        for _, row in df_orcamentos_old.iterrows():
             cursor.execute(
                'INSERT INTO orcamentos (id, cotacao_id, fornecedor_id, valor, prazo_pagamento, faturamento, data_registro, aprovado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (row['id'], row['cotacao_id'], row['fornecedor_id'], row['valor'], row['prazo_pagamento'], row['faturamento'], row['data_registro'], row['aprovado'])
            )

        # Migrar Pedidos de Compra e criar seus itens
        df_pedidos_old = pd.read_sql("SELECT * FROM pedidos_compra_old_single_item", conn)
        fornecedores_df = pd.read_sql("SELECT id, cnpj FROM fornecedores", conn).set_index('cnpj')
        
        for _, row in df_pedidos_old.iterrows():
            fornecedor_id = fornecedores_df.index.get_loc(row['fornecedor_cnpj']) if row['fornecedor_cnpj'] in fornecedores_df.index else None
            
            cursor.execute(
                'INSERT INTO pedidos_compra (id, cotacao_id, user_id, fornecedor_id, valor_total, data_abertura, status, data_finalizacao, nf_e_chave, nfs_pdf_path, data_registro) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (row['id'], row.get('orcamento_id'), row['user_id'], fornecedor_id, row['valor'], row['data_abertura'], row['status'], row['data_finalizacao'], row['nf_e_chave'], row['nfs_pdf_path'], row['data_registro'])
            )
            cursor.execute(
                'INSERT INTO pedido_itens (pedido_id, descricao, quantidade) VALUES (?, ?, ?)',
                (row['id'], row['item'], row['quantidade'])
            )

        # -- PASSO 5: LIMPAR TABELAS ANTIGAS --
        print("Removendo tabelas de backup...")
        cursor.execute("DROP TABLE cotacoes_old_single_item")
        cursor.execute("DROP TABLE orcamentos_old_single_item")
        cursor.execute("DROP TABLE pedidos_compra_old_single_item")

        conn.commit()
        print("\nMigração para múltiplos itens concluída com sucesso!")

    except Exception as e:
        print(f"\nOcorreu um erro durante a migração: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    migrar_para_multi_item()