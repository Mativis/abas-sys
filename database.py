import sqlite3
import pandas as pd
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import os
import io

# Constantes de Negócio
LIMITE_KM_TROCA = 10000
LIMITE_HORIMETRO_TROCA = 350
ATENCAO_KM = 1000
ATENCAO_HORAS = 50

def get_db_connection():
    """Retorna uma conexão com o banco de dados com row_factory ativado."""
    conn = sqlite3.connect('abastecimentos.db')
    conn.row_factory = sqlite3.Row
    return conn

# A função criar_tabelas é usada apenas para novas instalações.
# A migração de um banco existente deve ser feita com o script migracao_multi_item.py
def criar_tabelas():
    """Cria o esquema de banco de dados para uma nova instalação."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # --- Tabelas Principais ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('Administrador', 'Gestor', 'Comprador', 'Padrão'))
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fornecedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cnpj TEXT NOT NULL UNIQUE, nome TEXT NOT NULL, ie TEXT,
        endereco TEXT, tipo TEXT, contato TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )''')

    # --- Tabelas do Módulo Dealers (Estrutura Multi-Item) ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cotacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, titulo TEXT NOT NULL, data_limite TEXT NOT NULL,
        observacoes TEXT, status TEXT DEFAULT 'Aberta', data_aprovacao TEXT, data_registro TEXT,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cotacao_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cotacao_id INTEGER NOT NULL, descricao TEXT NOT NULL, quantidade REAL NOT NULL,
        FOREIGN KEY (cotacao_id) REFERENCES cotacoes (id)
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orcamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cotacao_id INTEGER NOT NULL, fornecedor_id INTEGER NOT NULL,
        valor REAL NOT NULL, prazo_pagamento TEXT, faturamento TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
        aprovado BOOLEAN DEFAULT 0,
        FOREIGN KEY (cotacao_id) REFERENCES cotacoes (id), FOREIGN KEY (fornecedor_id) REFERENCES fornecedores (id)
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedidos_compra (
        id INTEGER PRIMARY KEY AUTOINCREMENT, cotacao_id INTEGER, user_id INTEGER NOT NULL, fornecedor_id INTEGER NOT NULL,
        valor_total REAL NOT NULL, data_abertura TEXT, status TEXT DEFAULT 'Aberto', data_finalizacao TEXT,
        nf_e_chave TEXT, nfs_pdf_path TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (cotacao_id) REFERENCES cotacoes (id), FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (fornecedor_id) REFERENCES fornecedores (id)
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedido_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT, pedido_id INTEGER NOT NULL, descricao TEXT NOT NULL, quantidade REAL NOT NULL,
        FOREIGN KEY (pedido_id) REFERENCES pedidos_compra (id)
    )''')

    # --- Tabelas de Frotas (sem alterações) ---
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS abastecimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL, placa TEXT NOT NULL, responsavel TEXT,
        litros REAL NOT NULL, desconto REAL DEFAULT 0, odometro REAL, centro_custo TEXT, combustivel TEXT,
        custo_por_litro REAL NOT NULL, custo_bruto REAL NOT NULL, custo_liquido REAL NOT NULL,
        km_rodados REAL, km_litro REAL, posto TEXT, integracao_atheris BOOLEAN DEFAULT 0,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedagios (
        id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL, placa TEXT NOT NULL, valor REAL NOT NULL,
        observacoes TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS precos_combustivel (
        id INTEGER PRIMARY KEY AUTOINCREMENT, combustivel TEXT NOT NULL UNIQUE, preco REAL NOT NULL,
        data_atualizacao TEXT NOT NULL
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trocas_oleo (
        id INTEGER PRIMARY KEY AUTOINCREMENT, identificacao TEXT NOT NULL,
        tipo TEXT CHECK(tipo IN ('veiculo', 'maquina')) NOT NULL, data_troca TEXT NOT NULL,
        km_troca REAL, horimetro_troca REAL, proxima_troca_km REAL, proxima_troca_horimetro REAL,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(identificacao, tipo)
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS checklists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, identificacao TEXT NOT NULL, data TEXT NOT NULL,
        horimetro REAL, nivel_oleo TEXT CHECK(nivel_oleo IN ('ADEQUADO', 'BAIXO', 'CRÍTICO')) DEFAULT 'ADEQUADO',
        observacoes TEXT, itens_checklist TEXT, data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS manutencoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, identificacao TEXT NOT NULL, tipo TEXT NOT NULL,
        frota TEXT NOT NULL, descricao TEXT NOT NULL, fornecedor TEXT, valor REAL DEFAULT 0,
        data_abertura TEXT NOT NULL, previsao_conclusao TEXT, data_conclusao TEXT,
        observacoes TEXT, finalizada BOOLEAN DEFAULT 0, prazo_liberacao INTEGER,
        forma_pagamento TEXT, parcelas INTEGER DEFAULT 1, data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    
    conn.commit()
    conn.close()

# --- Funções de Cotação (REESTRUTURADAS) ---

def criar_cotacao_com_itens(user_id, dados):
    """Cria uma cotação (cabeçalho) e seus itens."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. Cria o cabeçalho da cotação, agora incluindo o STATUS
        cursor.execute('''
            INSERT INTO cotacoes (user_id, titulo, data_limite, observacoes, status, data_registro)
            VALUES (?, ?, ?, ?, 'Aberta', ?)
        ''', (user_id, dados['titulo'], dados['data_limite'], dados.get('observacoes', ''), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        cotacao_id = cursor.lastrowid

        # 2. Insere os itens da cotação
        itens_para_inserir = []
        for item in dados['itens']:
            itens_para_inserir.append((
                cotacao_id, item['descricao'], float(item['quantidade'])
            ))
        
        cursor.executemany('''
            INSERT INTO cotacao_itens (cotacao_id, descricao, quantidade)
            VALUES (?, ?, ?)
        ''', itens_para_inserir)
        
        conn.commit()
        return cotacao_id
    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar cotação com itens: {e}")
        return None
    finally:
        conn.close()

def obter_cotacoes():
    """Obtém o relatório de cotações."""
    conn = get_db_connection()
    query = '''
        SELECT
            c.id, c.titulo, c.data_limite, c.status,
            u.username as solicitante,
            (SELECT COUNT(*) FROM cotacao_itens ci WHERE ci.cotacao_id = c.id) as total_itens,
            (SELECT SUM(o.valor) FROM orcamentos o WHERE o.cotacao_id = c.id AND o.aprovado = 1) as valor_aprovado
        FROM cotacoes c
        JOIN users u ON c.user_id = u.id
        ORDER BY c.data_registro DESC
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def obter_cotacao_por_id(cotacao_id):
    """Busca um cabeçalho de cotação específico pelo seu ID."""
    conn = get_db_connection()
    try:
        query = "SELECT c.*, u.username as solicitante FROM cotacoes c JOIN users u ON c.user_id = u.id WHERE c.id = ?"
        df = pd.read_sql_query(query, conn, params=(cotacao_id,))
        return df.iloc[0].to_dict() if not df.empty else None
    finally:
        conn.close()

def obter_itens_por_cotacao_id(cotacao_id):
    """Busca todos os itens de uma cotação."""
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM cotacao_itens WHERE cotacao_id = ?", conn, params=(cotacao_id,))
    conn.close()
    return df.to_dict('records')

# --- Funções de Orçamento (REESTRUTURADAS) ---

def adicionar_orcamento(dados):
    """Adiciona uma nova proposta de orçamento a uma cotação."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            'INSERT INTO orcamentos (cotacao_id, fornecedor_id, valor, prazo_pagamento, faturamento) VALUES (?, ?, ?, ?, ?)',
            (dados['cotacao_id'], dados['fornecedor_id'], float(dados['valor']), dados['prazo_pagamento'], dados['faturamento'])
        )
        cursor.execute("UPDATE cotacoes SET status = 'Fechada' WHERE id = ? AND status = 'Aberta'", (dados['cotacao_id'],))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()

def aprovar_orcamento(orcamento_id, user_id):
    """
    Aprova um orçamento, atualiza status e cria um Pedido de Compra.
    Garante que apenas um orçamento seja marcado como 'aprovado' por cotação.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Pega os dados do orçamento que será aprovado
        orcamento = pd.read_sql('SELECT * FROM orcamentos WHERE id = ?', conn, params=(orcamento_id,)).iloc[0].to_dict()
        cotacao_id = orcamento['cotacao_id']
        
        # --- LÓGICA DE CORREÇÃO ---
        # 1. Primeiro, redefine o status de TODOS os orçamentos para esta cotação como NÃO aprovado.
        #    Isso previne que múltiplos orçamentos fiquem com o status de aprovado.
        cursor.execute("UPDATE orcamentos SET aprovado = 0 WHERE cotacao_id = ?", (cotacao_id,))
        
        # 2. Em seguida, marca APENAS o orçamento selecionado como APROVADO.
        cursor.execute("UPDATE orcamentos SET aprovado = 1 WHERE id = ?", (orcamento_id,))
        # --- FIM DA CORREÇÃO ---

        # 3. Atualiza o status da cotação-mãe para 'Aprovada'
        cursor.execute("UPDATE cotacoes SET status = 'Aprovada', data_aprovacao = ? WHERE id = ?", (datetime.now().strftime('%Y-%m-%d'), cotacao_id))

        # 4. Cria o Pedido de Compra
        cursor.execute('''
            INSERT INTO pedidos_compra (cotacao_id, user_id, fornecedor_id, valor_total, data_abertura, status)
            VALUES (?, ?, ?, ?, ?, 'Aberto')
        ''', (cotacao_id, user_id, orcamento['fornecedor_id'], orcamento['valor'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        pedido_id = cursor.lastrowid

        # 5. Copia os itens da cotação para o pedido
        itens_cotacao = obter_itens_por_cotacao_id(cotacao_id)
        itens_pedido = [(pedido_id, item['descricao'], item['quantidade']) for item in itens_cotacao]
        cursor.executemany('INSERT INTO pedido_itens (pedido_id, descricao, quantidade) VALUES (?, ?, ?)', itens_pedido)
        
        conn.commit()
        return pedido_id
    except Exception as e:
        conn.rollback()
        print(f"Erro ao aprovar orçamento: {e}")
        return None
    finally:
        conn.close()
    
def obter_orcamentos_por_cotacao_id(cotacao_id):
    """Busca todos os orçamentos de uma cotação específica."""
    conn = get_db_connection()
    query = '''
        SELECT o.*, f.nome as fornecedor_nome
        FROM orcamentos o
        JOIN fornecedores f ON o.fornecedor_id = f.id
        WHERE o.cotacao_id = ?
        ORDER BY o.valor ASC
    '''
    df = pd.read_sql(query, conn, params=(cotacao_id,))
    conn.close()
    return df.to_dict('records')

# --- Funções de Pedido de Compra (CORRIGIDAS) ---

def obter_pedidos_compra():
    conn = get_db_connection()
    # CORREÇÃO: JOIN com fornecedores via p.fornecedor_id
    query = '''
        SELECT
            p.id, p.cotacao_id, p.status, p.data_abertura, p.valor_total,
            f.nome as fornecedor_nome,
            (SELECT COUNT(*) FROM pedido_itens pi WHERE pi.pedido_id = p.id) as total_itens
        FROM pedidos_compra p
        JOIN fornecedores f ON p.fornecedor_id = f.id
        ORDER BY p.data_abertura DESC
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def obter_pedido_compra_por_id(pedido_id):
    conn = get_db_connection()
    # CORREÇÃO: JOIN com fornecedores via p.fornecedor_id
    query = '''
        SELECT p.*, f.nome as fornecedor_nome, f.cnpj as fornecedor_cnpj
        FROM pedidos_compra p
        JOIN fornecedores f ON p.fornecedor_id = f.id
        WHERE p.id = ?
    '''
    df = pd.read_sql(query, conn, params=(pedido_id,))
    return df.iloc[0].to_dict() if not df.empty else None

def obter_itens_por_pedido_id(pedido_id):
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM pedido_itens WHERE pedido_id = ?", conn, params=(pedido_id,))
    conn.close()
    return df.to_dict('records')

def finalizar_pedido_compra(pedido_id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE pedidos_compra
            SET status = 'Finalizado', data_finalizacao = ?, nf_e_chave = ?, nfs_pdf_path = ?
            WHERE id = ? AND status != 'Finalizado'
        ''', (datetime.now().strftime('%Y-%m-%d'), dados.get('nf_e_chave'), dados.get('nfs_pdf_path'), pedido_id))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

# --- (O resto das funções permanecem as mesmas) ---
def obter_dealer_intelligence(data_inicio, data_fim):
    conn = get_db_connection()
    query_orcamentos = '''
        SELECT c.id as cotacao_id, o.valor
        FROM orcamentos o
        JOIN cotacoes c ON o.cotacao_id = c.id
        WHERE c.data_registro BETWEEN ? AND ?
    '''
    df_orcamentos = pd.read_sql(query_orcamentos, conn, params=(data_inicio, data_fim))
    query_pedidos = '''
        SELECT p.valor_total, p.data_abertura, p.data_finalizacao, o.cotacao_id
        FROM pedidos_compra p
        JOIN orcamentos o ON p.cotacao_id = o.cotacao_id AND o.aprovado = 1
        WHERE p.status = 'Finalizado' AND p.data_abertura BETWEEN ? AND ?
    '''
    df_pedidos = pd.read_sql(query_pedidos, conn, params=(data_inicio, data_fim))
    conn.close()
    total_fechado = df_pedidos['valor_total'].sum() if not df_pedidos.empty else 0
    total_orcado = 0
    valor_poupado = 0
    descontos_perdidos = 0
    if not df_orcamentos.empty:
        total_orcado = df_orcamentos.groupby('cotacao_id')['valor'].min().sum()
        for cotacao_id, group in df_orcamentos.groupby('cotacao_id'):
            if cotacao_id in df_pedidos['cotacao_id'].values:
                menor_valor = group['valor'].min()
                valor_aprovado = df_pedidos[df_pedidos['cotacao_id'] == cotacao_id]['valor_total'].iloc[0]
                if valor_aprovado < menor_valor:
                     valor_poupado += menor_valor - valor_aprovado
                elif valor_aprovado > menor_valor:
                    descontos_perdidos += valor_aprovado - menor_valor
    media_dias_processamento = 0
    relatorio_processamento = []
    if not df_pedidos.empty and 'data_finalizacao' in df_pedidos.columns and pd.notna(df_pedidos['data_finalizacao']).all():
        df_pedidos['data_abertura'] = pd.to_datetime(df_pedidos['data_abertura'])
        df_pedidos['data_finalizacao'] = pd.to_datetime(df_pedidos['data_finalizacao'])
        df_pedidos['dias_processamento'] = (df_pedidos['data_finalizacao'] - df_pedidos['data_abertura']).dt.days
        media_dias_processamento = df_pedidos['dias_processamento'].mean()
        relatorio_processamento = df_pedidos[['data_abertura', 'data_finalizacao', 'dias_processamento']].to_dict('records')
    return {
        'total_fechado': total_fechado,
        'total_orcado': total_orcado,
        'valor_poupado': valor_poupado,
        'descontos_perdidos': descontos_perdidos,
        'media_dias_processamento': media_dias_processamento if pd.notna(media_dias_processamento) else 0,
        'relatorio_processamento': relatorio_processamento
    }

def get_user_by_username(username):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
    conn.close()
    return user

def get_user_by_id(user_id):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
    conn.close()
    return user

def get_all_users():
    conn = get_db_connection()
    df = pd.read_sql("SELECT id, username, role FROM users ORDER BY role, username", conn)
    conn.close()
    return df.to_dict('records')

def create_user(username, password, role):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        password_hash = generate_password_hash(password)
        if role not in ['Administrador', 'Gestor', 'Comprador', 'Padrão']:
            raise ValueError("Role inválida.")
        cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)", (username, password_hash, role))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except ValueError:
        return False
    finally:
        conn.close()

def update_user(user_id, username, role, password=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if role not in ['Administrador', 'Gestor', 'Comprador', 'Padrão']:
            raise ValueError("Role inválida.")
        
        if password and len(password) >= 3:
            password_hash = generate_password_hash(password)
            cursor.execute("UPDATE users SET username = ?, role = ?, password_hash = ? WHERE id = ?", (username, role, password_hash, user_id))
        else:
            cursor.execute("UPDATE users SET username = ?, role = ? WHERE id = ?", (username, role, user_id))
            
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.IntegrityError:
        return False
    except ValueError:
        return False
    finally:
        conn.close()

def delete_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

def criar_fornecedor(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO fornecedores (cnpj, nome, ie, endereco, tipo, contato)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (dados['cnpj'], dados['nome'], dados['ie'], dados['endereco'], dados['tipo'], dados['contato']))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def obter_fornecedores():
    conn = get_db_connection()
    df = pd.read_sql('SELECT id, cnpj, nome, ie, tipo, contato, data_registro FROM fornecedores ORDER BY nome', conn)
    conn.close()
    return df.to_dict('records')

def obter_precos_combustivel(): 
    conn = get_db_connection()
    query = "SELECT combustivel, preco, data_atualizacao FROM precos_combustivel ORDER BY combustivel"
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def atualizar_preco_combustivel(combustivel, novo_preco):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
        UPDATE precos_combustivel 
        SET preco = ?, data_atualizacao = ?
        WHERE combustivel = ?
        ''', (round(float(novo_preco), 3), datetime.now().strftime('%Y-%m-%d'), combustivel))
        conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao atualizar preço: {e}")
        return False
    finally:
        conn.close()

def criar_combustivel(combustivel, preco):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
        INSERT INTO precos_combustivel (combustivel, preco, data_atualizacao)
        VALUES (?, ?, ?)
        ''', (combustivel.upper(), round(float(preco), 3), datetime.now().strftime('%Y-%m-%d')))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"Erro ao criar combustível: {e}")
        return False
    finally:
        conn.close()

def obter_relatorio(data_inicio, data_fim, placa=None, centro_custo=None, combustivel=None, posto=None): 
    conn = get_db_connection()
    query = """
    SELECT 
        id, data, placa, responsavel, litros, desconto, odometro,
        centro_custo, combustivel, custo_por_litro, custo_bruto, 
        custo_liquido, km_litro, posto, integracao_atheris
    FROM abastecimentos
    WHERE data BETWEEN ? AND ?
    """
    params = [data_inicio, data_fim]
    conditions = []
    if placa:
        conditions.append("placa = ?")
        params.append(placa.upper())
    if centro_custo:
        conditions.append("centro_custo = ?")
        params.append(centro_custo)
    if combustivel:
        conditions.append("combustivel = ?")
        params.append(combustivel)
    if posto:
        conditions.append("posto = ?")
        params.append(posto)
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    query += " ORDER BY data DESC"
    
    try:
        df = pd.read_sql(query, conn, params=params)
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()
    return df

def obter_opcoes_filtro(coluna):
    conn = get_db_connection()
    query = f"SELECT DISTINCT {coluna} FROM abastecimentos WHERE {coluna} IS NOT NULL AND {coluna} != '' ORDER BY {coluna}"
    try:
        resultados = pd.read_sql(query, conn)
        return resultados[coluna].tolist()
    except Exception as e:
        print(f"Erro ao obter opções de filtro para {coluna}: {e}")
        return []
    finally:
        conn.close()

def obter_placas_veiculos():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT placa FROM abastecimentos WHERE placa IS NOT NULL ORDER BY placa")
        placas = [row[0] for row in cursor.fetchall()]
        return placas
    except Exception as e:
        print(f"Erro ao obter placas: {e}")
        return []
    finally:
        conn.close()

def calcular_medias_veiculos():
    conn = get_db_connection()
    query_calculo_km = """
    WITH abastecimentos_ordenados AS (
        SELECT 
            id, placa, data, odometro, litros,
            LAG(odometro) OVER (PARTITION BY placa ORDER BY data) as odometro_anterior
        FROM abastecimentos
        WHERE odometro IS NOT NULL
    ),
    abastecimentos_com_km AS (
        SELECT 
            id, odometro, odometro_anterior, litros,
            CASE WHEN odometro_anterior IS NOT NULL AND odometro > odometro_anterior 
                THEN odometro - odometro_anterior ELSE NULL END as km_rodados,
            CASE WHEN odometro_anterior IS NOT NULL AND odometro > odometro_anterior AND litros > 0
                THEN (odometro - odometro_anterior) / litros ELSE NULL END as km_litro_calculado
        FROM abastecimentos_ordenados
    )
    UPDATE abastecimentos
    SET km_rodados = (SELECT km_rodados FROM abastecimentos_com_km WHERE abastecimentos_com_km.id = abastecimentos.id),
    km_litro = (SELECT km_litro_calculado FROM abastecimentos_com_km WHERE abastecimentos_com_km.id = abastecimentos.id)
    WHERE EXISTS (SELECT 1 FROM abastecimentos_com_km WHERE abastecimentos_com_km.id = abastecimentos.id)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query_calculo_km)
        conn.commit()
    except Exception as e:
        print(f"Erro ao calcular km/litro: {e}")
        conn.rollback()
    
    query_medias = """
    SELECT 
        placa, COUNT(*) as total_abastecimentos, AVG(litros) as media_litros,
        AVG(km_litro) as media_kml, SUM(custo_bruto) as total_gasto,
        MAX(odometro) as km_atual, SUM(litros) as total_litros
    FROM abastecimentos
    WHERE km_litro IS NOT NULL
    GROUP BY placa ORDER BY media_kml DESC
    """
    try:
        df = pd.read_sql(query_medias, conn)
        return df.to_dict('records')
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def obter_registro_por_id(id):
    conn = get_db_connection()
    query = "SELECT * FROM abastecimentos WHERE id = ?"
    try:
        df = pd.read_sql(query, conn, params=(id,))
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def atualizar_registro(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    km_litro = None
    if dados.get('odometro'):
        cursor.execute("SELECT MAX(odometro) FROM abastecimentos WHERE placa = ? AND odometro IS NOT NULL AND id != ?", (dados['placa'].upper(), id))
        ultimo_odometro = cursor.fetchone()[0]
        if ultimo_odometro and dados['odometro'] > ultimo_odometro and dados['litros'] > 0:
            km_rodados = dados['odometro'] - ultimo_odometro
            km_litro = km_rodados / dados['litros']
    query = """
    UPDATE abastecimentos SET
        data = ?, placa = ?, responsavel = ?, litros = ?, desconto = ?, odometro = ?, centro_custo = ?,
        combustivel = ?, custo_por_litro = ?, custo_bruto = ?, custo_liquido = ?, posto = ?, km_litro = ?
    WHERE id = ?
    """
    try:
        cursor.execute(query, (
            dados['data'], dados['placa'].upper(), dados['responsavel'], round(float(dados['litros']), 3),
            round(float(dados['desconto']), 2), round(float(dados['odometro']), 1) if dados['odometro'] else None,
            dados['centro_custo'], dados['combustivel'], round(float(dados['custo_por_litro']), 3),
            round(float(dados['custo_bruto']), 2), round(float(dados['custo_liquido']), 2),
            dados.get('posto', ''), km_litro, id
        ))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def criar_registro(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    km_litro = None
    if dados.get('odometro'):
        cursor.execute("SELECT MAX(odometro) FROM abastecimentos WHERE placa = ? AND odometro IS NOT NULL", (dados['placa'].upper(),))
        ultimo_odometro = cursor.fetchone()[0]
        if ultimo_odometro and dados['odometro'] > ultimo_odometro and dados['litros'] > 0:
            km_rodados = dados['odometro'] - ultimo_odometro
            km_litro = km_rodados / dados['litros']
    query = """
    INSERT INTO abastecimentos (data, placa, responsavel, litros, desconto, odometro, centro_custo, combustivel, custo_por_litro, custo_bruto, custo_liquido, posto, km_litro)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor.execute(query, (
            dados['data'], dados['placa'].upper(), dados['responsavel'], round(float(dados['litros']), 3),
            round(float(dados['desconto']), 2), round(float(dados['odometro']), 1) if dados['odometro'] else None,
            dados['centro_custo'], dados['combustivel'], round(float(dados['custo_por_litro']), 3),
            round(float(dados['custo_bruto']), 2), round(float(dados['custo_liquido']), 2),
            dados.get('posto', ''), km_litro
        ))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def excluir_registro(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM abastecimentos WHERE id = ?", (id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def criar_pedagio(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "INSERT INTO pedagios (data, placa, valor, observacoes) VALUES (?, ?, ?, ?)"
    try:
        cursor.execute(query, (dados['data'], dados['placa'], round(float(dados['valor']), 2), dados.get('observacoes', '')))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"Erro ao criar pedágio: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def obter_pedagios_com_filtros(data_inicio, data_fim, placa=None):
    conn = get_db_connection()
    query = "SELECT * FROM pedagios WHERE data BETWEEN ? AND ?"
    params = [data_inicio, data_fim]
    if placa:
        query += " AND placa = ?"
        params.append(placa.upper())
    query += " ORDER BY data DESC, placa ASC"
    try:
        df = pd.read_sql(query, conn, params=params)
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter pedágios com filtros: {e}")
        return []
    finally:
        conn.close()

def obter_pedagio_por_id(id):
    conn = get_db_connection()
    query = "SELECT * FROM pedagios WHERE id = ?"
    try:
        df = pd.read_sql(query, conn, params=(id,))
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Erro ao obter pedágio por ID: {e}")
        return None
    finally:
        conn.close()

def atualizar_pedagio(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "UPDATE pedagios SET data = ?, placa = ?, valor = ?, observacoes = ? WHERE id = ?"
    try:
        cursor.execute(query, (dados['data'], dados['placa'], round(float(dados['valor']), 2), dados.get('observacoes', ''), id))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao atualizar pedágio: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def excluir_pedagio(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM pedagios WHERE id = ?", (id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao excluir pedágio: {e}")
        return False
    finally:
        conn.close()

def salvar_troca_oleo(identificacao, tipo, data_troca, km_troca=None, horimetro_troca=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if tipo == 'veiculo':
            if km_troca is None: raise ValueError("KM da troca é obrigatório para veículos")
            proxima_troca_km = km_troca + LIMITE_KM_TROCA
            proxima_troca_horimetro = None
        else:
            if horimetro_troca is None: raise ValueError("Horímetro da troca é obrigatório para máquinas")
            proxima_troca_km = None
            proxima_troca_horimetro = horimetro_troca + LIMITE_HORIMETRO_TROCA
        
        cursor.execute('SELECT id FROM trocas_oleo WHERE identificacao = ? AND tipo = ?', (identificacao, tipo))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE trocas_oleo 
                SET data_troca = ?, km_troca = ?, horimetro_troca = ?, 
                    proxima_troca_km = ?, proxima_troca_horimetro = ?
                WHERE identificacao = ? AND tipo = ?
            ''', (data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro, identificacao, tipo))
        else:
            cursor.execute('''
                INSERT INTO trocas_oleo (identificacao, tipo, data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (identificacao, tipo, data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao salvar troca de óleo: {e}")
        return False
    finally:
        conn.close()

def obter_troca_oleo_por_identificacao_tipo(identificacao, tipo):
    conn = get_db_connection()
    try:
        query = "SELECT identificacao, tipo, data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro FROM trocas_oleo WHERE identificacao = ? AND tipo = ?"
        df = pd.read_sql(query, conn, params=(identificacao, tipo))
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Erro ao obter troca de óleo: {e}")
        return None
    finally:
        conn.close()

def obter_trocas_oleo():
    conn = get_db_connection()
    try:
        query = "SELECT identificacao, tipo, data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro FROM trocas_oleo ORDER BY tipo, identificacao"
        df_trocas = pd.read_sql(query, conn)
        trocas = []
        for _, troca in df_trocas.iterrows():
            identificacao = troca['identificacao']
            tipo = troca['tipo']
            km_troca = troca['km_troca']
            horimetro_troca = troca['horimetro_troca']
            proxima_troca_km = troca['proxima_troca_km']
            proxima_troca_horimetro = troca['proxima_troca_horimetro']
            
            valor_atual = None
            proxima_troca = None
            remanescente = None
            
            cursor = conn.cursor()
            if tipo == 'veiculo':
                cursor.execute("SELECT MAX(odometro) FROM abastecimentos WHERE placa = ? AND odometro IS NOT NULL", (identificacao,))
                valor_atual = cursor.fetchone()[0]
                proxima_troca = proxima_troca_km
                if valor_atual and proxima_troca: remanescente = proxima_troca - valor_atual
                elif km_troca and proxima_troca: remanescente = proxima_troca - km_troca
                if remanescente is not None:
                    if remanescente <= 0: status = 'VENCIDO'
                    elif remanescente <= ATENCAO_KM: status = 'ATENÇÃO'
                    else: status = 'OK'
                else: status = 'N/A'
            else:
                cursor.execute("SELECT MAX(horimetro) FROM checklists WHERE identificacao = ? AND horimetro IS NOT NULL", (identificacao,))
                valor_atual = cursor.fetchone()[0]
                proxima_troca = proxima_troca_horimetro
                if valor_atual and proxima_troca: remanescente = proxima_troca - valor_atual
                elif horimetro_troca and proxima_troca: remanescente = proxima_troca - horimetro_troca
                if remanescente is not None:
                    if remanescente <= 0: status = 'VENCIDO'
                    elif remanescente <= ATENCAO_HORAS: status = 'ATENÇÃO'
                    else: status = 'OK'
                else: status = 'N/A'
            
            trocas.append({
                'identificacao': identificacao, 'tipo': tipo, 'data_troca': troca['data_troca'],
                'km_troca': km_troca, 'horimetro_troca': horimetro_troca,
                'km_atual': valor_atual if tipo == 'veiculo' else None,
                'horimetro_atual': valor_atual if tipo == 'maquina' else None,
                'proxima_troca': proxima_troca, 'remanescente': remanescente, 'status': status
            })
        ordem_status = {'VENCIDO': 0, 'ATENÇÃO': 1, 'OK': 2, 'N/A': 3}
        trocas.sort(key=lambda x: (ordem_status[x['status']], x['remanescente'] if x['remanescente'] is not None else float('inf')))
        return trocas
    except Exception as e:
        print(f"Erro ao obter trocas de óleo: {e}")
        return []
    finally:
        conn.close()

def obter_identificacoes_equipamentos():
    conn = get_db_connection()
    try:
        query = "SELECT DISTINCT identificacao FROM checklists WHERE identificacao IS NOT NULL AND identificacao != '' ORDER BY identificacao"
        df = pd.read_sql(query, conn)
        return df['identificacao'].tolist()
    except Exception as e:
        print(f"Erro ao obter identificações de equipamentos: {e}")
        return []
    finally:
        conn.close()

def obter_checklists_por_identificacao(identificacao):
    conn = get_db_connection()
    try:
        query = "SELECT id, data, horimetro, nivel_oleo, observacoes FROM checklists WHERE identificacao = ? ORDER BY data DESC, horimetro DESC"
        df = pd.read_sql(query, conn, params=(identificacao,))
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter checklists para identificação {identificacao}: {e}")
        return []
    finally:
        conn.close()

def excluir_troca_oleo(identificacao, tipo):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM trocas_oleo WHERE identificacao = ? AND tipo = ?", (identificacao, tipo))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao excluir troca de óleo: {e}")
        return False
    finally:
        conn.close()

def obter_manutencoes():
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            id, identificacao, tipo, frota, descricao, COALESCE(fornecedor, '') as fornecedor, COALESCE(valor, 0) as valor, data_abertura,
            COALESCE(previsao_conclusao, '') as previsao_conclusao, COALESCE(data_conclusao, '') as data_conclusao,
            COALESCE(observacoes, '') as observacoes, finalizada, COALESCE(prazo_liberacao, 0) as prazo_liberacao,
            COALESCE(forma_pagamento, '') as forma_pagamento, COALESCE(parcelas, 1) as parcelas, data_registro
        FROM manutencoes ORDER BY data_abertura DESC
        """
        df = pd.read_sql(query, conn)
        df['finalizada'] = df['finalizada'].astype(bool)
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter manutenções: {e}")
        return []
    finally:
        conn.close()

def obter_estatisticas_manutencoes():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as total FROM manutencoes")
        total = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) as abertas FROM manutencoes WHERE finalizada = 0")
        abertas = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) as finalizadas FROM manutencoes WHERE finalizada = 1")
        finalizadas = cursor.fetchone()[0]
        cursor.execute("SELECT COALESCE(SUM(valor), 0) as valor_total FROM manutencoes")
        valor_total = cursor.fetchone()[0]
        return {'total': total, 'abertas': abertas, 'finalizadas': finalizadas, 'valor_total': float(valor_total)}
    except Exception as e:
        print(f"Erro ao obter estatísticas de manutenções: {e}")
        return {'total': 0, 'abertas': 0, 'finalizadas': 0, 'valor_total': 0.0}
    finally:
        conn.close()

def obter_checklists():
    conn = get_db_connection()
    try:
        query = "SELECT * FROM checklists ORDER BY data DESC"
        df = pd.read_sql(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter checklists: {e}")
        return []
    finally:
        conn.close()
    
def obter_manutencao_por_id(id):
    conn = get_db_connection()
    try:
        query = """
        SELECT 
            id, identificacao, tipo, frota, descricao, COALESCE(fornecedor, '') as fornecedor, COALESCE(valor, 0) as valor, data_abertura,
            COALESCE(previsao_conclusao, '') as previsao_conclusao, COALESCE(data_conclusao, '') as data_conclusao,
            COALESCE(observacoes, '') as observacoes, finalizada, COALESCE(prazo_liberacao, 0) as prazo_liberacao,
            COALESCE(forma_pagamento, '') as forma_pagamento, COALESCE(parcelas, 1) as parcelas, data_registro
        FROM manutencoes 
        WHERE id = ?
        """
        df = pd.read_sql(query, conn, params=(id,))
        if not df.empty:
            df['finalizada'] = df['finalizada'].astype(bool)
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Erro ao obter manutenção: {e}")
        return None
    finally:
        conn.close()

def criar_manutencao(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO manutencoes (identificacao, tipo, frota, descricao, fornecedor, valor, data_abertura, previsao_conclusao, data_conclusao, observacoes, finalizada, prazo_liberacao, forma_pagamento, parcelas)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        fornecedor = dados.get('fornecedor', '') or ''
        valor = float(dados.get('valor', 0)) if dados.get('valor') not in [None, ''] else 0
        previsao_conclusao = dados.get('previsao_conclusao', '') or ''
        data_conclusao = dados.get('data_conclusao', '') or ''
        observacoes = dados.get('observacoes', '') or ''
        finalizada = 1 if dados.get('finalizada', False) else 0
        prazo_liberacao = int(dados.get('prazo_liberacao', 0)) if dados.get('prazo_liberacao') not in [None, ''] else None
        forma_pagamento = dados.get('forma_pagamento', '') or ''
        parcelas = int(dados.get('parcelas', 1)) if dados.get('parcelas') not in [None, ''] else 1
        
        cursor.execute(query, (dados['identificacao'], dados['tipo'], dados['frota'], dados['descricao'], fornecedor, valor, dados['data_abertura'], previsao_conclusao, data_conclusao, observacoes, finalizada, prazo_liberacao, forma_pagamento, parcelas))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"Erro ao criar manutenção: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def atualizar_manutencao(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    UPDATE manutencoes SET identificacao = ?, tipo = ?, frota = ?, descricao = ?, fornecedor = ?, valor = ?, data_abertura = ?, 
    previsao_conclusao = ?, data_conclusao = ?, observacoes = ?, finalizada = ?, prazo_liberacao = ?, forma_pagamento = ?, parcelas = ?
    WHERE id = ?
    """
    try:
        fornecedor = dados.get('fornecedor', '') or ''
        valor = float(dados.get('valor', 0)) if dados.get('valor') not in [None, ''] else 0
        previsao_conclusao = dados.get('previsao_conclusao', '') or ''
        data_conclusao = dados.get('data_conclusao', '') or ''
        observacoes = dados.get('observacoes', '') or ''
        finalizada = 1 if dados.get('finalizada', False) else 0
        prazo_liberacao = int(dados.get('prazo_liberacao', 0)) if dados.get('prazo_liberacao') not in [None, ''] else None
        forma_pagamento = dados.get('forma_pagamento', '') or ''
        parcelas = int(dados.get('parcelas', 1)) if dados.get('parcelas') not in [None, ''] else 1
        
        cursor.execute(query, (dados['identificacao'], dados['tipo'], dados['frota'], dados['descricao'], fornecedor, valor, dados['data_abertura'], previsao_conclusao, data_conclusao, observacoes, finalizada, prazo_liberacao, forma_pagamento, parcelas, id))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao atualizar manutenção: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def excluir_manutencao(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM manutencoes WHERE id = ?", (id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao excluir manutenção: {e}")
        return False
    finally:
        conn.close()

def atualizar_troca_oleo(identificacao, tipo, data_troca, km_troca=None, horimetro_troca=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if tipo == 'veiculo':
            if km_troca is None: raise ValueError("KM da troca é obrigatório para veículos")
            proxima_troca_km = km_troca + LIMITE_KM_TROCA
            proxima_troca_horimetro = None
        else:
            if horimetro_troca is None: raise ValueError("Horímetro da troca é obrigatório para máquinas")
            proxima_troca_km = None
            proxima_troca_horimetro = horimetro_troca + LIMITE_HORIMETRO_TROCA
        
        cursor.execute('''
            UPDATE trocas_oleo 
            SET data_troca = ?, km_troca = ?, horimetro_troca = ?, 
                    proxima_troca_km = ?, proxima_troca_horimetro = ?
            WHERE identificacao = ? AND tipo = ?
        ''', (data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro, identificacao, tipo))
        
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao atualizar troca de óleo: {e}")
        return False
    finally:
        conn.close()


def criar_checklist(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    INSERT INTO checklists (identificacao, data, horimetro, nivel_oleo, observacoes, itens_checklist)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    try:
        horimetro = float(dados['horimetro']) if dados.get('horimetro') else None
        cursor.execute(query, (
            dados['identificacao'], dados['data'], horimetro,
            dados['nivel_oleo'], dados.get('observacoes', ''), dados.get('itens_checklist', '')
        ))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()

def obter_checklist_por_id(id):
    conn = get_db_connection()
    try:
        checklist = conn.execute('SELECT * FROM checklists WHERE id = ?', (id,)).fetchone()
        return dict(checklist) if checklist else None
    finally:
        conn.close()

def atualizar_checklist(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
    UPDATE checklists SET
        identificacao = ?, data = ?, horimetro = ?, nivel_oleo = ?,
        observacoes = ?, itens_checklist = ?
    WHERE id = ?
    """
    try:
        horimetro = float(dados['horimetro']) if dados.get('horimetro') else None
        cursor.execute(query, (
            dados['identificacao'], dados['data'], horimetro,
            dados['nivel_oleo'], dados.get('observacoes', ''),
            dados.get('itens_checklist', ''), id
        ))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

def excluir_checklist(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM checklists WHERE id = ?', (id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

def obter_cotacoes_com_filtros(data_inicio=None, data_fim=None, status=None, pesquisa=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT c.*, u.username as criado_por_username
        FROM cotacoes c
        JOIN users u ON c.criado_por_id = u.id
        WHERE 1=1
    """
    params = []
    
    if data_inicio:
        query += " AND c.data_criacao >= ?"
        params.append(data_inicio)
    if data_fim:
        query += " AND date(c.data_criacao) <= ?"
        params.append(data_fim)
    if status:
        query += " AND c.status = ?"
        params.append(status)
    if pesquisa:
        pesquisa_like = f'%{pesquisa}%'
        query += " AND (c.titulo LIKE ? OR c.id LIKE ?)"
        params.extend([pesquisa_like, pesquisa_like])
        
    query += " ORDER BY c.id DESC"
    
    cursor.execute(query, params)
    cotacoes = cursor.fetchall()
    conn.close()
    return cotacoes


def obter_pedidos_compra_com_filtros(data_inicio=None, data_fim=None, status=None, pesquisa=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT pc.*, f.nome as fornecedor_nome
        FROM pedidos_compra pc
        JOIN fornecedores f ON pc.fornecedor_id = f.id
        WHERE 1=1
    """
    params = []
    
    if data_inicio:
        query += " AND pc.data_aprovacao >= ?"
        params.append(data_inicio)
    if data_fim:
        query += " AND date(pc.data_aprovacao) <= ?"
        params.append(data_fim)
    if status:
        query += " AND pc.status = ?"
        params.append(status)
    if pesquisa:
        pesquisa_like = f'%{pesquisa}%'
        query += " AND (pc.id LIKE ? OR pc.cotacao_id LIKE ? OR f.nome LIKE ?)"
        params.extend([pesquisa_like, pesquisa_like, pesquisa_like])
        
    query += " ORDER BY pc.id DESC"
    
    cursor.execute(query, params)
    pedidos = cursor.fetchall()
    conn.close()
    return pedidos