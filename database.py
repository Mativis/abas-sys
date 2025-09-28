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
    conn.row_factory = sqlite3.Row  # Permite acessar colunas por nome
    return conn

def criar_tabelas():
    """Cria todas as tabelas necessárias no banco de dados e insere usuários padrão."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Tabela de usuários (NOVA)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('Gestor', 'Comprador', 'Padrão'))
    )
    ''')

    # Tabela de fornecedores (NOVA)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fornecedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cnpj TEXT NOT NULL UNIQUE,
        nome TEXT NOT NULL,
        ie TEXT,
        endereco TEXT,
        tipo TEXT,
        contato TEXT,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Tabela de cotações (NOVA)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cotacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        item TEXT NOT NULL,
        quantidade REAL NOT NULL,
        data_limite TEXT NOT NULL,
        observacoes TEXT,
        fornecedor_id INTEGER,
        valor_fechado REAL,
        prazo_pagamento TEXT,
        faturamento TEXT,
        status TEXT DEFAULT 'Aberta', -- Aberta, Fechada, Aprovada, Rejeitada
        data_aprovacao TEXT,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (fornecedor_id) REFERENCES fornecedores (id)
    )
    ''')

    # Tabela de pedidos de compra (NOVA)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedidos_compra (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quote_id INTEGER,
        user_id INTEGER NOT NULL, -- ID do usuário que finalizou a compra/aprovou a cotação
        item TEXT NOT NULL,
        quantidade REAL NOT NULL,
        valor REAL NOT NULL,
        fornecedor_cnpj TEXT,
        status TEXT DEFAULT 'Aberto', -- Aberto, Em Edição, Finalizado, Cancelado
        nf_e_chave TEXT,
        nfs_pdf_path TEXT,
        data_abertura TEXT NOT NULL,
        data_finalizacao TEXT,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (quote_id) REFERENCES cotacoes (id)
    )
    ''')
    
    # Tabela de abastecimentos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS abastecimentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT NOT NULL,
        placa TEXT NOT NULL,
        responsavel TEXT,
        litros REAL NOT NULL,
        desconto REAL DEFAULT 0,
        odometro REAL,
        centro_custo TEXT,
        combustivel TEXT,
        custo_por_litro REAL NOT NULL,
        custo_bruto REAL NOT NULL,
        custo_liquido REAL NOT NULL,
        km_rodados REAL,
        km_litro REAL,
        posto TEXT,
        integracao_atheris BOOLEAN DEFAULT 0,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedagios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT NOT NULL,
        placa TEXT NOT NULL,
        valor REAL NOT NULL,
        observacoes TEXT,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS precos_combustivel (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        combustivel TEXT NOT NULL UNIQUE,
        preco REAL NOT NULL,
        data_atualizacao TEXT NOT NULL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trocas_oleo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identificacao TEXT NOT NULL,
        tipo TEXT CHECK(tipo IN ('veiculo', 'maquina')) NOT NULL,
        data_troca TEXT NOT NULL,
        km_troca REAL,
        horimetro_troca REAL,
        proxima_troca_km REAL,
        proxima_troca_horimetro REAL,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(identificacao, tipo)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS checklists (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identificacao TEXT NOT NULL,
        data TEXT NOT NULL,
        horimetro REAL,
        nivel_oleo TEXT CHECK(nivel_oleo IN ('ADEQUADO', 'BAIXO', 'CRÍTICO')) DEFAULT 'ADEQUADO',
        observacoes TEXT,
        itens_checklist TEXT,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS manutencoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identificacao TEXT NOT NULL,
        tipo TEXT NOT NULL,
        frota TEXT NOT NULL,
        descricao TEXT NOT NULL,
        fornecedor TEXT,
        valor REAL DEFAULT 0,
        data_abertura TEXT NOT NULL,
        previsao_conclusao TEXT,
        data_conclusao TEXT,
        observacoes TEXT,
        finalizada BOOLEAN DEFAULT 0,
        prazo_liberacao INTEGER,
        forma_pagamento TEXT,
        parcelas INTEGER DEFAULT 1,
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Inserir Usuários de Exemplo
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        users_iniciais = [
            ('gestor', generate_password_hash('123'), 'Gestor'),
            ('comprador', generate_password_hash('123'), 'Comprador'),
            ('padrao', generate_password_hash('123'), 'Padrão')
        ]
        
        cursor.executemany('''
        INSERT INTO users (username, password_hash, role)
        VALUES (?, ?, ?)
        ''', users_iniciais)

    # Inserir preços iniciais de combustível (mantido do original)
    cursor.execute("SELECT COUNT(*) FROM precos_combustivel")
    if cursor.fetchone()[0] == 0:
        precos_iniciais = [
            ('GASOLINA', 5.890, datetime.now().strftime('%Y-%m-%d')),
            ('DIESEL S10', 4.950, datetime.now().strftime('%Y-%m-%d')),
            ('ETANOL', 4.390, datetime.now().strftime('%Y-%m-%d')),
            ('DESSEL S500', 4.750, datetime.now().strftime('%Y-%m-%d')),
            ('ARLA', 2.990, datetime.now().strftime('%Y-%m-%d'))
        ]
        
        cursor.executemany('''
        INSERT INTO precos_combustivel (combustivel, preco, data_atualizacao)
        VALUES (?, ?, ?)
        ''', precos_iniciais)


    conn.commit()
    conn.close()

# --- Funções de Usuário (NOVO) ---
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

# --- Funções de Fornecedor (NOVO) ---
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
        return False # CNPJ duplicado
    finally:
        conn.close()

def obter_fornecedores():
    conn = get_db_connection()
    df = pd.read_sql('SELECT id, cnpj, nome, ie, tipo, contato, data_registro FROM fornecedores ORDER BY nome', conn)
    conn.close()
    return df.to_dict('records')

# --- Funções de Cotação (NOVO) ---
def criar_cotacao(user_id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO cotacoes (user_id, item, quantidade, data_limite, observacoes)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, dados['item'], float(dados['quantidade']), dados['data_limite'], dados['observacoes']))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()

def obter_cotacoes():
    conn = get_db_connection()
    query = '''
        SELECT 
            c.*, 
            u.username as solicitante,
            f.nome as fornecedor_nome
        FROM cotacoes c
        JOIN users u ON c.user_id = u.id
        LEFT JOIN fornecedores f ON c.fornecedor_id = f.id
        ORDER BY c.data_registro DESC
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def fechar_cotacao(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        fornecedor = conn.execute('SELECT cnpj FROM fornecedores WHERE id = ?', (dados['fornecedor_id'],)).fetchone()
        fornecedor_cnpj = fornecedor['cnpj'] if fornecedor else None
        
        cursor.execute('''
            UPDATE cotacoes 
            SET fornecedor_id = ?, valor_fechado = ?, prazo_pagamento = ?, faturamento = ?, status = 'Fechada'
            WHERE id = ? AND status = 'Aberta'
        ''', (dados['fornecedor_id'] or None, float(dados['valor_fechado']), dados['prazo_pagamento'], dados['faturamento'], id))
        conn.commit()
        return cursor.rowcount > 0, fornecedor_cnpj
    finally:
        conn.close()

def aprovar_cotacao(id, user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. Atualizar status da cotação para Aprovada
        cursor.execute("UPDATE cotacoes SET status = 'Aprovada', data_aprovacao = ? WHERE id = ? AND status = 'Fechada'", (datetime.now().strftime('%Y-%m-%d'), id))
        
        cotacao = pd.read_sql('SELECT * FROM cotacoes WHERE id = ?', conn, params=(id,)).iloc[0].to_dict()
        fornecedor = pd.read_sql('SELECT cnpj FROM fornecedores WHERE id = ?', conn, params=(cotacao['fornecedor_id'],)).iloc[0].to_dict()

        if cursor.rowcount > 0 and cotacao:
            # 2. Gerar Pedido de Compra
            cursor.execute('''
                INSERT INTO pedidos_compra (quote_id, user_id, item, quantidade, valor, data_abertura, status, fornecedor_cnpj)
                VALUES (?, ?, ?, ?, ?, ?, 'Aberto', ?)
            ''', (id, user_id, cotacao['item'], cotacao['quantidade'], cotacao['valor_fechado'], datetime.now().strftime('%Y-%m-%d'), fornecedor['cnpj']))
            conn.commit()
            return True
        return False
    finally:
        conn.close()

# --- Funções de Pedido de Compra (NOVO) ---
def obter_pedidos_compra():
    conn = get_db_connection()
    query = '''
        SELECT 
            p.*, 
            u.username as comprador,
            f.nome as fornecedor_nome
        FROM pedidos_compra p
        JOIN users u ON p.user_id = u.id
        LEFT JOIN fornecedores f ON p.fornecedor_cnpj = f.cnpj
        ORDER BY p.data_abertura DESC
    '''
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def obter_pedido_compra_por_id(id):
    conn = get_db_connection()
    query = '''
        SELECT 
            p.*, 
            u.username as comprador,
            f.nome as fornecedor_nome
        FROM pedidos_compra p
        JOIN users u ON p.user_id = u.id
        LEFT JOIN fornecedores f ON p.fornecedor_cnpj = f.cnpj
        WHERE p.id = ?
    '''
    df = pd.read_sql(query, conn, params=(id,))
    conn.close()
    if not df.empty:
        return df.iloc[0].to_dict()
    return None

def atualizar_pedido_compra(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE pedidos_compra 
            SET item = ?, quantidade = ?, valor = ?, status = ?
            WHERE id = ?
        ''', (dados['item'], float(dados['quantidade']), float(dados['valor']), dados['status'], id))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

def finalizar_pedido_compra(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE pedidos_compra 
            SET status = 'Finalizado', data_finalizacao = ?, nf_e_chave = ?, nfs_pdf_path = ?
            WHERE id = ? AND status != 'Finalizado'
        ''', (datetime.now().strftime('%Y-%m-%d'), dados.get('nf_e_chave'), dados.get('nfs_pdf_path'), id))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()

# --- Funções de Dealer Intelligence (NOVO) ---
def obter_dealer_intelligence(data_inicio, data_fim):
    conn = get_db_connection()
    
    query_valores = '''
        SELECT 
            SUM(CASE WHEN p.status = 'Finalizado' THEN p.valor ELSE 0 END) as total_fechado,
            SUM(c.valor_fechado) as total_orcado
        FROM cotacoes c
        LEFT JOIN pedidos_compra p ON c.id = p.quote_id
        WHERE c.data_registro BETWEEN ? AND ?
    '''
    
    query_tempo = '''
        SELECT
            p.data_abertura,
            p.data_finalizacao
        FROM pedidos_compra p
        WHERE p.data_finalizacao IS NOT NULL AND p.data_abertura BETWEEN ? AND ?
    '''

    df_valores = pd.read_sql(query_valores, conn, params=(data_inicio, data_fim))
    df_tempo = pd.read_sql(query_tempo, conn, params=(data_inicio, data_fim))

    conn.close()
    
    total_fechado = df_valores['total_fechado'].iloc[0] if not df_valores.empty else 0
    total_orcado = df_valores['total_orcado'].iloc[0] if not df_valores.empty else 0
    
    relatorio_processamento = []
    media_dias_processamento = 0
    
    if not df_tempo.empty:
        df_tempo['data_abertura'] = pd.to_datetime(df_tempo['data_abertura'])
        df_tempo['data_finalizacao'] = pd.to_datetime(df_tempo['data_finalizacao'])
        df_tempo['dias_processamento'] = (df_tempo['data_finalizacao'] - df_tempo['data_abertura']).dt.days
        media_dias_processamento = df_tempo['dias_processamento'].mean()
        
        relatorio_processamento = df_tempo[['data_abertura', 'data_finalizacao', 'dias_processamento']].to_dict('records')

    return {
        'total_fechado': total_fechado,
        'total_orcado': total_orcado,
        'media_dias_processamento': media_dias_processamento if pd.notna(media_dias_processamento) else 0,
        'relatorio_processamento': relatorio_processamento
    }

# --- Funções Antigas (Mantidas/Corrigidas) ---
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
    """Exclui um registro de abastecimento pelo ID. (Função solicitada para correção)"""
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
    """Obtém todos os checklists (necessário para a listagem da rota /checklists)"""
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
    """Busca uma manutenção específica por ID (manutenções.html)"""
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
    """Cria uma nova manutenção (api_manutencoes POST)"""
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
    """Atualiza uma manutenção existente (api_manutencoes PUT)"""
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
    """Exclui uma manutenção pelo ID (api_manutencoes DELETE)"""
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
    """Atualiza dados de troca de óleo (usada na lógica de metricas-uso, mas não exposta na API principal do app.py)"""
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

# Fim das Funções (Corrigidas)