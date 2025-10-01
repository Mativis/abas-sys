import sqlite3
from werkzeug.security import generate_password_hash
import pandas as pd
from datetime import datetime

# --- Configuração do Banco de Dados ---

def dict_factory(cursor, row):
    """Converte as tuplas do banco de dados em dicionários."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    conn = sqlite3.connect('abastecimentos.db', timeout=10)
    conn.row_factory = dict_factory
    return conn

def criar_tabelas():
    """Cria todas as tabelas necessárias no banco de dados se elas não existirem."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Tabela de Abastecimentos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS abastecimentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE,
            placa TEXT,
            km INTEGER,
            combustivel TEXT,
            litros REAL,
            custo_por_litro REAL,
            custo_bruto REAL,
            desconto REAL,
            custo_liquido REAL,
            posto TEXT,
            motorista TEXT,
            centro_custo TEXT,
            observacoes TEXT
        )
    ''')

    # Tabela de Preços de Combustível
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS precos_combustivel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            combustivel TEXT UNIQUE NOT NULL,
            preco REAL NOT NULL
        )
    ''')
    
    # Tabela de Manutenções
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS manutencoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            placa TEXT NOT NULL,
            tipo_manutencao TEXT NOT NULL,
            descricao TEXT,
            custo REAL,
            status TEXT DEFAULT 'Aberta'
        )
    ''')

    # Tabela de Trocas de Óleo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trocas_oleo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identificacao TEXT NOT NULL,
            tipo_oleo TEXT NOT NULL,
            data_troca DATE NOT NULL,
            km_troca INTEGER,
            horimetro_troca INTEGER,
            proxima_troca_km INTEGER,
            proxima_troca_horimetro INTEGER,
            UNIQUE(identificacao, tipo_oleo)
        )
    ''')

    # Tabela de Checklists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS checklists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            identificacao TEXT NOT NULL,
            tipo_veiculo TEXT NOT NULL,
            horimetro_km INTEGER NOT NULL,
            status_geral TEXT NOT NULL,
            observacoes TEXT
        )
    ''')
    
    # Tabela de Pedágios
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedagios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            placa TEXT NOT NULL,
            valor REAL NOT NULL,
            localizacao TEXT
        )
    ''')
    
    # Tabela de Usuários
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('Administrador', 'Gestor', 'Comprador', 'Padrão'))
        )
    ''')

    # Tabelas do Módulo Dealers
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fornecedores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            cnpj TEXT,
            contato_nome TEXT,
            contato_email TEXT,
            contato_telefone TEXT,
            data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cotacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            titulo TEXT NOT NULL,
            criado_por_id INTEGER NOT NULL,
            data_criacao DATE NOT NULL,
            data_limite DATE NOT NULL,
            observacoes TEXT,
            status TEXT NOT NULL DEFAULT 'Aberta',
            FOREIGN KEY (criado_por_id) REFERENCES users(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cotacao_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cotacao_id INTEGER NOT NULL,
            descricao TEXT NOT NULL,
            quantidade INTEGER NOT NULL,
            FOREIGN KEY (cotacao_id) REFERENCES cotacoes(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orcamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cotacao_id INTEGER NOT NULL,
            fornecedor_id INTEGER NOT NULL,
            valor REAL NOT NULL,
            prazo_pagamento TEXT,
            faturamento TEXT,
            data_orcamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            aprovado INTEGER DEFAULT 0,
            FOREIGN KEY (cotacao_id) REFERENCES cotacoes(id),
            FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pedidos_compra (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cotacao_id INTEGER NOT NULL,
            orcamento_id INTEGER NOT NULL,
            fornecedor_id INTEGER NOT NULL,
            aprovado_por_id INTEGER NOT NULL,
            data_aprovacao DATE NOT NULL,
            valor_total REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'Aberto',
            nf_e_chave TEXT,
            nfs_pdf_path TEXT,
            FOREIGN KEY (cotacao_id) REFERENCES cotacoes(id),
            FOREIGN KEY (orcamento_id) REFERENCES orcamentos(id),
            FOREIGN KEY (fornecedor_id) REFERENCES fornecedores(id),
            FOREIGN KEY (aprovado_por_id) REFERENCES users(id)
        )
    ''')

    conn.commit()
    conn.close()


# --- Funções de Relatórios e CRUD Principal ---

def obter_relatorio(data_inicio, data_fim, placa=None, centro_custo=None, combustivel=None, posto=None):
    conn = get_db_connection()
    query = "SELECT * FROM abastecimentos WHERE data BETWEEN ? AND ?"
    params = [data_inicio, data_fim]
    
    if placa:
        query += " AND placa = ?"
        params.append(placa)
    if centro_custo:
        query += " AND centro_custo = ?"
        params.append(centro_custo)
    if combustivel:
        query += " AND combustivel = ?"
        params.append(combustivel)
    if posto:
        query += " AND posto = ?"
        params.append(posto)
        
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def calcular_medias_veiculos():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT placa, km, litros FROM abastecimentos ORDER BY placa, km", conn)
    conn.close()
    
    if df.empty:
        return []

    df['km_anterior'] = df.groupby('placa')['km'].shift(1)
    df['km_rodado'] = df['km'] - df['km_anterior']
    df = df[df['km_rodado'] > 0]
    df['kml'] = df['km_rodado'] / df['litros']
    
    medias = df.groupby('placa')['kml'].mean().reset_index()
    medias['kml'] = medias['kml'].round(2)
    return medias.to_dict('records')

def obter_precos_combustivel():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM precos_combustivel")
    precos = cursor.fetchall()
    conn.close()
    return precos

def atualizar_preco_combustivel(combustivel, preco):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE precos_combustivel SET preco = ? WHERE combustivel = ?", (preco, combustivel))
    conn.commit()
    conn.close()

def criar_combustivel(combustivel, preco):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO precos_combustivel (combustivel, preco) VALUES (?, ?)", (combustivel, preco))
        conn.commit()
    except sqlite3.IntegrityError:
        # Combustível já existe
        return False
    finally:
        conn.close()
    return True

def obter_opcoes_filtro(coluna):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT {coluna} FROM abastecimentos WHERE {coluna} IS NOT NULL ORDER BY {coluna}")
    opcoes = [row[coluna] for row in cursor.fetchall()]
    conn.close()
    return opcoes

def excluir_registro(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM abastecimentos WHERE id = ?", (id,))
    conn.commit()
    rows_deleted = cursor.rowcount
    conn.close()
    return rows_deleted > 0

def atualizar_registro(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE abastecimentos 
        SET data=?, placa=?, km=?, combustivel=?, litros=?, custo_por_litro=?, 
            custo_bruto=?, desconto=?, custo_liquido=?, posto=?, motorista=?, 
            centro_custo=?, observacoes=?
        WHERE id=?
    """, (
        dados['data'], dados['placa'], dados['km'], dados['combustivel'], dados['litros'], 
        dados['custo_por_litro'], dados['custo_bruto'], dados.get('desconto'), dados['custo_liquido'], 
        dados.get('posto'), dados.get('motorista'), dados.get('centro_custo'), 
        dados.get('observacoes'), id
    ))
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated > 0

def criar_registro(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO abastecimentos (data, placa, km, combustivel, litros, custo_por_litro, custo_bruto, desconto, custo_liquido, posto, motorista, centro_custo, observacoes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        dados['data'], dados['placa'], dados['km'], dados['combustivel'], dados['litros'],
        dados['custo_por_litro'], dados['custo_bruto'], dados.get('desconto'), dados['custo_liquido'],
        dados.get('posto'), dados.get('motorista'), dados.get('centro_custo'), dados.get('observacoes')
    ))
    conn.commit()
    id = cursor.lastrowid
    conn.close()
    return id

def obter_registro_por_id(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM abastecimentos WHERE id = ?", (id,))
    registro = cursor.fetchone()
    conn.close()
    return registro

# --- Funções de Métricas de Uso (Troca de Óleo) ---

def obter_trocas_oleo():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trocas_oleo ORDER BY identificacao, tipo_oleo")
    trocas = cursor.fetchall()
    conn.close()
    return trocas

def salvar_troca_oleo(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO trocas_oleo (identificacao, tipo_oleo, data_troca, km_troca, horimetro_troca, proxima_troca_km, proxima_troca_horimetro)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            dados['identificacao'], dados['tipo_oleo'], dados['data_troca'], dados.get('km_troca'), 
            dados.get('horimetro_troca'), dados.get('proxima_troca_km'), dados.get('proxima_troca_horimetro')
        ))
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    return True

def obter_placas_veiculos():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT placa FROM abastecimentos ORDER BY placa")
    placas = [row['placa'] for row in cursor.fetchall()]
    conn.close()
    return placas

def obter_identificacoes_equipamentos():
    # Esta função pode ser melhorada para buscar de uma tabela de equipamentos no futuro
    return ["EQ-01", "EQ-02", "EQ-03"]

def obter_checklists_por_identificacao(identificacao):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM checklists WHERE identificacao = ? ORDER BY data DESC", (identificacao,))
    checklists = cursor.fetchall()
    conn.close()
    return checklists

def excluir_troca_oleo(identificacao, tipo):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM trocas_oleo WHERE identificacao = ? AND tipo_oleo = ?", (identificacao, tipo))
    conn.commit()
    rows_deleted = cursor.rowcount
    conn.close()
    return rows_deleted > 0

def obter_troca_oleo_por_identificacao_tipo(identificacao, tipo):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trocas_oleo WHERE identificacao = ? AND tipo_oleo = ?", (identificacao, tipo))
    troca = cursor.fetchone()
    conn.close()
    return troca

def atualizar_troca_oleo(identificacao, tipo, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE trocas_oleo 
        SET data_troca=?, km_troca=?, horimetro_troca=?, proxima_troca_km=?, proxima_troca_horimetro=?
        WHERE identificacao=? AND tipo_oleo=?
    """, (
        dados['data_troca'], dados.get('km_troca'), dados.get('horimetro_troca'),
        dados.get('proxima_troca_km'), dados.get('proxima_troca_horimetro'),
        identificacao, tipo
    ))
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated > 0

# --- Funções de Manutenções ---

def obter_manutencoes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM manutencoes ORDER BY data DESC")
    manutencoes = cursor.fetchall()
    conn.close()
    return manutencoes

def obter_manutencao_por_id(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM manutencoes WHERE id = ?", (id,))
    manutencao = cursor.fetchone()
    conn.close()
    return manutencao

def criar_manutencao(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO manutencoes (data, placa, tipo_manutencao, descricao, custo, status) VALUES (?, ?, ?, ?, ?, ?)",
        (dados['data'], dados['placa'], dados['tipo_manutencao'], dados['descricao'], dados['custo'], dados.get('status', 'Aberta'))
    )
    conn.commit()
    id = cursor.lastrowid
    conn.close()
    return id

def atualizar_manutencao(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE manutencoes SET data=?, placa=?, tipo_manutencao=?, descricao=?, custo=?, status=? WHERE id=?",
        (dados['data'], dados['placa'], dados['tipo_manutencao'], dados['descricao'], dados['custo'], dados['status'], id)
    )
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated > 0

def excluir_manutencao(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM manutencoes WHERE id = ?", (id,))
    conn.commit()
    rows_deleted = cursor.rowcount
    conn.close()
    return rows_deleted > 0

def obter_estatisticas_manutencoes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total, SUM(CASE WHEN status = 'Aberta' THEN 1 ELSE 0 END) as abertas, SUM(CASE WHEN status = 'Finalizada' THEN 1 ELSE 0 END) as finalizadas, SUM(custo) as valor_total FROM manutencoes")
    estatisticas = cursor.fetchone()
    conn.close()
    return estatisticas

# --- Funções de Pedágios ---

def criar_pedagio(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO pedagios (data, placa, valor, localizacao) VALUES (?, ?, ?, ?)",
        (dados['data'], dados['placa'], dados['valor'], dados.get('localizacao'))
    )
    conn.commit()
    id = cursor.lastrowid
    conn.close()
    return id

def obter_pedagios_com_filtros(data_inicio, data_fim, placa=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "SELECT * FROM pedagios WHERE data BETWEEN ? AND ?"
    params = [data_inicio, data_fim]
    if placa:
        query += " AND placa = ?"
        params.append(placa)
    query += " ORDER BY data DESC"
    cursor.execute(query, params)
    pedagios = cursor.fetchall()
    conn.close()
    return pedagios

def obter_pedagio_por_id(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM pedagios WHERE id = ?", (id,))
    pedagio = cursor.fetchone()
    conn.close()
    return pedagio

def atualizar_pedagio(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE pedagios SET data=?, placa=?, valor=?, localizacao=? WHERE id=?",
        (dados['data'], dados['placa'], dados['valor'], dados.get('localizacao'), id)
    )
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated > 0

def excluir_pedagio(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM pedagios WHERE id = ?", (id,))
    conn.commit()
    rows_deleted = cursor.rowcount
    conn.close()
    return rows_deleted > 0

# --- Funções de Checklists ---

def obter_checklists():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM checklists ORDER BY data DESC")
    checklists = cursor.fetchall()
    conn.close()
    return checklists

def criar_checklist(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO checklists (data, identificacao, tipo_veiculo, horimetro_km, status_geral, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
        (dados['data'], dados['identificacao'], dados['tipo_veiculo'], dados['horimetro_km'], dados['status_geral'], dados.get('observacoes'))
    )
    conn.commit()
    id = cursor.lastrowid
    conn.close()
    return id

def obter_checklist_por_id(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM checklists WHERE id = ?", (id,))
    checklist = cursor.fetchone()
    conn.close()
    return checklist

def atualizar_checklist(id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE checklists SET data=?, identificacao=?, tipo_veiculo=?, horimetro_km=?, status_geral=?, observacoes=? WHERE id=?",
        (dados['data'], dados['identificacao'], dados['tipo_veiculo'], dados['horimetro_km'], dados['status_geral'], dados.get('observacoes'), id)
    )
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated > 0

def excluir_checklist(id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM checklists WHERE id = ?", (id,))
    conn.commit()
    rows_deleted = cursor.rowcount
    conn.close()
    return rows_deleted > 0

# --- Funções de Usuários ---

def get_user_by_username(username):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    user = cursor.fetchone()
    conn.close()
    return user

def get_user_by_id(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def get_all_users():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, role FROM users ORDER BY username")
    users = cursor.fetchall()
    conn.close()
    return users

def create_user(username, password, role):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)", 
                       (username, generate_password_hash(password), role))
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    return True

def update_user(user_id, username, role, password=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if password:
            cursor.execute("UPDATE users SET username=?, role=?, password_hash=? WHERE id=?", 
                           (username, role, generate_password_hash(password), user_id))
        else:
            cursor.execute("UPDATE users SET username=?, role=? WHERE id=?", (username, role, user_id))
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    return True

def delete_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    rows_deleted = cursor.rowcount
    conn.close()
    return rows_deleted > 0

# --- Funções do Módulo Dealers ---

def obter_fornecedores():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fornecedores ORDER BY nome")
    fornecedores = cursor.fetchall()
    conn.close()
    return fornecedores

def criar_fornecedor(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO fornecedores (nome, cnpj, contato_nome, contato_email, contato_telefone)
            VALUES (?, ?, ?, ?, ?)
        """, (
            dados['nome'], dados.get('cnpj'), dados.get('contato_nome'),
            dados.get('contato_email'), dados.get('contato_telefone')
        ))
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    return True

def obter_cotacoes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.*, u.username as criado_por_username
        FROM cotacoes c
        JOIN users u ON c.criado_por_id = u.id
        ORDER BY c.id DESC
    """)
    cotacoes = cursor.fetchall()
    conn.close()
    return cotacoes

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

def criar_cotacao_com_itens(user_id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO cotacoes (titulo, criado_por_id, data_criacao, data_limite, observacoes)
            VALUES (?, ?, ?, ?, ?)
        """, (
            dados['titulo'], user_id, datetime.now().strftime('%Y-%m-%d'),
            dados['data_limite'], dados.get('observacoes')
        ))
        cotacao_id = cursor.lastrowid

        for item in dados['itens']:
            cursor.execute("""
                INSERT INTO cotacao_itens (cotacao_id, descricao, quantidade)
                VALUES (?, ?, ?)
            """, (cotacao_id, item['descricao'], item['quantidade']))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar cotação: {e}")
        return None
    finally:
        conn.close()
    return cotacao_id

def obter_cotacao_por_id(cotacao_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT c.*, u.username as criado_por_username FROM cotacoes c JOIN users u ON c.criado_por_id = u.id WHERE c.id = ?", (cotacao_id,))
    cotacao = cursor.fetchone()
    conn.close()
    return cotacao

def obter_itens_por_cotacao_id(cotacao_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cotacao_itens WHERE cotacao_id = ?", (cotacao_id,))
    itens = cursor.fetchall()
    conn.close()
    return itens

def adicionar_orcamento(dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO orcamentos (cotacao_id, fornecedor_id, valor, prazo_pagamento, faturamento)
            VALUES (?, ?, ?, ?, ?)
        """, (
            dados['cotacao_id'], dados['fornecedor_id'], dados['valor'],
            dados.get('prazo_pagamento'), dados.get('faturamento')
        ))
        conn.commit()
    except Exception as e:
        print(f"Erro ao adicionar orçamento: {e}")
        return False
    finally:
        conn.close()
    return True

def aprovar_orcamento(orcamento_id, user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Pega dados do orçamento
        cursor.execute("SELECT * FROM orcamentos WHERE id = ?", (orcamento_id,))
        orcamento = cursor.fetchone()
        if not orcamento:
            return None

        # Marca orçamento como aprovado
        cursor.execute("UPDATE orcamentos SET aprovado = 1 WHERE id = ?", (orcamento_id,))
        
        # Fecha a cotação
        cursor.execute("UPDATE cotacoes SET status = 'Fechada' WHERE id = ?", (orcamento['cotacao_id'],))

        # Cria o pedido de compra
        cursor.execute("""
            INSERT INTO pedidos_compra (cotacao_id, orcamento_id, fornecedor_id, aprovado_por_id, data_aprovacao, valor_total)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            orcamento['cotacao_id'], orcamento_id, orcamento['fornecedor_id'],
            user_id, datetime.now().strftime('%Y-%m-%d'), orcamento['valor']
        ))
        pedido_id = cursor.lastrowid
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro ao aprovar orçamento: {e}")
        return None
    finally:
        conn.close()
    return pedido_id

def obter_orcamentos_por_cotacao_id(cotacao_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT o.*, f.nome as fornecedor_nome
        FROM orcamentos o
        JOIN fornecedores f ON o.fornecedor_id = f.id
        WHERE o.cotacao_id = ?
        ORDER BY o.valor ASC
    """, (cotacao_id,))
    orcamentos = cursor.fetchall()
    conn.close()
    return orcamentos

def obter_pedidos_compra():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pc.*, f.nome as fornecedor_nome
        FROM pedidos_compra pc
        JOIN fornecedores f ON pc.fornecedor_id = f.id
        ORDER BY pc.id DESC
    """)
    pedidos = cursor.fetchall()
    conn.close()
    return pedidos

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

def obter_pedido_compra_por_id(pedido_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT pc.*, f.nome as fornecedor_nome, u.username as aprovado_por_username
        FROM pedidos_compra pc
        JOIN fornecedores f ON pc.fornecedor_id = f.id
        JOIN users u ON pc.aprovado_por_id = u.id
        WHERE pc.id = ?
    """, (pedido_id,))
    pedido = cursor.fetchone()
    conn.close()
    return pedido

def obter_itens_por_pedido_id(pedido_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ci.* FROM cotacao_itens ci
        JOIN pedidos_compra pc ON ci.cotacao_id = pc.cotacao_id
        WHERE pc.id = ?
    """, (pedido_id,))
    itens = cursor.fetchall()
    conn.close()
    return itens

def finalizar_pedido_compra(pedido_id, dados):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE pedidos_compra SET status='Finalizado', nf_e_chave=?, nfs_pdf_path=? WHERE id=?",
            (dados.get('nf_e_chave'), dados.get('nfs_pdf_path'), pedido_id)
        )
        conn.commit()
    except Exception as e:
        print(f"Erro ao finalizar pedido: {e}")
        return False
    finally:
        conn.close()
    return True

def obter_dealer_intelligence(data_inicio, data_fim):
    conn = get_db_connection()
    
    query_pedidos = """
        SELECT f.nome as fornecedor, SUM(pc.valor_total) as total_gasto
        FROM pedidos_compra pc
        JOIN fornecedores f ON pc.fornecedor_id = f.id
        WHERE pc.data_aprovacao BETWEEN ? AND ?
        GROUP BY f.nome
        ORDER BY total_gasto DESC
    """
    
    query_itens = """
        SELECT ci.descricao, SUM(ci.quantidade) as total_comprado
        FROM cotacao_itens ci
        JOIN pedidos_compra pc ON ci.cotacao_id = pc.cotacao_id
        WHERE pc.data_aprovacao BETWEEN ? AND ?
        GROUP BY ci.descricao
        ORDER BY total_comprado DESC
    """

    df_pedidos = pd.read_sql_query(query_pedidos, conn, params=[data_inicio, data_fim])
    df_itens = pd.read_sql_query(query_itens, conn, params=[data_inicio, data_fim])
    
    conn.close()
    
    return {
        'top_fornecedores': df_pedidos.to_dict('records'),
        'top_itens': df_itens.to_dict('records')
    }