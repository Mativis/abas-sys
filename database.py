import sqlite3
import pandas as pd
from datetime import datetime

def criar_tabelas():
    """Cria todas as tabelas necessárias no banco de dados"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
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
    
    # Tabela de preços de combustíveis
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS precos_combustivel (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        combustivel TEXT NOT NULL UNIQUE,
        preco REAL NOT NULL,
        data_atualizacao TEXT NOT NULL
    )
    ''')
    
    # Tabela de trocas de óleo (ATUALIZADA)
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
    
    # Tabela de checklists
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
    
    # Tabela de manutenções (ATUALIZADA COM NOVOS CAMPOS)
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
        prazo_liberacao INTEGER,           -- NOVO CAMPO: prazo em dias úteis
        forma_pagamento TEXT,              -- NOVO CAMPO: forma de pagamento
        parcelas INTEGER DEFAULT 1,        -- NOVO CAMPO: número de parcelas
        data_registro TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Verificar e atualizar a estrutura da tabela trocas_oleo se necessário
    cursor.execute("PRAGMA table_info(trocas_oleo)")
    colunas_existentes = [coluna[1] for coluna in cursor.fetchall()]

    # Atualizar estrutura da tabela trocas_oleo se for antiga
    if 'placa' in colunas_existentes and 'identificacao' not in colunas_existentes:
        print("Atualizando estrutura da tabela trocas_oleo...")
        
        # Criar tabela temporária com nova estrutura
        cursor.execute('''
        CREATE TABLE trocas_oleo_temp (
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
        
        # Copiar dados da tabela antiga para a nova
        cursor.execute('''
        INSERT INTO trocas_oleo_temp (id, identificacao, tipo, data_troca, km_troca, horimetro_troca, 
                                      proxima_troca_km, proxima_troca_horimetro, data_registro)
        SELECT id, placa, 'veiculo', data_troca, km_troca, horimetro_troca, 
               proxima_troca_km, proxima_troca_horimetro, data_registro
        FROM trocas_oleo
        ''')
        
        # Excluir tabela antiga
        cursor.execute('DROP TABLE trocas_oleo')
        
        # Renomear tabela temporária
        cursor.execute('ALTER TABLE trocas_oleo_temp RENAME TO trocas_oleo')
        
        print("Estrutura da tabela trocas_oleo atualizada com sucesso!")
    
    # Verificar e atualizar a estrutura da tabela manutencoes se necessário
    cursor.execute("PRAGMA table_info(manutencoes)")
    colunas_manutencao = [coluna[1] for coluna in cursor.fetchall()]
    
    # Adicionar novos campos se não existirem
    if 'prazo_liberacao' not in colunas_manutencao:
        cursor.execute('ALTER TABLE manutencoes ADD COLUMN prazo_liberacao INTEGER')
        print("Campo 'prazo_liberacao' adicionado à tabela manutencoes.")
    
    if 'forma_pagamento' not in colunas_manutencao:
        cursor.execute('ALTER TABLE manutencoes ADD COLUMN forma_pagamento TEXT')
        print("Campo 'forma_pagamento' adicionado à tabela manutencoes.")
    
    if 'parcelas' not in colunas_manutencao:
        cursor.execute('ALTER TABLE manutencoes ADD COLUMN parcelas INTEGER DEFAULT 1')
        print("Campo 'parcelas' adicionado à tabela manutencoes.")
    
    # Valores padrão para combustíveis
    cursor.execute("SELECT COUNT(*) FROM precos_combustivel")
    if cursor.fetchone()[0] == 0:
        precos_iniciais = [
            ('GASOLINA', 5.890, datetime.now().strftime('%Y-%m-%d')),
            ('DIESEL S10', 4.950, datetime.now().strftime('%Y-%m-%d')),
            ('ETANOL', 4.390, datetime.now().strftime('%Y-%m-%d')),
            ('DIESEL S500', 4.750, datetime.now().strftime('%Y-%m-%d')),
            ('ARLA', 2.990, datetime.now().strftime('%Y-%m-%d'))
        ]
        
        cursor.executemany('''
        INSERT INTO precos_combustivel (combustivel, preco, data_atualizacao)
        VALUES (?, ?, ?)
        ''', precos_iniciais)
    
    conn.commit()
    conn.close()

def criar_combustivel(combustivel, preco):
    """Cria um novo tipo de combustível"""
    conn = sqlite3.connect('abastecimentos.db')
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

def obter_precos_combustivel():
    """Retorna os preços atuais dos combustíveis"""
    conn = sqlite3.connect('abastecimentos.db')
    query = "SELECT combustivel, preco, data_atualizacao FROM precos_combustivel ORDER BY combustivel"
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def atualizar_preco_combustivel(combustivel, novo_preco):
    """Atualiza o preço de um combustível"""
    conn = sqlite3.connect('abastecimentos.db')
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

def criar_abastecimento_atheris(dados):
    """Cria um novo registro de abastecimento via integração Atheris"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    # Calcular km_litro se possível
    km_litro = None
    if dados.get('odometro'):
        # Buscar o último odômetro deste veículo
        cursor.execute(
            "SELECT MAX(odometro) FROM abastecimentos WHERE placa = ? AND odometro IS NOT NULL",
            (dados['placa'].upper(),)
        )
        ultimo_odometro = cursor.fetchone()[0]
        
        if ultimo_odometro and dados['odometro'] > ultimo_odometro and dados['litros'] > 0:
            km_rodados = dados['odometro'] - ultimo_odometro
            km_litro = km_rodados / dados['litros']
    
    query = '''
    INSERT INTO abastecimentos (
        data, placa, responsavel, litros, desconto, odometro,
        centro_custo, combustivel, custo_por_litro, custo_bruto, 
        custo_liquido, posto, integracao_atheris, km_litro
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
    '''
    
    try:
        cursor.execute(query, (
            dados['data'],
            dados['placa'].upper(),
            dados['responsavel'],
            round(float(dados['litros']), 3),
            round(float(dados['desconto']), 2),
            round(float(dados['odometro']), 1) if dados['odometro'] else None,
            dados['centro_custo'],
            dados['combustivel'],
            round(float(dados['custo_por_litro']), 3),
            round(float(dados['custo_bruto']), 2),
            round(float(dados['custo_liquido']), 2),
            dados.get('posto', ''),
            km_litro
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao criar abastecimento Atheris: {e}")
        return False
    finally:
        conn.close()

def obter_relatorio(data_inicio, data_fim, placa=None, centro_custo=None, combustivel=None, posto=None): 
    """Obtém relatório de abastecimentos com filtros"""
    conn = sqlite3.connect('abastecimentos.db')
    
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

def calcular_medias_veiculos():
    """Calcula médias de consumo por veículo"""
    conn = sqlite3.connect('abastecimentos.db')
    
    # Primeiro precisamos calcular o km_litro para cada abastecimento
    # onde temos odômetro atual e anterior
    query_calculo_km = """
    WITH abastecimentos_ordenados AS (
        SELECT 
            id,
            placa,
            data,
            odometro,
            litros,
            LAG(odometro) OVER (PARTITION BY placa ORDER BY data) as odometro_anterior
        FROM abastecimentos
        WHERE odometro IS NOT NULL
    ),
    abastecimentos_com_km AS (
        SELECT 
            id,
            placa,
            data,
            odometro,
            odometro_anterior,
            litros,
            CASE 
                WHEN odometro_anterior IS NOT NULL AND odometro > odometro_anterior 
                THEN odometro - odometro_anterior 
                ELSE NULL 
            END as km_rodados,
            CASE 
                WHEN odometro_anterior IS NOT NULL AND odometro > odometro_anterior AND litros > 0
                THEN (odometro - odometro_anterior) / litros
                ELSE NULL 
            END as km_litro_calculado
        FROM abastecimentos_ordenados
    )
    UPDATE abastecimentos
    SET km_rodados = (
        SELECT km_rodados 
        FROM abastecimentos_com_km 
        WHERE abastecimentos_com_km.id = abastecimentos.id
    ),
    km_litro = (
        SELECT km_litro_calculado 
        FROM abastecimentos_com_km 
        WHERE abastecimentos_com_km.id = abastecimentos.id
    )
    WHERE EXISTS (
        SELECT 1 
        FROM abastecimentos_com_km 
        WHERE abastecimentos_com_km.id = abastecimentos.id
    )
    """
    
    cursor = conn.cursor()
    try:
        # Primeiro atualiza os cálculos de km/litro
        cursor.execute(query_calculo_km)
        conn.commit()
    except Exception as e:
        print(f"Erro ao calcular km/litro: {e}")
        conn.rollback()
    
    # Agora busca as médias por veículo
    query_medias = """
    SELECT 
        placa,
        COUNT(*) as total_abastecimentos,
        AVG(litros) as media_litros,
        AVG(km_litro) as media_kml,
        SUM(custo_bruto) as total_gasto,
        MAX(odometro) as km_atual,
        SUM(litros) as total_litros
    FROM abastecimentos
    WHERE km_litro IS NOT NULL
    GROUP BY placa
    ORDER BY media_kml DESC
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
    """Obtém um registro específico pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
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

def obter_opcoes_filtro(coluna):
    """Obtém opções para filtros dinâmicos"""
    conn = sqlite3.connect('abastecimentos.db')
    query = f"SELECT DISTINCT {coluna} FROM abastecimentos WHERE {coluna} IS NOT NULL AND {coluna} != '' ORDER BY {coluna}"
    try:
        resultados = pd.read_sql(query, conn)
        return resultados[coluna].tolist()
    except Exception as e:
        print(f"Erro ao obter opções de filtro para {coluna}: {e}")
        return []
    finally:
        conn.close()

def excluir_registro(id):
    """Exclui um registro pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
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

def atualizar_registro(id, dados):
    """Atualiza um registro existente"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    # Calcular km_litro se possível
    km_litro = None
    if dados.get('odometro'):
        # Buscar o último odômetro deste veículo (excluindo o registro atual)
        cursor.execute(
            "SELECT MAX(odometro) FROM abastecimentos WHERE placa = ? AND odometro IS NOT NULL AND id != ?",
            (dados['placa'].upper(), id)
        )
        ultimo_odometro = cursor.fetchone()[0]
        
        if ultimo_odometro and dados['odometro'] > ultimo_odometro and dados['litros'] > 0:
            km_rodados = dados['odometro'] - ultimo_odometro
            km_litro = km_rodados / dados['litros']
    
    query = """
    UPDATE abastecimentos SET
        data = ?,
        placa = ?,
        responsavel = ?,
        litros = ?,
        desconto = ?,
        odometro = ?,
        centro_custo = ?,
        combustivel = ?,
        custo_por_litro = ?,
        custo_bruto = ?,
        custo_liquido = ?,
        posto = ?,
        km_litro = ?
    WHERE id = ?
    """
    
    try:
        cursor.execute(query, (
            dados['data'],
            dados['placa'].upper(),
            dados['responsavel'],
            round(float(dados['litros']), 3),
            round(float(dados['desconto']), 2),
            round(float(dados['odometro']), 1) if dados['odometro'] else None,
            dados['centro_custo'],
            dados['combustivel'],
            round(float(dados['custo_por_litro']), 3),
            round(float(dados['custo_bruto']), 2),
            round(float(dados['custo_liquido']), 2),
            dados.get('posto', ''),
            km_litro,
            id
        ))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def criar_registro(dados):
    """Cria um novo registro"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    # Calcular km_litro se possível
    km_litro = None
    if dados.get('odometro'):
        # Buscar o último odômetro deste veículo
        cursor.execute(
            "SELECT MAX(odometro) FROM abastecimentos WHERE placa = ? AND odometro IS NOT NULL",
            (dados['placa'].upper(),)
        )
        ultimo_odometro = cursor.fetchone()[0]
        
        if ultimo_odometro and dados['odometro'] > ultimo_odometro and dados['litros'] > 0:
            km_rodados = dados['odometro'] - ultimo_odometro
            km_litro = km_rodados / dados['litros']
    
    query = """
    INSERT INTO abastecimentos (
        data, placa, responsavel, litros, desconto, odometro,
        centro_custo, combustivel, custo_por_litro, custo_bruto, custo_liquido, posto, km_litro
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor.execute(query, (
            dados['data'],
            dados['placa'].upper(),
            dados['responsavel'],
            round(float(dados['litros']), 3),
            round(float(dados['desconto']), 2),
            round(float(dados['odometro']), 1) if dados['odometro'] else None,
            dados['centro_custo'],
            dados['combustivel'],
            round(float(dados['custo_por_litro']), 3),
            round(float(dados['custo_bruto']), 2),
            round(float(dados['custo_liquido']), 2),
            dados.get('posto', ''),
            km_litro
        ))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        conn.close()
        raise e
    finally:
        conn.close()

def salvar_troca_oleo(identificacao, tipo, data_troca, km_troca=None, horimetro_troca=None):
    """Salva ou atualiza dados de troca de óleo (FUNÇÃO ATUALIZADA)"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    try:
        # Calcular próximas trocas baseadas no tipo
        if tipo == 'veiculo':
            if km_troca is None:
                raise ValueError("KM da troca é obrigatório para veículos")
            proxima_troca_km = km_troca + 10000
            proxima_troca_horimetro = None
        else:  # maquina
            if horimetro_troca is None:
                raise ValueError("Horímetro da troca é obrigatório para máquinas")
            proxima_troca_km = None
            proxima_troca_horimetro = horimetro_troca + 350
        
        # Verificar se já existe registro para esta identificação e tipo
        cursor.execute('SELECT id FROM trocas_oleo WHERE identificacao = ? AND tipo = ?', (identificacao, tipo))
        existing = cursor.fetchone()
        
        if existing:
            # Atualizar registro existente
            cursor.execute('''
                UPDATE trocas_oleo 
                SET data_troca = ?, km_troca = ?, horimetro_troca = ?, 
                    proxima_troca_km = ?, proxima_troca_horimetro = ?
                WHERE identificacao = ? AND tipo = ?
            ''', (data_troca, km_troca, horimetro_troca, 
                  proxima_troca_km, proxima_troca_horimetro, identificacao, tipo))
        else:
            # Inserir novo registro
            cursor.execute('''
                INSERT INTO trocas_oleo (identificacao, tipo, data_troca, km_troca, horimetro_troca, 
                                        proxima_troca_km, proxima_troca_horimetro)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (identificacao, tipo, data_troca, km_troca, horimetro_troca, 
                  proxima_troca_km, proxima_troca_horimetro))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao salvar troca de óleo: {e}")
        return False
    finally:
        conn.close()

def obter_troca_oleo_por_identificacao_tipo(identificacao, tipo):
    """Obtém uma troca de óleo específica por identificação e tipo"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = """
        SELECT identificacao, tipo, data_troca, km_troca, horimetro_troca, 
               proxima_troca_km, proxima_troca_horimetro
        FROM trocas_oleo 
        WHERE identificacao = ? AND tipo = ?
        """
        df = pd.read_sql(query, conn, params=(identificacao, tipo))
        
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Erro ao obter troca de óleo: {e}")
        return None
    finally:
        conn.close()

def atualizar_troca_oleo(identificacao, tipo, data_troca, km_troca=None, horimetro_troca=None):
    """Atualiza dados de troca de óleo"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    try:
        # Calcular próximas trocas baseadas no tipo
        if tipo == 'veiculo':
            if km_troca is None:
                raise ValueError("KM da troca é obrigatório para veículos")
            proxima_troca_km = km_troca + 10000
            proxima_troca_horimetro = None
        else:  # maquina
            if horimetro_troca is None:
                raise ValueError("Horímetro da troca é obrigatório para máquinas")
            proxima_troca_km = None
            proxima_troca_horimetro = horimetro_troca + 350
        
        cursor.execute('''
            UPDATE trocas_oleo 
            SET data_troca = ?, km_troca = ?, horimetro_troca = ?, 
                proxima_troca_km = ?, proxima_troca_horimetro = ?
            WHERE identificacao = ? AND tipo = ?
        ''', (data_troca, km_troca, horimetro_troca, 
              proxima_troca_km, proxima_troca_horimetro, identificacao, tipo))
        
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao atualizar troca de óleo: {e}")
        return False
    finally:
        conn.close()

def obter_trocas_oleo():
    """Obtém todas as trocas de óleo com informações atuais (FUNÇÃO ATUALIZADA)"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = """
        SELECT identificacao, tipo, data_troca, km_troca, horimetro_troca, 
               proxima_troca_km, proxima_troca_horimetro
        FROM trocas_oleo 
        ORDER BY tipo, identificacao
        """
        df_trocas = pd.read_sql(query, conn)
        
        trocas = []
        
        for _, troca in df_trocas.iterrows():
            identificacao = troca['identificacao']
            tipo = troca['tipo']
            data_troca = troca['data_troca']
            km_troca = troca['km_troca']
            horimetro_troca = troca['horimetro_troca']
            proxima_troca_km = troca['proxima_troca_km']
            proxima_troca_horimetro = troca['proxima_troca_horimetro']
            
            # Buscar valores atuais baseados no tipo
            valor_atual = None
            proxima_troca = None
            remanescente = None
            
            if tipo == 'veiculo':
                # Buscar KM atual (do último abastecimento)
                query_km = """
                SELECT MAX(odometro) FROM abastecimentos 
                WHERE placa = ? AND odometro IS NOT NULL
                """
                cursor = conn.cursor()
                cursor.execute(query_km, (identificacao,))
                valor_atual = cursor.fetchone()[0]
                proxima_troca = proxima_troca_km
                
                if valor_atual and proxima_troca:
                    remanescente = proxima_troca - valor_atual
                elif km_troca and proxima_troca:
                    remanescente = proxima_troca - km_troca
            else:  # maquina
                # Buscar horímetro atual (do último checklist)
                query_horimetro = """
                SELECT MAX(horimetro) FROM checklists 
                WHERE identificacao = ? AND horimetro IS NOT NULL
                """
                cursor = conn.cursor()
                cursor.execute(query_horimetro, (identificacao,))
                valor_atual = cursor.fetchone()[0]
                proxima_troca = proxima_troca_horimetro
                
                if valor_atual and proxima_troca:
                    remanescente = proxima_troca - valor_atual
                elif horimetro_troca and proxima_troca:
                    remanescente = proxima_troca - horimetro_troca
            
            # Determinar status baseado no tipo e valores remanescentes
            status = 'OK'
            
            if tipo == 'veiculo':
                if remanescente is not None and remanescente <= 0:
                    status = 'VENCIDO'
                elif remanescente is not None and remanescente <= 1000:
                    status = 'ATENÇÃO'
            else:  # maquina
                if remanescente is not None and remanescente <= 0:
                    status = 'VENCIDO'
                elif remanescente is not None and remanescente <= 50:
                    status = 'ATENÇÃO'
            
            trocas.append({
                'identificacao': identificacao,
                'tipo': tipo,
                'data_troca': data_troca,
                'km_troca': km_troca,
                'horimetro_troca': horimetro_troca,
                'km_atual': valor_atual if tipo == 'veiculo' else None,
                'horimetro_atual': valor_atual if tipo == 'maquina' else None,
                'proxima_troca': proxima_troca,
                'remanescente': remanescente,
                'status': status
            })
        
        # Ordenar por status (vencidos primeiro, depois atenção, depois OK)
        ordem_status = {'VENCIDO': 0, 'ATENÇÃO': 1, 'OK': 2}
        trocas.sort(key=lambda x: (ordem_status[x['status']], 
                                  x['remanescente'] if x['remanescente'] is not None else float('inf')))
        
        return trocas
    except Exception as e:
        print(f"Erro ao obter trocas de óleo: {e}")
        return []
    finally:
        conn.close()

def obter_placas_veiculos():
    """Obtém todas as placas de veículos cadastradas"""
    conn = sqlite3.connect('abastecimentos.db')
    
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

def criar_checklist(dados):
    """Cria um novo checklist"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    query = """
    INSERT INTO checklists (identificacao, data, horimetro, nivel_oleo, observacoes, itens_checklist)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor.execute(query, (
            dados['identificacao'],
            dados['data'],
            dados.get('horimetro'),
            dados.get('nivel_oleo', 'ADEQUADO'),
            dados.get('observacoes', ''),
            dados.get('itens_checklist', '')
        ))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"Erro ao criar checklist: {e}")
        return False
    finally:
        conn.close()

def obter_checklists():
    """Obtém todos os checklists"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = "SELECT * FROM checklists ORDER BY data DESC"
        df = pd.read_sql(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter checklists: {e}")
        return []
    finally:
        conn.close()

def obter_checklist_por_id(id):
    """Obtém um checklist específico pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = "SELECT * FROM checklists WHERE id = ?"
        df = pd.read_sql(query, conn, params=(id,))
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Erro ao obter checklist: {e}")
        return None
    finally:
        conn.close()

def atualizar_checklist(id, dados):
    """Atualiza um checklist existente"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    query = """
    UPDATE checklists SET
        identificacao = ?, data = ?, horimetro = ?, 
        nivel_oleo = ?, observacoes = ?, itens_checklist = ?
    WHERE id = ?
    """
    
    try:
        cursor.execute(query, (
            dados['identificacao'],
            dados['data'],
            dados.get('horimetro'),
            dados.get('nivel_oleo', 'ADEQUADO'),
            dados.get('observacoes', ''),
            dados.get('itens_checklist', ''),
            id
        ))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao atualizar checklist: {e}")
        return False
    finally:
        conn.close()

def excluir_checklist(id):
    """Exclui um checklist pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM checklists WHERE id = ?", (id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao excluir checklist: {e}")
        return False
    finally:
        conn.close()

def obter_identificacoes_equipamentos():
    """Obtém todas as identificações de equipamentos dos checklists"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = """
        SELECT DISTINCT identificacao 
        FROM checklists 
        WHERE identificacao IS NOT NULL AND identificacao != ''
        ORDER BY identificacao
        """
        df = pd.read_sql(query, conn)
        return df['identificacao'].tolist()
    except Exception as e:
        print(f"Erro ao obter identificações de equipamentos: {e}")
        return []
    finally:
        conn.close()

def obter_checklists_por_identificacao(identificacao):
    """Obtém todos os checklists relacionados a uma identificação"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = """
        SELECT id, data, horimetro, nivel_oleo, observacoes 
        FROM checklists 
        WHERE identificacao = ?
        ORDER BY data DESC, horimetro DESC
        """
        df = pd.read_sql(query, conn, params=(identificacao,))
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter checklists para identificação {identificacao}: {e}")
        return []
    finally:
        conn.close()

def excluir_troca_oleo(identificacao, tipo):
    """Exclui uma troca de óleo pela identificação e tipo"""
    conn = sqlite3.connect('abastecimentos.db')
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

# NOVAS FUNÇÕES PARA MANUTENÇÕES
def obter_manutencoes():
    """Obtém todas as manutenções"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = "SELECT * FROM manutencoes ORDER BY data_abertura DESC"
        df = pd.read_sql(query, conn)
        return df.to_dict('records')
    except Exception as e:
        print(f"Erro ao obter manutenções: {e}")
        return []
    finally:
        conn.close()

def obter_manutencao_por_id(id):
    """Obtém uma manutenção específica pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        query = "SELECT * FROM manutencoes WHERE id = ?"
        df = pd.read_sql(query, conn, params=(id,))
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except Exception as e:
        print(f"Erro ao obter manutenção: {e}")
        return None
    finally:
        conn.close()

def criar_manutencao(dados):
    """Cria uma nova manutenção"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    query = """
    INSERT INTO manutencoes (
        identificacao, tipo, frota, descricao, fornecedor, valor, 
        data_abertura, previsao_conclusao, data_conclusao, observacoes, 
        finalizada, prazo_liberacao, forma_pagamento, parcelas
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor.execute(query, (
            dados['identificacao'],
            dados['tipo'],
            dados['frota'],
            dados['descricao'],
            dados.get('fornecedor', ''),
            dados.get('valor', 0),
            dados['data_abertura'],
            dados.get('previsao_conclusao', ''),
            dados.get('data_conclusao', ''),
            dados.get('observacoes', ''),
            dados.get('finalizada', False),
            dados.get('prazo_liberacao'),
            dados.get('forma_pagamento', ''),
            dados.get('parcelas', 1)
        ))
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        print(f"Erro ao criar manutenção: {e}")
        return False
    finally:
        conn.close()

def atualizar_manutencao(id, dados):
    """Atualiza uma manutenção existente"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    query = """
    UPDATE manutencoes SET
        identificacao = ?, tipo = ?, frota = ?, descricao = ?, 
        fornecedor = ?, valor = ?, data_abertura = ?, 
        previsao_conclusao = ?, data_conclusao = ?, 
        observacoes = ?, finalizada = ?, prazo_liberacao = ?,
        forma_pagamento = ?, parcelas = ?
    WHERE id = ?
    """
    
    try:
        cursor.execute(query, (
            dados['identificacao'],
            dados['tipo'],
            dados['frota'],
            dados['descricao'],
            dados.get('fornecedor', ''),
            dados.get('valor', 0),
            dados['data_abertura'],
            dados.get('previsao_conclusao', ''),
            dados.get('data_conclusao', ''),
            dados.get('observacoes', ''),
            dados.get('finalizada', False),
            dados.get('prazo_liberacao'),
            dados.get('forma_pagamento', ''),
            dados.get('parcelas', 1),
            id
        ))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        print(f"Erro ao atualizar manutenção: {e}")
        return False
    finally:
        conn.close()

def excluir_manutencao(id):
    """Exclui uma manutenção pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
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

def obter_estatisticas_manutencoes():
    """Obtém estatísticas das manutenções"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as total FROM manutencoes")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) as abertas FROM manutencoes WHERE finalizada = 0")
        abertas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) as finalizadas FROM manutencoes WHERE finalizada = 1")
        finalizadas = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(valor) as valor_total FROM manutencoes")
        valor_total = cursor.fetchone()[0] or 0
        
        return {
            'total': total,
            'abertas': abertas,
            'finalizadas': finalizadas,
            'valor_total': valor_total
        }
    except Exception as e:
        print(f"Erro ao obter estatísticas de manutenções: {e}")
        return {
            'total': 0,
            'abertas': 0,
            'finalizadas': 0,
            'valor_total': 0
        }
    finally:
        conn.close()