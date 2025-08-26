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