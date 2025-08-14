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
    
    query = '''
    INSERT INTO abastecimentos (
        data, placa, responsavel, litros, desconto, odometro,
        centro_custo, combustivel, custo_por_litro, custo_bruto, 
        custo_liquido, integracao_atheris
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
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
            round(float(dados['custo_liquido']), 2)
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao criar abastecimento Atheris: {e}")
        return False
    finally:
        conn.close()

def importar_dados(df):
    """Importa dados de um DataFrame para o banco de dados"""
    conn = sqlite3.connect('abastecimentos.db')
    
    try:
        # Padroniza nomes de colunas
        df.columns = df.columns.str.strip().str.upper()
        
        # Mapeamento de colunas
        col_mapping = {
            'DATA': ['DATA', 'DATE'],
            'PLACA': ['PLACA', 'VEÍCULO', 'VEICULO'],
            'RESPONSAVEL': ['RESPONSÁVEL', 'RESPONSAVEL', 'MOTORISTA', 'OPERADOR'],
            'LITROS': ['LITROS', 'QUANTIDADE', 'VOLUME'],
            'DESCONTO': ['DESCONTO', 'DESC'],
            'ODOMETRO': ['ODÔMETRO', 'ODOMETRO', 'KM', 'QUILOMETRAGEM'],
            'CENTRO_CUSTO': ['CENTRO DE CUSTO', 'CENTRO CUSTO', 'CENTRO_CUSTO', 'DEPARTAMENTO'],
            'COMBUSTIVEL': ['COMBUSTÍVEL', 'COMBUSTIVEL', 'TIPO', 'PRODUTO'],
            'CUSTO_POR_LITRO': ['CUSTO POR LITRO', 'VALOR/LITRO', 'PRECO/LITRO', 'CUSTO_POR_LITRO'],
            'CUSTO_BRUTO': ['CUSTO BRUTO', 'TOTAL BRUTO', 'VALOR BRUTO', 'CUSTO_BRUTO'],
            'CUSTO_LIQUIDO': ['CUSTO LÍQ', 'CUSTO LIQUIDO', 'TOTAL LIQUIDO', 'VALOR LIQUIDO', 'CUSTO_LIQUIDO']
        }
        
        # Mapeia colunas existentes
        colunas_existentes = {}
        for col_db, alternativas in col_mapping.items():
            for alt in alternativas:
                if alt in df.columns:
                    colunas_existentes[col_db] = alt
                    break
        
        # Verifica colunas obrigatórias
        colunas_obrigatorias = ['DATA', 'PLACA', 'LITROS', 'CUSTO_POR_LITRO', 'CUSTO_BRUTO', 'CUSTO_LIQUIDO']
        for col in colunas_obrigatorias:
            if col not in colunas_existentes:
                raise ValueError(f"Coluna obrigatória não encontrada: {col}")
        
        # Renomeia colunas
        df = df.rename(columns={v: k for k, v in colunas_existentes.items()})
        
        # Preenche colunas opcionais faltantes
        for col in ['RESPONSAVEL', 'DESCONTO', 'ODOMETRO', 'CENTRO_CUSTO', 'COMBUSTIVEL']:
            if col not in df.columns:
                df[col] = None if col == 'RESPONSAVEL' else 0
        
        # Converte tipos de dados
        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce').dt.strftime('%Y-%m-%d')
        for col in ['LITROS', 'DESCONTO', 'ODOMETRO', 'CUSTO_POR_LITRO', 'CUSTO_BRUTO', 'CUSTO_LIQUIDO']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Trata valores nulos
        df['LITROS'] = df['LITROS'].fillna(0)
        df['CUSTO_POR_LITRO'] = df['CUSTO_POR_LITRO'].fillna(0)
        df['CUSTO_BRUTO'] = df['CUSTO_BRUTO'].fillna(0)
        df['CUSTO_LIQUIDO'] = df['CUSTO_LIQUIDO'].fillna(0)
        
        # Valida odômetro
        df['ODOMETRO'] = df.apply(lambda x: x['ODOMETRO'] if pd.notna(x['ODOMETRO']) and x['ODOMETRO'] > 0 else None, axis=1)
        
        # Calcula KM/L
        df = df.sort_values(['PLACA', 'DATA'])
        df['ODOMETRO_ANTERIOR'] = df.groupby('PLACA')['ODOMETRO'].shift(1)
        df['KM_LITRO'] = df.apply(lambda x: (x['ODOMETRO'] - x['ODOMETRO_ANTERIOR'])/x['LITROS'] 
                              if pd.notna(x['ODOMETRO']) and pd.notna(x['ODOMETRO_ANTERIOR']) 
                              and x['ODOMETRO'] > x['ODOMETRO_ANTERIOR'] and x['LITROS'] > 0 
                              else None, axis=1)
        
        # Padroniza nomes de colunas para minúsculas
        df.columns = df.columns.str.lower()
        
        # Importa para o banco de dados
        df.to_sql('abastecimentos', conn, if_exists='append', index=False)
        
    except Exception as e:
        conn.close()
        raise e
    
    conn.close()

def obter_relatorio(data_inicio, data_fim, placa=None, centro_custo=None, combustivel=None): 
    """Obtém relatório de abastecimentos com filtros"""
    conn = sqlite3.connect('abastecimentos.db')
    
    query = """
    SELECT 
        id, data, placa, responsavel, litros, desconto, odometro,
        centro_custo, combustivel, custo_por_litro, custo_bruto, 
        custo_liquido, km_litro, integracao_atheris
    FROM abastecimentos
    WHERE data BETWEEN ? AND ?
    """
    
    params = [data_inicio, data_fim]
    
    conditions = []
    if placa:
        conditions.append("placa = ?")
        params.append(placa)
    if centro_custo:
        conditions.append("centro_custo = ?")
        params.append(centro_custo)
    if combustivel:
        conditions.append("combustivel = ?")
        params.append(combustivel)
    
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
    
    query = """
    SELECT 
        placa,
        COUNT(*) as total_abastecimentos,
        AVG(litros) as media_litros,
        AVG(km_litro) as media_kml,
        SUM(custo_bruto) as total_gasto,
        MAX(odometro) as km_atual
    FROM abastecimentos
    WHERE km_litro IS NOT NULL
    GROUP BY placa
    ORDER BY media_kml DESC
    """
    
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        conn.close()
        raise e
    
    conn.close()
    return df.to_dict('records')

def obter_opcoes_filtro(coluna):
    """Obtém opções para filtros dinâmicos"""
    conn = sqlite3.connect('abastecimentos.db')
    query = f"SELECT DISTINCT {coluna} FROM abastecimentos WHERE {coluna} IS NOT NULL ORDER BY {coluna}"
    resultados = pd.read_sql(query, conn)
    conn.close()
    return resultados[coluna].tolist()

def excluir_registro(id):
    """Exclui um registro pelo ID"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM abastecimentos WHERE id = ?", (id,))
        conn.commit()
    except Exception as e:
        conn.close()
        raise e
    conn.close()

def atualizar_registro(id, dados):
    """Atualiza um registro existente"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
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
        custo_liquido = ?
    WHERE id = ?
    """
    
    try:
        cursor.execute(query, (
            dados['data'],
            dados['placa'],
            dados['responsavel'],
            dados['litros'],
            dados['desconto'],
            dados['odometro'],
            dados['centro_custo'],
            dados['combustivel'],
            dados['custo_por_litro'],
            dados['custo_bruto'],
            dados['custo_liquido'],
            id
        ))
        conn.commit()
    except Exception as e:
        conn.close()
        raise e
    
    conn.close()

def criar_registro(dados):
    """Cria um novo registro"""
    conn = sqlite3.connect('abastecimentos.db')
    cursor = conn.cursor()
    
    query = """
    INSERT INTO abastecimentos (
        data, placa, responsavel, litros, desconto, odometro,
        centro_custo, combustivel, custo_por_litro, custo_bruto, custo_liquido
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor.execute(query, (
            dados['data'],
            dados['placa'],
            dados['responsavel'],
            dados['litros'],
            dados['desconto'],
            dados['odometro'],
            dados['centro_custo'],
            dados['combustivel'],
            dados['custo_por_litro'],
            dados['custo_bruto'],
            dados['custo_liquido']
        ))
        conn.commit()
    except Exception as e:
        conn.close()
        raise e
    
    conn.close()