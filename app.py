from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, jsonify
import pandas as pd
import os
import sqlite3    
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime
from database import ( 
    criar_tabelas, 
    obter_relatorio,
    calcular_medias_veiculos,
    obter_precos_combustivel,
    atualizar_preco_combustivel,
    criar_combustivel,
    criar_abastecimento_atheris,
    obter_opcoes_filtro,
    excluir_registro,
    atualizar_registro,
    criar_registro,
    obter_registro_por_id
)

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_123'
app.config['STATIC_FOLDER'] = 'static'

# Configuração de pastas
os.makedirs(app.config['STATIC_FOLDER'], exist_ok=True)

def gerar_grafico(df, x_col, y_cols, title, tipo='bar', filename='grafico.png'):
    plt.figure(figsize=(10, 5))
    
    if tipo == 'bar':
        df.plot(x=x_col, y=y_cols, kind='bar')
    else:
        for col in y_cols:
            plt.plot(df[x_col], df[col], marker='o', label=col)
    
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    grafico_path = os.path.join(app.config['STATIC_FOLDER'], filename)
    plt.savefig(grafico_path)
    plt.close()
    
    return grafico_path

@app.route('/')
def index():
    return render_template('index.html', active_page='index')

@app.route('/relatorios', methods=['GET', 'POST'])
def relatorios():
    # Obter preços de combustível
    precos_combustivel = obter_precos_combustivel()
    
    if request.method == 'POST':
        filtros = {
            'data_inicio': request.form.get('data_inicio', '2023-01-01'),
            'data_fim': request.form.get('data_fim', datetime.now().strftime('%Y-%m-%d')),
            'placa': request.form.get('placa', '').strip() or None,
            'centro_custo': request.form.get('centro_custo', '').strip() or None,
            'combustivel': request.form.get('combustivel', '').strip() or None,
            'posto': request.form.get('posto', '').strip() or None
        }
        
        try:
            df = obter_relatorio(**filtros)
            
            if request.form.get('imprimir'):
                return render_template(
                    'relatorio_impressao.html',
                    dados=df.to_dict('records'),
                    filtros=filtros,
                    data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M')
                )
            
            return render_template(
                'relatorios.html',
                dados=df.to_dict('records'),
                filtros=filtros,
                opcoes_centro_custo=obter_opcoes_filtro('centro_custo'),
                opcoes_combustivel=obter_opcoes_filtro('combustivel'),
                opcoes_posto=obter_opcoes_filtro('posto'),
                precos_combustivel=precos_combustivel,
                active_page='relatorios'
            )
        except Exception as e:
            flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
            return render_template('relatorios.html',
                                 opcoes_centro_custo=obter_opcoes_filtro('centro_custo'),
                                 opcoes_combustivel=obter_opcoes_filtro('combustivel'),
                                 opcoes_posto=obter_opcoes_filtro('posto'),
                                 precos_combustivel=precos_combustivel,
                                 active_page='relatorios')
    
    return render_template('relatorios.html',
                         opcoes_centro_custo=obter_opcoes_filtro('centro_custo'),
                         opcoes_combustivel=obter_opcoes_filtro('combustivel'),
                         opcoes_posto=obter_opcoes_filtro('posto'),
                         precos_combustivel=precos_combustivel,
                         active_page='relatorios')

@app.route('/medias-veiculos')
def medias_veiculos():
    try:
        dados = calcular_medias_veiculos()
        df = pd.DataFrame(dados)
        if not df.empty:
            gerar_grafico(df, 'placa', ['media_kml'], 'Média de KM/L por Veículo', 'bar', 'grafico_kml.png')
        return render_template('medias_veiculos.html', dados=dados, active_page='medias')
    except Exception as e:
        flash(f'Erro ao calcular médias: {str(e)}', 'danger')
        return render_template('medias_veiculos.html', dados=[], active_page='medias')

@app.route('/medias-veiculos-dados')
def medias_veiculos_dados():
    try:
        dados = calcular_medias_veiculos()
        return jsonify(dados)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/metricas-uso', methods=['GET', 'POST'])
def metricas_uso():
    """Página de métricas de uso e controle de troca de óleo"""
    if request.method == 'POST':
        try:
            placa = request.form.get('placa').upper()
            data_troca = request.form.get('data_troca')
            km_troca = float(request.form.get('km_troca'))
            proxima_troca_km = km_troca + 10000
            
            # Salvar/atualizar dados de troca de óleo
            conn = sqlite3.connect('abastecimentos.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trocas_oleo (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    placa TEXT NOT NULL,
                    data_troca TEXT NOT NULL,
                    km_troca REAL NOT NULL,
                    proxima_troca_km REAL NOT NULL,
                    data_registro TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Verificar se já existe registro para esta placa
            cursor.execute('SELECT id FROM trocas_oleo WHERE placa = ?', (placa,))
            existing = cursor.fetchone()
            
            if existing:
                cursor.execute('''
                    UPDATE trocas_oleo 
                    SET data_troca = ?, km_troca = ?, proxima_troca_km = ?
                    WHERE placa = ?
                ''', (data_troca, km_troca, proxima_troca_km, placa))
            else:
                cursor.execute('''
                    INSERT INTO trocas_oleo (placa, data_troca, km_troca, proxima_troca_km)
                    VALUES (?, ?, ?, ?)
                ''', (placa, data_troca, km_troca, proxima_troca_km))
            
            conn.commit()
            conn.close()
            
            flash('Dados de troca de óleo salvos com sucesso!', 'success')
            
        except Exception as e:
            flash(f'Erro ao salvar dados: {str(e)}', 'danger')
        
        return redirect(url_for('metricas_uso'))
    
    # Obter dados para exibição
    conn = sqlite3.connect('abastecimentos.db')
    
    # Obter últimas trocas de óleo
    cursor = conn.cursor()
    cursor.execute('''
        SELECT t.placa, t.data_troca, t.km_troca, t.proxima_troca_km,
               MAX(a.odometro) as km_atual,
               (t.proxima_troca_km - MAX(a.odometro)) as km_remanescentes
        FROM trocas_oleo t
        LEFT JOIN abastecimentos a ON t.placa = a.placa
        GROUP BY t.placa
        ORDER BY km_remanescentes ASC
    ''')
    
    trocas_oleo = []
    for row in cursor.fetchall():
        trocas_oleo.append({
            'placa': row[0],
            'data_troca': row[1],
            'km_troca': row[2],
            'proxima_troca_km': row[3],
            'km_atual': row[4] if row[4] else row[2],  # Se não há abastecimento, usa km_troca
            'km_remanescentes': row[5] if row[5] is not None else (row[3] - row[2])
        })
    
    # Obter placas disponíveis
    cursor.execute("SELECT DISTINCT placa FROM abastecimentos WHERE placa IS NOT NULL ORDER BY placa")
    placas = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return render_template('metricas_uso.html', 
                         trocas_oleo=trocas_oleo,
                         placas=placas,
                         active_page='metricas')

@app.route('/reajuste-combustiveis', methods=['GET', 'POST'])
def reajuste_combustiveis():
    if request.method == 'POST':
        try:
            if 'novo_combustivel' in request.form:
                # Cadastro de novo combustível
                combustivel = request.form.get('novo_combustivel').strip().upper()
                preco = request.form.get('novo_preco')
                
                if criar_combustivel(combustivel, preco):
                    flash(f'Combustível {combustivel} cadastrado com sucesso!', 'success')
                else:
                    flash(f'Combustível {combustivel} já existe!', 'warning')
            else:
                # Atualização de preço existente
                combustivel = request.form.get('combustivel')
                novo_preco = request.form.get('novo_preco')
                
                if atualizar_preco_combustivel(combustivel, novo_preco):
                    flash(f'Preço do {combustivel} atualizado com sucesso!', 'success')
                else:
                    flash('Erro ao atualizar preço', 'danger')
        except Exception as e:
            flash(f'Erro no processamento: {str(e)}', 'danger')
        
        return redirect(url_for('reajuste_combustiveis'))
    
    precos = obter_precos_combustivel()
    return render_template('reajuste_combustiveis.html', precos=precos, active_page='combustiveis')

@app.route('/api/registros', methods=['POST'])
def criar_registro_api():
    try:
        dados = request.get_json()
        
        # Validar dados obrigatórios
        if not all([dados.get('data'), dados.get('placa'), dados.get('combustivel'), 
                   dados.get('litros'), dados.get('custo_por_litro')]):
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando'}), 400
        
        # Calcular valores com arredondamento
        litros = float(dados['litros'])
        custo_por_litro = float(dados['custo_por_litro'])
        desconto = float(dados.get('desconto', 0))
        
        custo_bruto = round(litros * custo_por_litro, 2)
        custo_liquido = round(custo_bruto - desconto, 2)
        
        # Adicionar valores calculados
        dados['custo_bruto'] = custo_bruto
        dados['custo_liquido'] = custo_liquido
        
        id = criar_registro(dados)
        return jsonify({'success': True, 'id': id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/registros/<int:id>', methods=['GET', 'PUT', 'DELETE'])
def gerenciar_registro(id):
    if request.method == 'GET':
        try:
            registro = obter_registro_por_id(id)
            if registro:
                return jsonify({'success': True, 'data': registro})
            return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
            
    elif request.method == 'PUT':
        try:
            dados = request.get_json()
            
            # Validar dados obrigatórios
            if not all([dados.get('data'), dados.get('placa'), dados.get('combustivel'), 
                       dados.get('litros'), dados.get('custo_por_litro')]):
                return jsonify({'success': False, 'error': 'Dados obrigatórios faltando'}), 400
            
            # Calcular valores com arredondamento
            litros = float(dados['litros'])
            custo_por_litro = float(dados['custo_por_litro'])
            desconto = float(dados.get('desconto', 0))
            
            custo_bruto = round(litros * custo_por_litro, 2)
            custo_liquido = round(custo_bruto - desconto, 2)
            
            # Adicionar valores calculados
            dados['custo_bruto'] = custo_bruto
            dados['custo_liquido'] = custo_liquido
            
            if atualizar_registro(id, dados):
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Nenhum registro atualizado'}), 404
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
            
    elif request.method == 'DELETE':
        try:
            if excluir_registro(id):
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Nenhum registro excluído'}), 404
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory(app.config['STATIC_FOLDER'], filename)

@app.context_processor
def inject_now():
    return {'now': datetime.now()}

if __name__ == '__main__':
    criar_tabelas()
    app.run(debug=True, host='0.0.0.0', port=5000)