from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, jsonify
import pandas as pd
import os
import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
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
    obter_registro_por_id,
    obter_trocas_oleo,
    salvar_troca_oleo,
    obter_placas_veiculos
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
    # Calcular estatísticas para o dashboard
    try:
        # Total de abastecimentos
        conn = sqlite3.connect('abastecimentos.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM abastecimentos")
        total_abastecimentos = cursor.fetchone()[0]
        
        # Total de veículos
        cursor.execute("SELECT COUNT(DISTINCT placa) FROM abastecimentos")
        total_veiculos = cursor.fetchone()[0]
        
        # Total de manutenções
        cursor.execute("SELECT COUNT(*) FROM manutencoes")
        total_manutencoes = cursor.fetchone()[0]
        
        # Gasto total
        cursor.execute("SELECT SUM(custo_liquido) FROM abastecimentos")
        gasto_total = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return render_template('index.html', 
                             active_page='index',
                             total_abastecimentos=total_abastecimentos,
                             total_veiculos=total_veiculos,
                             total_manutencoes=total_manutencoes,
                             gasto_total=gasto_total)
    except Exception as e:
        print(f"Erro ao carregar dados do dashboard: {e}")
        return render_template('index.html', 
                             active_page='index',
                             total_abastecimentos=0,
                             total_veiculos=0,
                             total_manutencoes=0,
                             gasto_total=0)

@app.route('/api/dashboard')
def api_dashboard():
    """API para dados do dashboard"""
    try:
        conn = sqlite3.connect('abastecimentos.db')
        cursor = conn.cursor()
        
        # Total de abastecimentos
        cursor.execute("SELECT COUNT(*) FROM abastecimentos")
        total_abastecimentos = cursor.fetchone()[0]
        
        # Total de veículos
        cursor.execute("SELECT COUNT(DISTINCT placa) FROM abastecimentos")
        total_veiculos = cursor.fetchone()[0]
        
        # Total de manutenções
        cursor.execute("SELECT COUNT(*) FROM manutencoes")
        total_manutencoes = cursor.fetchone()[0]
        
        # Gasto total
        cursor.execute("SELECT SUM(custo_liquido) FROM abastecimentos")
        gasto_total = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_abastecimentos': total_abastecimentos,
            'total_veiculos': total_veiculos,
            'total_manutencoes': total_manutencoes,
            'gasto_total': gasto_total
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

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

@app.route('/manutencoes')
def manutencoes():
    """Página de gerenciamento de manutenções"""
    try:
        # Verificar se a tabela de manutenções existe e criar se não existir
        conn = sqlite3.connect('abastecimentos.db')
        cursor = conn.cursor()
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='manutencoes'
        ''')
        
        if not cursor.fetchone():
            # Criar tabela se não existir
            cursor.execute('''
                CREATE TABLE manutencoes (
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
                    data_registro TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
        
        # Buscar manutenções
        query = "SELECT * FROM manutencoes ORDER BY data_abertura DESC"
        df = pd.read_sql(query, conn)
        manutencoes = df.to_dict('records')
        
        # Estatísticas para a página
        cursor.execute("SELECT COUNT(*) FROM manutencoes")
        total_manutencoes = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM manutencoes WHERE finalizada = 0")
        manutencoes_abertas = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM manutencoes WHERE finalizada = 1")
        manutencoes_finalizadas = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(valor) FROM manutencoes")
        valor_total = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return render_template('manutencoes.html', 
                             active_page='manutencoes',
                             manutencoes=manutencoes,
                             total_manutencoes=total_manutencoes,
                             manutencoes_abertas=manutencoes_abertas,
                             manutencoes_finalizadas=manutencoes_finalizadas,
                             valor_total=valor_total)
    except Exception as e:
        flash(f'Erro ao carregar manutenções: {str(e)}', 'danger')
        return render_template('manutencoes.html', 
                             active_page='manutencoes',
                             manutencoes=[],
                             total_manutencoes=0,
                             manutencoes_abertas=0,
                             manutencoes_finalizadas=0,
                             valor_total=0)

@app.route('/api/manutencoes', methods=['GET', 'POST'])
def api_manutencoes():
    """API para gerenciar manutenções"""
    conn = sqlite3.connect('abastecimentos.db')
    
    if request.method == 'GET':
        # Retornar lista de manutenções
        try:
            query = "SELECT * FROM manutencoes ORDER BY data_abertura DESC"
            df = pd.read_sql(query, conn)
            
            # Calcular estatísticas
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM manutencoes")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM manutencoes WHERE finalizada = 0")
            abertas = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM manutencoes WHERE finalizada = 1")
            finalizadas = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(valor) FROM manutencoes")
            valor_total = cursor.fetchone()[0] or 0
            
            conn.close()
            
            return jsonify({
                'manutencoes': df.to_dict('records'),
                'estatisticas': {
                    'total': total,
                    'abertas': abertas,
                    'finalizadas': finalizadas,
                    'valor_total': valor_total
                }
            })
        except Exception as e:
            conn.close()
            return jsonify({'success': False, 'error': str(e)}), 500
    
    elif request.method == 'POST':
        # Criar nova manutenção
        dados = request.get_json()
        
        query = '''
            INSERT INTO manutencoes 
            (identificacao, tipo, frota, descricao, fornecedor, valor, 
             data_abertura, previsao_conclusao, data_conclusao, observacoes, finalizada)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        try:
            cursor = conn.cursor()
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
                dados.get('finalizada', False)
            ))
            conn.commit()
            manutencao_id = cursor.lastrowid
            conn.close()
            
            return jsonify({'success': True, 'id': manutencao_id})
        except Exception as e:
            conn.close()
            return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/manutencoes/<int:id>', methods=['GET', 'PUT', 'DELETE'])
def api_manutencao(id):
    """API para gerenciar uma manutenção específica"""
    conn = sqlite3.connect('abastecimentos.db')
    
    if request.method == 'GET':
        # Buscar manutenção específica
        try:
            query = "SELECT * FROM manutencoes WHERE id = ?"
            df = pd.read_sql(query, conn, params=(id,))
            conn.close()
            
            if not df.empty:
                return jsonify(df.iloc[0].to_dict())
            return jsonify({'error': 'Manutenção não encontrada'}), 404
        except Exception as e:
            conn.close()
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        # Atualizar manutenção
        dados = request.get_json()
        
        query = '''
            UPDATE manutencoes SET
                identificacao = ?, tipo = ?, frota = ?, descricao = ?, 
                fornecedor = ?, valor = ?, data_abertura = ?, 
                previsao_conclusao = ?, data_conclusao = ?, 
                observacoes = ?, finalizada = ?
            WHERE id = ?
        '''
        
        try:
            cursor = conn.cursor()
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
                id
            ))
            conn.commit()
            conn.close()
            
            return jsonify({'success': True})
        except Exception as e:
            conn.close()
            return jsonify({'success': False, 'error': str(e)}), 400
    
    elif request.method == 'DELETE':
        # Excluir manutenção
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM manutencoes WHERE id = ?", (id,))
            conn.commit()
            conn.close()
            
            if cursor.rowcount > 0:
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Manutenção não encontrada'}), 404
        except Exception as e:
            conn.close()
            return jsonify({'success': False, 'error': str(e)}), 400

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
            
            if salvar_troca_oleo(placa, data_troca, km_troca):
                flash('Dados de troca de óleo salvos com sucesso!', 'success')
            else:
                flash('Erro ao salvar dados de troca de óleo', 'danger')
            
        except Exception as e:
            flash(f'Erro ao salvar dados: {str(e)}', 'danger')
        
        return redirect(url_for('metricas_uso'))
    
    # Obter dados para exibição
    trocas_oleo = obter_trocas_oleo()
    placas = obter_placas_veiculos()
    
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