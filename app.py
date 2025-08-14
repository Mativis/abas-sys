from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, jsonify
import pandas as pd
import os
from werkzeug.utils import secure_filename
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime
from database import (
    criar_tabelas,
    importar_dados,
    obter_relatorio,
    calcular_medias_veiculos,
    obter_precos_combustivel,
    atualizar_preco_combustivel,
    criar_combustivel,
    criar_abastecimento_atheris,
    obter_opcoes_filtro,
    excluir_registro,
    atualizar_registro,
    criar_registro
)

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_123'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['ALLOWED_EXTENSIONS'] = {'xlsx', 'csv'}
app.config['STATIC_FOLDER'] = 'static'

# Configuração de pastas
for folder in [app.config['UPLOAD_FOLDER'], app.config['STATIC_FOLDER']]:
    os.makedirs(folder, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

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
    return render_template('index.html')

@app.route('/importar', methods=['GET', 'POST'])
def importar():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Nenhum arquivo enviado', 'danger')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            flash('Nenhum arquivo selecionado', 'warning')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            try:
                if filename.endswith('.xlsx'):
                    df = pd.read_excel(filepath, engine='openpyxl')
                else:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        first_line = f.readline()
                        delimiter = ';' if ';' in first_line else ','
                    df = pd.read_csv(filepath, encoding='utf-8', delimiter=delimiter)
                
                importar_dados(df)
                flash('Dados importados com sucesso!', 'success')
            except Exception as e:
                flash(f'Erro ao importar arquivo: {str(e)}', 'danger')
            finally:
                if os.path.exists(filepath):
                    os.remove(filepath)
            
            return redirect(url_for('index'))
        
        flash('Tipo de arquivo não permitido. Use .xlsx ou .csv', 'danger')
    
    return render_template('importar.html')

@app.route('/relatorios', methods=['GET', 'POST'])
def relatorios():
    if request.method == 'POST':
        filtros = {
            'data_inicio': request.form.get('data_inicio', '2023-01-01'),
            'data_fim': request.form.get('data_fim', datetime.now().strftime('%Y-%m-%d')),
            'placa': request.form.get('placa', '').strip() or None,
            'centro_custo': request.form.get('centro_custo', '').strip() or None,
            'combustivel': request.form.get('combustivel', '').strip() or None
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
                opcoes_combustivel=obter_opcoes_filtro('combustivel')
            )
        except Exception as e:
            flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
    
    return render_template('relatorios.html',
                         opcoes_centro_custo=obter_opcoes_filtro('centro_custo'),
                         opcoes_combustivel=obter_opcoes_filtro('combustivel'))

@app.route('/medias-veiculos')
def medias_veiculos():
    dados = calcular_medias_veiculos()
    df = pd.DataFrame(dados)
    if not df.empty:
        gerar_grafico(df, 'placa', ['media_kml'], 'Média de KM/L por Veículo', 'bar', 'grafico_kml.png')
    return render_template('medias_veiculos.html', dados=dados)

@app.route('/integracao-atheris', methods=['GET', 'POST'])
def integracao_atheris():
    precos = obter_precos_combustivel()
    
    if request.method == 'POST':
        try:
            combustivel_selecionado = request.form.get('combustivel')
            preco_padrao = next(
                (p['preco'] for p in precos if p['combustivel'] == combustivel_selecionado),
                0
            )
            
            dados = {
                'data': request.form.get('data'),
                'placa': request.form.get('placa'),
                'responsavel': request.form.get('responsavel'),
                'litros': request.form.get('litros'),
                'desconto': request.form.get('desconto', 0),
                'odometro': request.form.get('odometro', 0),
                'centro_custo': request.form.get('centro_custo'),
                'combustivel': combustivel_selecionado,
                'custo_por_litro': request.form.get('custo_por_litro', preco_padrao),
                'custo_bruto': float(request.form.get('litros')) * float(request.form.get('custo_por_litro', preco_padrao)),
                'custo_liquido': (float(request.form.get('litros')) * float(request.form.get('custo_por_litro', preco_padrao))) - float(request.form.get('desconto', 0))
            }
            
            if criar_abastecimento_atheris(dados):
                flash('Abastecimento registrado com sucesso via Atheris!', 'success')
            else:
                flash('Erro ao registrar abastecimento', 'danger')
        except Exception as e:
            flash(f'Erro no processamento: {str(e)}', 'danger')
        
        return redirect(url_for('integracao_atheris'))
    
    return render_template('integracao_atheris.html', precos=precos)

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
    return render_template('reajuste_combustiveis.html', precos=precos)

@app.route('/api/registros', methods=['POST'])
def criar_registro_api():
    dados = request.get_json()
    try:
        criar_registro(dados)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/registros/<int:id>', methods=['PUT', 'DELETE'])
def gerenciar_registro(id):
    if request.method == 'PUT':
        dados = request.get_json()
        try:
            atualizar_registro(id, dados)
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    elif request.method == 'DELETE':
        try:
            excluir_registro(id)
            return jsonify({'success': True})
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
    app.run(debug=True)