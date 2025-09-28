from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_from_directory, session, g
import pandas as pd
import os
import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import io
import csv
from werkzeug.security import check_password_hash
from functools import wraps
from werkzeug.utils import secure_filename
import json

from database import ( 
    criar_tabelas, 
    obter_relatorio,
    calcular_medias_veiculos,
    obter_precos_combustivel,
    atualizar_preco_combustivel,
    criar_combustivel,
    obter_opcoes_filtro,
    excluir_registro, # Função que estava causando o erro de importação
    atualizar_registro,
    criar_registro,
    obter_registro_por_id,
    obter_trocas_oleo,
    salvar_troca_oleo,
    obter_placas_veiculos,
    obter_identificacoes_equipamentos,
    obter_checklists_por_identificacao,
    excluir_troca_oleo,
    obter_troca_oleo_por_identificacao_tipo,
    obter_manutencoes,
    obter_manutencao_por_id,
    criar_manutencao,
    atualizar_manutencao,
    excluir_manutencao,
    obter_estatisticas_manutencoes,
    criar_pedagio,
    obter_pedagios_com_filtros,
    obter_pedagio_por_id,
    atualizar_pedagio,
    excluir_pedagio,
    obter_checklists, # Função de checklist (listagem)
    # NOVAS FUNÇÕES DEALERS
    get_user_by_username,
    get_user_by_id,
    obter_fornecedores,
    criar_fornecedor,
    obter_cotacoes,
    criar_cotacao,
    fechar_cotacao,
    aprovar_cotacao,
    obter_pedidos_compra,
    obter_pedido_compra_por_id,
    atualizar_pedido_compra,
    finalizar_pedido_compra,
    obter_dealer_intelligence
)

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_123'
app.config['STATIC_FOLDER'] = 'static'

# Configuração de pastas
os.makedirs(app.config['STATIC_FOLDER'], exist_ok=True)
UPLOAD_FOLDER = os.path.join(app.config['STATIC_FOLDER'], 'dealer_files')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Controles de Acesso ---
@app.before_request
def load_logged_in_user():
    user_id = session.get('user_id')
    if user_id is None:
        g.user = None
    else:
        g.user = get_user_by_id(user_id)

def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if g.user is None:
            flash('Você precisa estar logado para acessar esta página.', 'warning')
            return redirect(url_for('login'))
        return view(*args, **kwargs)
    return wrapped_view

def roles_required(roles):
    def wrapper(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if g.user is None:
                flash('Você precisa estar logado para acessar esta página.', 'warning')
                return redirect(url_for('login'))
            if g.user['role'] not in roles:
                flash('Acesso negado. Sua função não permite acessar esta página.', 'danger')
                return redirect(url_for('index'))
            return view(*args, **kwargs)
        return wrapped_view
    return wrapper

# --- Funções Auxiliares ---
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

# --- Rotas de Autenticação ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if g.user:
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user = get_user_by_username(username)
        
        if user and check_password_hash(user['password_hash'], password):
            session.clear()
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['username'] = user['username']
            flash(f'Bem-vindo, {user["username"]} ({user["role"]})!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Usuário ou senha inválidos.', 'danger')
    
    return render_template('login.html', active_page='login')

@app.route('/logout')
@login_required
def logout():
    session.clear()
    flash('Você foi desconectado.', 'info')
    return redirect(url_for('login'))

# --- Rotas Principais (Protegidas) ---
@app.route('/')
@login_required
def index():
    try:
        conn = sqlite3.connect('abastecimentos.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM abastecimentos")
        total_abastecimentos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT placa) FROM abastecimentos")
        total_veiculos = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(valor) FROM manutencoes")
        total_manutencoes = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COUNT(*) FROM manutencoes")
        manutencoescount = cursor.fetchone()[0] or 0
        cursor.execute("SELECT SUM(custo_liquido) FROM abastecimentos")
        gasto_total = cursor.fetchone()[0] or 0
        conn.close()
        
        return render_template('index.html', 
                             active_page='index',
                             total_abastecimentos=total_abastecimentos,
                             total_veiculos=total_veiculos,
                             total_manutencoes=total_manutencoes,
                             manutencoescount=manutencoescount,
                             gasto_total=gasto_total)
    except Exception as e:
        print(f"Erro ao carregar dados do dashboard: {e}")
        return render_template('index.html', active_page='index', total_abastecimentos=0, total_veiculos=0, total_manutencoes=0, manutencoescount=0, gasto_total=0)

@app.route('/relatorios', methods=['GET', 'POST'])
@login_required
def relatorios():
    precos_combustivel = obter_precos_combustivel()
    if request.method == 'POST':
        filtros = {
            'data_inicio': request.form.get('data_inicio', '2025-01-01'),
            'data_fim': request.form.get('data_fim', datetime.now().strftime('%Y-%m-%d')),
            'placa': request.form.get('placa', '').strip() or None,
            'centro_custo': request.form.get('centro_custo', '').strip() or None,
            'combustivel': request.form.get('combustivel', '').strip() or None,
            'posto': request.form.get('posto', '').strip() or None
        }
        try:
            df = obter_relatorio(**filtros)
            if request.form.get('imprimir'):
                return render_template('relatorio_impressao.html', dados=df.to_dict('records'), filtros=filtros, data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M'))
            return render_template('relatorios.html', dados=df.to_dict('records'), filtros=filtros, opcoes_centro_custo=obter_opcoes_filtro('centro_custo'), opcoes_combustivel=obter_opcoes_filtro('combustivel'), opcoes_posto=obter_opcoes_filtro('posto'), precos_combustivel=precos_combustivel, active_page='relatorios')
        except Exception as e:
            flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
            return render_template('relatorios.html', filtros={'data_inicio': '2025-01-01', 'data_fim': datetime.now().strftime('%Y-%m-%d')}, opcoes_centro_custo=obter_opcoes_filtro('centro_custo'), opcoes_combustivel=obter_opcoes_filtro('combustivel'), opcoes_posto=obter_opcoes_filtro('posto'), precos_combustivel=obter_precos_combustivel(), active_page='relatorios')
    
    return render_template('relatorios.html', filtros={'data_inicio': '2025-01-01', 'data_fim': datetime.now().strftime('%Y-%m-%d')}, opcoes_centro_custo=obter_opcoes_filtro('centro_custo'), opcoes_combustivel=obter_opcoes_filtro('combustivel'), opcoes_posto=obter_opcoes_filtro('posto'), precos_combustivel=obter_precos_combustivel(), active_page='relatorios')

@app.route('/manutencoes')
@login_required
def manutencoes():
    try:
        manutencoes_list = obter_manutencoes()
        estatisticas = obter_estatisticas_manutencoes()
        return render_template('manutencoes.html', active_page='manutencoes', manutencoes=manutencoes_list, total_manutencoes=estatisticas['total'], manutencoes_abertas=estatisticas['abertas'], manutencoes_finalizadas=estatisticas['finalizadas'], valor_total=estatisticas['valor_total'])
    except Exception as e:
        flash(f'Erro ao carregar manutenções: {str(e)}', 'danger')
        return render_template('manutencoes.html', active_page='manutencoes', manutencoes=[], total_manutencoes=0, manutencoes_abertas=0, manutencoes_finalizadas=0, valor_total=0)

@app.route('/checklists')
@login_required
def checklists():
    try:
        from database import obter_checklists
        checklists_list = obter_checklists()
        return render_template('checklists.html', active_page='checklists', checklists=checklists_list)
    except Exception as e:
        flash(f'Erro ao carregar checklists: {str(e)}', 'danger')
        return render_template('checklists.html', active_page='checklists', checklists=[])

@app.route('/medias-veiculos')
@login_required
def medias_veiculos():
    try:
        dados = calcular_medias_veiculos()
        df = pd.DataFrame(dados)
        if not df.empty:
            # Recomenda-se gerar o gráfico usando JS no template em produção, mas mantido o código original aqui.
            pass
        return render_template('medias_veiculos.html', dados=dados, active_page='medias')
    except Exception as e:
        flash(f'Erro ao calcular médias: {str(e)}', 'danger')
        return render_template('medias_veiculos.html', dados=[], active_page='medias')

@app.route('/metricas-uso', methods=['GET', 'POST'])
@login_required
def metricas_uso():
    if request.method == 'POST':
        try:
            identificacao = request.form.get('identificacao')
            tipo = request.form.get('tipo')
            data_troca = request.form.get('data_troca')
            identificacao_original = request.form.get('identificacao_original')
            tipo_original = request.form.get('tipo_original')
            
            if tipo == 'veiculo':
                km_troca = float(request.form.get('km_troca')) if request.form.get('km_troca') else None
                horimetro_troca = None
            else:
                km_troca = None
                horimetro_troca = float(request.form.get('horimetro_troca')) if request.form.get('horimetro_troca') else None
            
            if identificacao_original and tipo_original:
                if identificacao != identificacao_original or tipo != tipo_original:
                    excluir_troca_oleo(identificacao_original, tipo_original)
                if salvar_troca_oleo(identificacao, tipo, data_troca, km_troca, horimetro_troca):
                    flash('Troca de óleo atualizada com sucesso!', 'success')
                else:
                    flash('Erro ao atualizar troca de óleo', 'danger')
            else:
                if salvar_troca_oleo(identificacao, tipo, data_troca, km_troca, horimetro_troca):
                    flash('Dados de troca de óleo salvos com sucesso!', 'success')
                else:
                    flash('Erro ao salvar dados de troca de óleo', 'danger')
            
        except Exception as e:
            flash(f'Erro ao salvar dados: {str(e)}', 'danger')
        
        return redirect(url_for('metricas_uso'))
    
    trocas_oleo_list = obter_trocas_oleo()
    placas = obter_placas_veiculos()
    identificacoes_equipamentos = obter_identificacoes_equipamentos()
    
    return render_template('metricas_uso.html', 
                         trocas_oleo=trocas_oleo_list,
                         placas=placas,
                         identificacoes_equipamentos=identificacoes_equipamentos,
                         active_page='metricas')

@app.route('/reajuste-combustiveis', methods=['GET', 'POST'])
@login_required
@roles_required(['Gestor'])
def reajuste_combustiveis():
    if request.method == 'POST':
        try:
            if 'novo_combustivel' in request.form:
                combustivel = request.form.get('novo_combustivel').strip().upper()
                preco = request.form.get('novo_preco')
                if criar_combustivel(combustivel, preco):
                    flash(f'Combustível {combustivel} cadastrado com sucesso!', 'success')
                else:
                    flash(f'Combustível {combustivel} já existe!', 'warning')
            else:
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

@app.route('/pedagios', methods=['GET', 'POST'])
@login_required
def pedagios():
    placas_disponiveis = obter_placas_veiculos()
    
    if request.method == 'POST':
        filtros = {
            'data_inicio': request.form.get('data_inicio', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')),
            'data_fim': request.form.get('data_fim', datetime.now().strftime('%Y-%m-%d')),
            'placa': request.form.get('placa', '').strip() or None
        }
        try:
            pedagios_list = obter_pedagios_com_filtros(**filtros)
            if request.form.get('imprimir'):
                return render_template('relatorio_pedagios_impressao.html', pedagios=pedagios_list, filtros=filtros, data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M'))
            return render_template('pedagios.html', active_page='pedagios', pedagios=pedagios_list, filtros=filtros, placas_disponiveis=placas_disponiveis)
        except Exception as e:
            flash(f'Erro ao gerar relatório de pedágios: {str(e)}', 'danger')
            return render_template('pedagios.html', active_page='pedagios', pedagios=[], placas_disponiveis=placas_disponiveis)
    
    filtros = {'data_inicio': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'), 'data_fim': datetime.now().strftime('%Y-%m-%d'), 'placa': None}
    pedagios_list = obter_pedagios_com_filtros(**filtros)
    
    return render_template('pedagios.html', active_page='pedagios', pedagios=pedagios_list, filtros=filtros, placas_disponiveis=placas_disponiveis)

# --- Rotas do Módulo Dealers (NOVAS) ---
@app.route('/dealers/fornecedores', methods=['GET', 'POST'])
@login_required
@roles_required(['Gestor', 'Comprador'])
def fornecedores():
    if request.method == 'POST':
        try:
            dados = {
                'cnpj': request.form['cnpj'].strip(),
                'nome': request.form['nome'].strip(),
                'ie': request.form['ie'].strip() or None,
                'endereco': request.form['endereco'].strip() or None,
                'tipo': request.form['tipo'].strip(),
                'contato': request.form['contato'].strip()
            }
            if criar_fornecedor(dados):
                flash('Fornecedor cadastrado com sucesso!', 'success')
            else:
                flash('Erro: CNPJ já cadastrado ou dados inválidos.', 'danger')
        except Exception as e:
            flash(f'Erro ao cadastrar fornecedor: {str(e)}', 'danger')
        return redirect(url_for('fornecedores'))
    
    fornecedores_list = obter_fornecedores()
    return render_template('fornecedores.html', active_page='fornecedores', fornecedores=fornecedores_list)

@app.route('/dealers/cotacoes', methods=['GET', 'POST'])
@login_required
def cotacoes():
    user_role = session.get('role')
    
    if request.method == 'POST':
        action = request.form.get('action')
        cotacao_id = request.form.get('cotacao_id')
        
        try:
            if action == 'criar':
                dados = {'item': request.form['item'], 'quantidade': request.form['quantidade'], 'data_limite': request.form['data_limite'], 'observacoes': request.form['observacoes']}
                if criar_cotacao(session['user_id'], dados):
                    flash('Cotação criada com sucesso!', 'success')
                else:
                    flash('Erro ao criar cotação.', 'danger')
            
            elif action == 'fechar' and user_role == 'Comprador':
                dados = {'fornecedor_id': request.form.get('fornecedor_id'), 'valor_fechado': request.form['valor_fechado'], 'prazo_pagamento': request.form['prazo_pagamento'], 'faturamento': request.form['faturamento']}
                success, fornecedor_cnpj = fechar_cotacao(cotacao_id, dados)
                if success:
                    flash(f'Cotação fechada com o CNPJ {fornecedor_cnpj}. Aguardando aprovação do Gestor.', 'success')
                else:
                    flash('Erro ao fechar cotação. Verifique o status.', 'danger')

            elif action == 'aprovar' and user_role == 'Gestor':
                if aprovar_cotacao(cotacao_id, session['user_id']):
                    flash('Cotação aprovada! Pedido de Compra gerado.', 'success')
                else:
                    flash('Erro ao aprovar cotação ou ela não está no status Fechada.', 'danger')
        
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')
        
        return redirect(url_for('cotacoes'))
    
    cotacoes_list = obter_cotacoes()
    fornecedores_list = obter_fornecedores()
    return render_template('cotacoes.html', active_page='cotacoes', cotacoes=cotacoes_list, fornecedores=fornecedores_list)

@app.route('/dealers/pedidos-compra', methods=['GET', 'POST'])
@login_required
@roles_required(['Gestor', 'Comprador'])
def pedidos_compra():
    user_role = session.get('role')
    
    if request.method == 'POST':
        pedido_id = request.form.get('pedido_id')
        action = request.form.get('action')
        
        try:
            if action == 'editar' and user_role == 'Comprador':
                dados = {'item': request.form['item'], 'quantidade': request.form['quantidade'], 'valor': request.form['valor'], 'status': request.form['status']}
                if atualizar_pedido_compra(pedido_id, dados):
                    flash('Pedido de Compra atualizado com sucesso!', 'success')
                else:
                    flash('Erro ao atualizar Pedido de Compra.', 'danger')

            elif action == 'finalizar' and user_role == 'Comprador':
                chave_nfe = request.form.get('nf_e_chave').strip()
                nfs_file = request.files.get('nfs_pdf')
                
                pdf_path = None
                
                if not chave_nfe and not nfs_file:
                    flash('Para finalizar, é obrigatório informar a Chave NF-e OU anexar o PDF da NFS.', 'danger')
                    return redirect(url_for('pedidos_compra'))

                if chave_nfe and len(chave_nfe) not in [44, 54]:
                    flash('Chave NF-e inválida. Deve ter 44 ou 54 dígitos.', 'danger')
                    return redirect(url_for('pedidos_compra'))
                
                if nfs_file and nfs_file.filename != '' and nfs_file.filename.lower().endswith('.pdf'):
                    filename = secure_filename(f"nfs_pc{pedido_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf")
                    pdf_path = os.path.join('dealer_files', filename)
                    nfs_file.save(os.path.join(app.config['STATIC_FOLDER'], pdf_path))
                
                dados = {'nf_e_chave': chave_nfe, 'nfs_pdf_path': pdf_path}
                
                if finalizar_pedido_compra(pedido_id, dados):
                    flash('Pedido de Compra finalizado com sucesso!', 'success')
                else:
                    flash('Erro ao finalizar Pedido de Compra.', 'danger')
            
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')

        return redirect(url_for('pedidos_compra'))

    pedidos_list = obter_pedidos_compra()
    return render_template('pedidos_compra.html', active_page='pedidos_compra', pedidos=pedidos_list, user_role=user_role)


@app.route('/dealers/dealer-intelligence', methods=['GET', 'POST'])
@login_required
@roles_required(['Gestor'])
def dealer_intelligence():
    data_fim = request.form.get('data_fim', datetime.now().strftime('%Y-%m-%d'))
    data_inicio = request.form.get('data_inicio', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
    
    data = obter_dealer_intelligence(data_inicio, data_fim)

    return render_template('dealer_intelligence.html', 
                           active_page='dealer_intelligence',
                           data=data,
                           filtros={'data_inicio': data_inicio, 'data_fim': data_fim})

# --- APIs Existentes (Atualizadas com @login_required) ---

@app.route('/api/dashboard')
@login_required
def api_dashboard():
    try:
        conn = sqlite3.connect('abastecimentos.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM abastecimentos")
        total_abastecimentos = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT placa) FROM abastecimentos")
        total_veiculos = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(valor) FROM manutencoes")
        total_manutencoes = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COUNT(*) FROM manutencoes")
        manutencoescount = cursor.fetchone()[0] or 0
        cursor.execute("SELECT SUM(custo_liquido) FROM abastecimentos")
        gasto_total = cursor.fetchone()[0] or 0
        conn.close()
        
        return jsonify({
            'success': True,
            'total_abastecimentos': total_abastecimentos,
            'total_veiculos': total_veiculos,
            'total_manutencoes': total_manutencoes,
            'manutencoescount': manutencoescount,
            'gasto_total': gasto_total
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/manutencoes', methods=['GET', 'POST'])
@login_required
def api_manutencoes():
    if request.method == 'GET':
        try:
            manutencoes_list = obter_manutencoes()
            estatisticas = obter_estatisticas_manutencoes()
            return jsonify({'manutencoes': manutencoes_list, 'estatisticas': estatisticas})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    elif request.method == 'POST':
        dados = request.get_json()
        try:
            manutencao_id = criar_manutencao(dados)
            if manutencao_id: return jsonify({'success': True, 'id': manutencao_id})
            else: return jsonify({'success': False, 'error': 'Erro ao criar manutenção'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/manutencoes/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
@roles_required(['Gestor', 'Comprador'])
def api_manutencao(id):
    if request.method == 'GET':
        try:
            manutencao = obter_manutencao_por_id(id)
            if manutencao: return jsonify(manutencao)
            return jsonify({'error': 'Manutenção não encontrada'}), 404
        except Exception as e: return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        dados = request.get_json()
        try:
            if atualizar_manutencao(id, dados): return jsonify({'success': True})
            else: return jsonify({'success': False, 'error': 'Erro ao atualizar manutenção'}), 400
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400
    
    elif request.method == 'DELETE':
        try:
            if excluir_manutencao(id): return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Manutenção não encontrada'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/checklists', methods=['GET', 'POST'])
@login_required
def api_checklists():
    if request.method == 'GET':
        try:
            from database import obter_checklists
            checklists_list = obter_checklists()
            return jsonify({'checklists': checklists_list})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    elif request.method == 'POST':
        dados = request.get_json()
        try:
            from database import criar_checklist
            checklist_id = criar_checklist(dados)
            if checklist_id: return jsonify({'success': True, 'id': checklist_id})
            else: return jsonify({'success': False, 'error': 'Erro ao criar checklist'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/checklists/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
def api_checklist(id):
    from database import obter_checklist_por_id, atualizar_checklist, excluir_checklist
    if request.method == 'GET':
        try:
            checklist = obter_checklist_por_id(id)
            if checklist: return jsonify(checklist)
            return jsonify({'error': 'Checklist não encontrado'}), 404
        except Exception as e: return jsonify({'error': str(e)}), 500
    
    elif request.method == 'PUT':
        dados = request.get_json()
        try:
            if atualizar_checklist(id, dados): return jsonify({'success': True})
            else: return jsonify({'success': False, 'error': 'Erro ao atualizar checklist'}), 400
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400
    
    elif request.method == 'DELETE':
        try:
            if excluir_checklist(id): return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Checklist não encontrado'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/checklists/<identificacao>')
@login_required
def api_checklists_por_identificacao(identificacao):
    try:
        checklists_list = obter_checklists_por_identificacao(identificacao)
        return jsonify({'success': True, 'checklists': checklists_list, 'identificacao': identificacao})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/troca-oleo/<identificacao>/<tipo>', methods=['GET'])
@login_required
def api_obter_troca_oleo(identificacao, tipo):
    try:
        troca = obter_troca_oleo_por_identificacao_tipo(identificacao, tipo)
        if troca: return jsonify({'success': True, 'data': troca})
        return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404
    except Exception as e: return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/trocas-oleo/<identificacao>/<tipo>', methods=['DELETE'])
@login_required
def api_excluir_troca_oleo(identificacao, tipo):
    try:
        if excluir_troca_oleo(identificacao, tipo): return jsonify({'success': True})
        return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404
    except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/registros', methods=['POST'])
@login_required
@roles_required(['Gestor', 'Comprador', 'Padrão'])
def criar_registro_api():
    try:
        dados = request.get_json()
        if not all([dados.get('data'), dados.get('placa'), dados.get('combustivel'), dados.get('litros'), dados.get('custo_por_litro')]):
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando'}), 400
        
        litros = float(dados['litros'])
        custo_por_litro = float(dados['custo_por_litro'])
        desconto = float(dados.get('desconto', 0))
        custo_bruto = round(litros * custo_por_litro, 2)
        custo_liquido = round(custo_bruto - desconto, 2)
        
        dados['custo_bruto'] = custo_bruto
        dados['custo_liquido'] = custo_liquido
        
        id = criar_registro(dados)
        return jsonify({'success': True, 'id': id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/registros/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
@roles_required(['Gestor', 'Comprador', 'Padrão'])
def gerenciar_registro(id):
    if request.method == 'GET':
        try:
            registro = obter_registro_por_id(id)
            if registro: return jsonify({'success': True, 'data': registro})
            return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400
            
    elif request.method == 'PUT':
        try:
            dados = request.get_json()
            if not all([dados.get('data'), dados.get('placa'), dados.get('combustivel'), dados.get('litros'), dados.get('custo_por_litro')]):
                return jsonify({'success': False, 'error': 'Dados obrigatórios faltando'}), 400
            
            litros = float(dados['litros'])
            custo_por_litro = float(dados['custo_por_litro'])
            desconto = float(dados.get('desconto', 0))
            custo_bruto = round(litros * custo_por_litro, 2)
            custo_liquido = round(custo_bruto - desconto, 2)
            
            dados['custo_bruto'] = custo_bruto
            dados['custo_liquido'] = custo_liquido
            
            if atualizar_registro(id, dados): return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Nenhum registro atualizado'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400
            
    elif request.method == 'DELETE':
        try:
            if excluir_registro(id): return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Nenhum registro excluído'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/pedagios', methods=['POST'])
@login_required
def api_criar_pedagio():
    try:
        dados = request.get_json()
        if not all([dados.get('data'), dados.get('placa'), dados.get('valor')]):
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando'}), 400
        
        pedagio_id = criar_pedagio(dados)
        if pedagio_id: return jsonify({'success': True, 'id': pedagio_id})
        return jsonify({'success': False, 'error': 'Erro ao criar registro de pedágio'}), 400
    except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/pedagios/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
def api_gerenciar_pedagio(id):
    if request.method == 'GET':
        try:
            pedagio = obter_pedagio_por_id(id)
            if pedagio: return jsonify(pedagio)
            return jsonify({'error': 'Registro de pedágio não encontrado'}), 404
        except Exception as e: return jsonify({'error': str(e)}), 500
            
    elif request.method == 'PUT':
        try:
            dados = request.get_json()
            if atualizar_pedagio(id, dados): return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Nenhum registro atualizado'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400
            
    elif request.method == 'DELETE':
        try:
            if excluir_pedagio(id): return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Nenhum registro excluído'}), 404
        except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/medias-veiculos-dados')
@login_required
def medias_veiculos_dados():
    try:
        dados = calcular_medias_veiculos()
        return jsonify(dados)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pedidos-compra/<int:id>', methods=['GET'])
@login_required
@roles_required(['Gestor', 'Comprador'])
def api_pedido_compra(id):
    pedido = obter_pedido_compra_por_id(id)
    if pedido: return jsonify(pedido)
    return jsonify({'error': 'Pedido não encontrado'}), 404

# --- Context Processor e Main ---
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory(app.config['STATIC_FOLDER'], filename)

@app.context_processor
def inject_now():
    return {'now': datetime.now(), 'g': g}

if __name__ == '__main__':
    criar_tabelas()
    app.run(debug=True, host='0.0.0.0', port='5005')