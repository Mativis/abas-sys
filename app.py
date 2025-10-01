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
    excluir_registro,
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
    atualizar_troca_oleo,
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
    obter_checklists,
    criar_checklist,
    obter_checklist_por_id,
    atualizar_checklist,
    excluir_checklist,
    get_user_by_username,
    get_user_by_id,
    get_all_users,
    create_user,
    update_user,
    delete_user,
    obter_fornecedores,
    criar_fornecedor,
    obter_cotacoes,
    criar_cotacao_com_itens,
    obter_cotacao_por_id,
    obter_itens_por_cotacao_id,
    adicionar_orcamento,
    aprovar_orcamento,
    obter_orcamentos_por_cotacao_id,
    obter_pedidos_compra,
    obter_pedido_compra_por_id,
    obter_itens_por_pedido_id,
    finalizar_pedido_compra,
    obter_dealer_intelligence,
    obter_cotacoes_com_filtros,
    obter_pedidos_compra_com_filtros
)

app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_123'
app.config['STATIC_FOLDER'] = 'static'

# Define o caminho absoluto para a pasta de uploads
UPLOAD_FOLDER = os.path.join(app.root_path, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --- Controles de Acesso ---
@app.before_request
def load_logged_in_user():
    user_id = session.get('user_id')
    g.user = get_user_by_id(user_id) if user_id else None

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
            if g.user['role'] not in roles and g.user['role'] != 'Administrador':
                flash('Acesso negado. Sua função não permite acessar esta página.', 'danger')
                return redirect(url_for('index'))
            return view(*args, **kwargs)
        return wrapped_view
    return wrapper

# --- Rotas de Cotações e Orçamentos ---

@app.route('/dealers/cotacoes-relatorio', methods=['GET', 'POST'])
@login_required
def cotacoes_relatorio():
    if request.method == 'POST':
        try:
            dados = {
                'titulo': request.form['titulo'],
                'data_limite': request.form['data_limite'],
                'observacoes': request.form.get('observacoes', ''),
                'itens': []
            }
            descricoes = request.form.getlist('item_descricao[]')
            quantidades = request.form.getlist('item_quantidade[]')
            
            for i in range(len(descricoes)):
                if descricoes[i] and quantidades[i]:
                    dados['itens'].append({'descricao': descricoes[i], 'quantidade': quantidades[i]})
            
            if not dados['itens']:
                flash('Você deve adicionar pelo menos um item à cotação.', 'warning')
                return redirect(url_for('cotacoes_relatorio'))

            cotacao_id = criar_cotacao_com_itens(session['user_id'], dados)
            
            if cotacao_id:
                flash('Cotação criada com sucesso!', 'success')
                return redirect(url_for('cotacao_detalhe', cotacao_id=cotacao_id))
            else:
                flash('Erro ao criar cotação.', 'danger')
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')
        return redirect(url_for('cotacoes_relatorio'))

    # Lógica de Filtros para GET
    filtros = {
        'data_inicio': request.args.get('data_inicio', ''),
        'data_fim': request.args.get('data_fim', ''),
        'status': request.args.get('status', ''),
        'pesquisa': request.args.get('pesquisa', '')
    }
    
    cotacoes_list = obter_cotacoes_com_filtros(**filtros)

    if request.args.get('imprimir'):
        return render_template('relatorio_cotacoes_impressao.html', cotacoes=cotacoes_list, filtros=filtros, data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M'))

    return render_template('cotacoes_relatorio.html', active_page='cotacoes', cotacoes=cotacoes_list, filtros=filtros)

@app.route('/uploads/<path:filename>')
@login_required
def serve_upload(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

@app.route('/dealers/cotacao/<int:cotacao_id>', methods=['GET', 'POST'])
@login_required
def cotacao_detalhe(cotacao_id):
    user_role = g.user['role']

    if request.method == 'POST':
        action = request.form.get('action')
        try:
            if action == 'adicionar_orcamento' and user_role in ['Administrador', 'Comprador', 'Gestor']:
                dados = {
                    'cotacao_id': cotacao_id,
                    'fornecedor_id': request.form.get('fornecedor_id'),
                    'valor': request.form['valor'],
                    'prazo_pagamento': request.form['prazo_pagamento'],
                    'faturamento': request.form['faturamento']
                }
                if adicionar_orcamento(dados):
                    flash('Orçamento adicionado com sucesso!', 'success')
                else:
                    flash('Erro ao adicionar orçamento.', 'danger')

            elif action == 'aprovar_orcamento' and user_role in ['Administrador', 'Gestor']:
                orcamento_id = request.form.get('orcamento_id')
                pedido_id = aprovar_orcamento(orcamento_id, session['user_id'])
                if pedido_id:
                    flash('Orçamento aprovado e Pedido de Compra gerado!', 'success')
                    return redirect(url_for('pedido_detalhe', pedido_id=pedido_id))
                else:
                    flash('Erro ao aprovar orçamento.', 'danger')
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')
        
        return redirect(url_for('cotacao_detalhe', cotacao_id=cotacao_id))

    cotacao = obter_cotacao_por_id(cotacao_id)
    itens = obter_itens_por_cotacao_id(cotacao_id)
    orcamentos = obter_orcamentos_por_cotacao_id(cotacao_id)
    fornecedores = obter_fornecedores()
    return render_template('cotacao_detalhe.html', active_page='cotacoes', cotacao=cotacao, itens=itens, orcamentos=orcamentos, fornecedores=fornecedores)


# --- Rotas de Pedidos de Compra ---
@app.route('/dealers/pedidos-relatorio', methods=['GET'])
@login_required
def pedidos_relatorio():
    filtros = {
        'data_inicio': request.args.get('data_inicio', ''),
        'data_fim': request.args.get('data_fim', ''),
        'status': request.args.get('status', ''),
        'pesquisa': request.args.get('pesquisa', '')
    }
    pedidos_list = obter_pedidos_compra_com_filtros(**filtros)

    if request.args.get('imprimir'):
        return render_template('relatorio_pedidos_impressao.html', pedidos=pedidos_list, filtros=filtros, data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M'))
        
    return render_template('pedidos_relatorio.html', active_page='pedidos_compra', pedidos=pedidos_list, filtros=filtros)


@app.route('/dealers/pedido/<int:pedido_id>', methods=['GET', 'POST'])
@login_required
def pedido_detalhe(pedido_id):
    user_role = g.user['role']

    if request.method == 'POST':
        action = request.form.get('action')
        try:
            if action == 'finalizar' and user_role in ['Administrador', 'Comprador', 'Gestor']:
                chave_nfe = request.form.get('nf_e_chave').strip()
                nfs_file = request.files.get('nfs_pdf')
                db_pdf_filename = None

                if not chave_nfe and not (nfs_file and nfs_file.filename != ''):
                    flash('Para finalizar, é obrigatório informar a Chave NF-e OU anexar o PDF da NFS.', 'danger')
                    return redirect(url_for('pedido_detalhe', pedido_id=pedido_id))

                if chave_nfe and len(chave_nfe) not in [44, 54]:
                    flash('Chave NF-e inválida. Deve ter 44 ou 54 dígitos.', 'danger')
                    return redirect(url_for('pedido_detalhe', pedido_id=pedido_id))
                
                if nfs_file and nfs_file.filename != '' and nfs_file.filename.lower().endswith('.pdf'):
                    filename = secure_filename(f"nfs_pc{pedido_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf")
                    nfs_file.save(os.path.join(UPLOAD_FOLDER, filename))
                    db_pdf_filename = filename
                
                dados = {'nf_e_chave': chave_nfe, 'nfs_pdf_path': db_pdf_filename}
                if finalizar_pedido_compra(pedido_id, dados):
                    flash('Pedido de Compra finalizado com sucesso!', 'success')
                else:
                    flash('Erro ao finalizar Pedido de Compra.', 'danger')
            
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')

        return redirect(url_for('pedido_detalhe', pedido_id=pedido_id))

    pedido = obter_pedido_compra_por_id(pedido_id)
    itens = obter_itens_por_pedido_id(pedido_id)
    return render_template('pedido_detalhe.html', active_page='pedidos_compra', pedido=pedido, itens=itens, user_role=user_role)


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
        return render_template('medias_veiculos.html', dados=dados, active_page='medias')
    except Exception as e:
        flash(f'Erro ao calcular médias: {str(e)}', 'danger')
        return render_template('medias_veiculos.html', dados=[], active_page='medias')

@app.route('/metricas-uso', methods=['GET', 'POST'])
@login_required
def metricas_uso():
    if request.method == 'POST':
        try:
            pass
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
@roles_required(['Administrador', 'Gestor'])
def reajuste_combustiveis():
    if request.method == 'POST':
        try:
            pass
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
        pass
    filtros = {'data_inicio': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'), 'data_fim': datetime.now().strftime('%Y-%m-%d'), 'placa': None}
    pedagios_list = obter_pedagios_com_filtros(**filtros)
    return render_template('pedagios.html', active_page='pedagios', pedagios=pedagios_list, filtros=filtros, placas_disponiveis=placas_disponiveis)

@app.route('/dealers/fornecedores', methods=['GET', 'POST'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador'])
def fornecedores():
    if request.method == 'POST':
        try:
            pass
        except Exception as e:
            flash(f'Erro ao cadastrar fornecedor: {str(e)}', 'danger')
        return redirect(url_for('fornecedores'))
    
    fornecedores_list = obter_fornecedores()
    return render_template('fornecedores.html', active_page='fornecedores', fornecedores=fornecedores_list)

@app.route('/dealers/dealer-intelligence', methods=['GET', 'POST'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def dealer_intelligence():
    data_fim = request.form.get('data_fim', datetime.now().strftime('%Y-%m-%d'))
    data_inicio = request.form.get('data_inicio', (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'))
    
    data = obter_dealer_intelligence(data_inicio, data_fim)

    return render_template('dealer_intelligence.html', 
                           active_page='dealer_intelligence',
                           data=data,
                           filtros={'data_inicio': data_inicio, 'data_fim': data_fim})

@app.route('/admin/users', methods=['GET'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def user_management():
    users = get_all_users()
    roles = ['Administrador', 'Gestor', 'Comprador', 'Padrão']
    return render_template('user_management.html', active_page='user_management', users=users, roles=roles)


# --- APIs ---
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
@roles_required(['Administrador', 'Gestor', 'Comprador'])
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
            checklists_list = obter_checklists()
            return jsonify({'checklists': checklists_list})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    elif request.method == 'POST':
        dados = request.get_json()
        try:
            checklist_id = criar_checklist(dados)
            if checklist_id: return jsonify({'success': True, 'id': checklist_id})
            else: return jsonify({'success': False, 'error': 'Erro ao criar checklist'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/checklists/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
def api_checklist(id):
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
@roles_required(['Administrador', 'Gestor', 'Comprador', 'Padrão'])
def api_criar_registro():
    try:
        dados = request.get_json()
        if not all([dados.get('data'), dados.get('placa'), dados.get('combustivel'), dados.get('litros'), dados.get('custo_por_litro')]):
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando.'}), 400
        
        litros = float(dados['litros'])
        custo_por_litro = float(dados['custo_por_litro'])
        desconto = float(dados.get('desconto', 0))
        custo_bruto = round(litros * custo_por_litro, 2)
        custo_liquido = round(custo_bruto - desconto, 2)
        
        dados['custo_bruto'] = custo_bruto
        dados['custo_liquido'] = custo_liquido
        
        id_criado = criar_registro(dados)
        return jsonify({'success': True, 'id': id_criado})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/registros/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador', 'Padrão'])
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
                return jsonify({'success': False, 'error': 'Dados obrigatórios faltando.'}), 400
            
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
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando.'}), 400
        
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
@roles_required(['Administrador', 'Gestor', 'Comprador'])
def api_pedido_compra(id):
    pedido = obter_pedido_compra_por_id(id)
    if pedido: return jsonify(pedido)
    return jsonify({'error': 'Pedido não encontrado'}), 404

@app.route('/api/users', methods=['POST'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def api_create_user():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    role = data.get('role')
    
    if not all([username, password, role]):
        return jsonify({'success': False, 'error': 'Dados obrigatórios faltando.'}), 400
    
    if len(password) < 3:
        return jsonify({'success': False, 'error': 'A senha deve ter pelo menos 3 caracteres.'}), 400
    
    if create_user(username, password, role):
        return jsonify({'success': True}), 201
    else:
        return jsonify({'success': False, 'error': 'Nome de usuário já existe ou role inválida.'}), 409

@app.route('/api/users/<int:user_id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def api_manage_user(user_id):
    if request.method == 'GET':
        user = get_user_by_id(user_id)
        if user:
            return jsonify({'id': user['id'], 'username': user['username'], 'role': user['role']})
        return jsonify({'success': False, 'error': 'Usuário não encontrado.'}), 404
        
    elif request.method == 'PUT':
        data = request.get_json()
        username = data.get('username')
        role = data.get('role')
        password = data.get('password')
        
        if not all([username, role]):
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando.'}), 400
        
        if update_user(user_id, username, role, password):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Erro ao atualizar. Nome de usuário pode estar em uso ou role inválida.'}), 409
            
    elif request.method == 'DELETE':
        if user_id == g.user['id']:
             return jsonify({'success': False, 'error': 'Não é permitido deletar seu próprio usuário.'}), 403
             
        if delete_user(user_id):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Usuário não encontrado.'}), 404

@app.context_processor
def inject_now():
    return {'now': datetime.now(), 'g': g}

if __name__ == '__main__':
    criar_tabelas()
    app.run(debug=True, host='0.0.0.0', port='5005')