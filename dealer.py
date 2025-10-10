# routes/dealer.py

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, g, jsonify, send_from_directory
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
import os

from database import (
    obter_fornecedores,
    criar_fornecedor,
    criar_cotacao_com_itens,
    obter_cotacao_por_id,
    obter_itens_por_cotacao_id,
    adicionar_orcamento,
    aprovar_orcamento,
    obter_orcamentos_por_cotacao_id,
    obter_pedido_compra_por_id,
    obter_itens_por_pedido_id,
    finalizar_pedido_compra,
    obter_dealer_intelligence,
    obter_cotacoes_com_filtros,
    obter_pedidos_compra_com_filtros
)
from utils import login_required, roles_required

# Define o prefixo '/dealers' para todas as rotas neste Blueprint
dealer_bp = Blueprint('dealer', __name__, url_prefix='/dealers')

# --- Rotas Principais Dealer ---

@dealer_bp.route('/cotacoes-relatorio', methods=['GET', 'POST'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador'])
def cotacoes_relatorio():
    if request.method == 'POST':
        try:
            dados = {'titulo': request.form['titulo'], 'data_limite': request.form['data_limite'],
                     'observacoes': request.form.get('observacoes', ''), 'itens': []}
            
            for desc, quant in zip(request.form.getlist('item_descricao[]'), request.form.getlist('item_quantidade[]')):
                if desc and quant:
                    dados['itens'].append({'descricao': desc, 'quantidade': quant})
            
            if not dados['itens']:
                flash('Adicione pelo menos um item à cotação.', 'warning')
            else:
                cotacao_id = criar_cotacao_com_itens(session['user_id'], dados)
                if cotacao_id:
                    # Note: O url_for aponta para a nova rota dentro deste Blueprint
                    flash('Cotação criada com sucesso!', 'success')
                    return redirect(url_for('dealer.cotacao_detalhe', cotacao_id=cotacao_id))
                else:
                    flash('Erro ao criar cotação.', 'danger')
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')
        
        return redirect(url_for('dealer.cotacoes_relatorio'))

    filtros = {
        'data_inicio': request.args.get('data_inicio', ''),
        'data_fim': request.args.get('data_fim', ''),
        'status': request.args.get('status', ''),
        'pesquisa': request.args.get('pesquisa', '')
    }
    
    cotacoes_list = obter_cotacoes_com_filtros(**filtros)
    
    return render_template('cotacoes_relatorio.html', active_page='cotacoes_relatorio', cotacoes=cotacoes_list, filtros=filtros)


@dealer_bp.route('/cotacao/<int:cotacao_id>', methods=['GET', 'POST'])
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
                    return redirect(url_for('dealer.pedido_detalhe', pedido_id=pedido_id))
                else:
                    flash('Erro ao aprovar orçamento.', 'danger')
        except Exception as e:
            flash(f'Erro na operação: {str(e)}', 'danger')
        
        return redirect(url_for('dealer.cotacao_detalhe', cotacao_id=cotacao_id))

    cotacao = obter_cotacao_por_id(cotacao_id)
    itens = obter_itens_por_cotacao_id(cotacao_id)
    orcamentos = obter_orcamentos_por_cotacao_id(cotacao_id)
    fornecedores = obter_fornecedores()
    return render_template('cotacao_detalhe.html', active_page='cotacoes_relatorio', cotacao=cotacao, itens=itens, orcamentos=orcamentos, fornecedores=fornecedores)


@dealer_bp.route('/pedidos-relatorio', methods=['GET'])
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
        
    return render_template('pedidos_relatorio.html', active_page='pedidos_relatorio', pedidos=pedidos_list, filtros=filtros)


@dealer_bp.route('/pedido/<int:pedido_id>', methods=['GET', 'POST'])
@login_required
def pedido_detalhe(pedido_id):
    user_role = g.user['role']
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')

    if request.method == 'POST':
        action = request.form.get('action')
        try:
            if action == 'finalizar' and user_role in ['Administrador', 'Comprador', 'Gestor']:
                chave_nfe = request.form.get('nf_e_chave').strip()
                nfs_file = request.files.get('nfs_pdf')
                db_pdf_filename = None

                if not chave_nfe and not (nfs_file and nfs_file.filename != ''):
                    flash('Para finalizar, é obrigatório informar a Chave NF-e OU anexar o PDF da NFS.', 'danger')
                    return redirect(url_for('dealer.pedido_detalhe', pedido_id=pedido_id))

                if chave_nfe and len(chave_nfe) not in [44, 54]:
                    flash('Chave NF-e inválida. Deve ter 44 ou 54 dígitos.', 'danger')
                    return redirect(url_for('dealer.pedido_detalhe', pedido_id=pedido_id))
                
                if nfs_file and nfs_file.filename != '' and nfs_file.filename.lower().endswith('.pdf'):
                    # A função secure_filename foi movida para este escopo
                    from werkzeug.utils import secure_filename
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

        return redirect(url_for('dealer.pedido_detalhe', pedido_id=pedido_id))

    pedido = obter_pedido_compra_por_id(pedido_id)
    itens = obter_itens_por_pedido_id(pedido_id)
    # A rota 'serve_upload' para visualizar o PDF agora está em frota.serve_upload
    return render_template('pedido_detalhe.html', active_page='pedidos_relatorio', pedido=pedido, itens=itens, user_role=user_role)


@dealer_bp.route('/fornecedores', methods=['GET', 'POST'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador'])
def fornecedores():
    if request.method == 'POST':
        try:
            # Lógica extraída da análise do template fornecedores.html e database.py
            dados = {
                'cnpj': request.form['cnpj'],
                'nome': request.form['nome'],
                'ie': request.form.get('ie', ''),
                'endereco': request.form.get('endereco', ''),
                'tipo': request.form['tipo'],
                'contato': request.form.get('contato', '')
            }
            if criar_fornecedor(dados):
                flash('Fornecedor cadastrado com sucesso!', 'success')
            else:
                flash('Erro ao cadastrar fornecedor. CNPJ ou Nome já pode existir.', 'danger')
        except Exception as e:
            flash(f'Erro ao cadastrar fornecedor: {str(e)}', 'danger')
        return redirect(url_for('dealer.fornecedores'))
    
    fornecedores_list = obter_fornecedores()
    return render_template('fornecedores.html', active_page='fornecedores', fornecedores=fornecedores_list)


@dealer_bp.route('/dealer-intelligence', methods=['GET', 'POST'])
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


# --- APIs Dealer (apenas uma existia no app.py original) ---

@dealer_bp.route('/api/pedidos-compra/<int:id>', methods=['GET'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador'])
def api_pedido_compra(id):
    pedido = obter_pedido_compra_por_id(id)
    if pedido: return jsonify(pedido)
    return jsonify({'error': 'Pedido não encontrado'}), 404