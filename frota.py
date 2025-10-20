# routes/frota.py

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session, g, send_from_directory
from datetime import datetime, timedelta
import os
import sqlite3 # Necessário para replicar a lógica de 'index' e 'api_dashboard' que usava sqlite3 diretamente

from database import (
    obter_relatorio,
    calcular_medias_veiculos,
    criar_requisicao,
    obter_todas_requisicoes,
    obter_requisicao_por_id,
    atualizar_requisicao,
    excluir_requisicao,
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
    obter_pedido_compra_por_id,
    # NOVAS IMPORTAÇÕES NOTION-LIKE
    create_notion_page,
    get_notion_pages_by_category,
    get_notion_page_by_id,
    update_notion_page,
    transfer_notion_page,
    delete_notion_page
)
from utils import login_required, roles_required

# Blueprint para as rotas principais (sem prefixo)
frota_bp = Blueprint('frota', __name__)


# --- Rotas Principais Frota / Raiz ---

@frota_bp.route('/')
@login_required
def index():
    # Lógica original do app.py para / (index) usando sqlite3 diretamente
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

@frota_bp.route('/relatorios', methods=['GET', 'POST'])
@login_required
def relatorios():
    requisicao_para_abastecer = None
    requisicao_id = request.args.get('requisicao_id', type=int)
    if requisicao_id:
        requisicao_para_abastecer = obter_requisicao_por_id(requisicao_id)

    filtros = {}
    if request.method == 'POST':
        filtros = {
            'data_inicio': request.form.get('data_inicio'),
            'data_fim': request.form.get('data_fim'),
            'placa': request.form.get('placa', '').strip() or None,
            'centro_custo': request.form.get('centro_custo', '').strip() or None,
            'combustivel': request.form.get('combustivel', '').strip() or None,
            'posto': request.form.get('posto', '').strip() or None
        }
    else:
        filtros = {
            'data_inicio': request.args.get('data_inicio', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')),
            'data_fim': request.args.get('data_fim', datetime.now().strftime('%Y-%m-%d')),
            'placa': request.args.get('placa', '').strip() or None
        }

    try:
        df = obter_relatorio(**filtros)
        dados = df.to_dict('records')
        if request.values.get('imprimir'):
            return render_template('relatorio_impressao.html', dados=dados, filtros=filtros, data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M'))
    except Exception as e:
        flash(f'Erro ao gerar relatório: {str(e)}', 'danger')
        dados = []

    filtros_para_template = {k: (v or '') for k, v in filtros.items()}

    return render_template(
        'relatorios.html',
        dados=dados,
        filtros=filtros_para_template,
        opcoes_centro_custo=obter_opcoes_filtro('centro_custo'),
        opcoes_combustivel=obter_opcoes_filtro('combustivel'),
        opcoes_posto=obter_opcoes_filtro('posto'),
        precos_combustivel=obter_precos_combustivel(),
        active_page='relatorios',
        requisicao_para_abastecer=requisicao_para_abastecer
    )

@frota_bp.route('/manutencoes')
@login_required
def manutencoes():
    try:
        manutencoes_list = obter_manutencoes()
        estatisticas = obter_estatisticas_manutencoes()
        return render_template('manutencoes.html', active_page='manutencoes', manutencoes=manutencoes_list, total_manutencoes=estatisticas['total'], manutencoes_abertas=estatisticas['abertas'], manutencoes_finalizadas=estatisticas['finalizadas'], valor_total=estatisticas['valor_total'])
    except Exception as e:
        flash(f'Erro ao carregar manutenções: {str(e)}', 'danger')
        return render_template('manutencoes.html', active_page='manutencoes', manutencoes=[], total_manutencoes=0, manutencoes_abertas=0, manutencoes_finalizadas=0, valor_total=0)

@frota_bp.route('/checklists')
@login_required
def checklists():
    try:
        checklists_list = obter_checklists()
        return render_template('checklists.html', active_page='checklists', checklists=checklists_list)
    except Exception as e:
        flash(f'Erro ao carregar checklists: {str(e)}', 'danger')
        return render_template('checklists.html', active_page='checklists', checklists=[])

@frota_bp.route('/medias-veiculos')
@login_required
def medias_veiculos():
    try:
        dados = calcular_medias_veiculos()
        return render_template('medias_veiculos.html', dados=dados, active_page='medias_veiculos')
    except Exception as e:
        flash(f'Erro ao calcular médias: {str(e)}', 'danger')
        return render_template('medias_veiculos.html', dados=[], active_page='medias_veiculos')

@frota_bp.route('/metricas-uso', methods=['GET', 'POST'])
@login_required
def metricas_uso():
    if request.method == 'POST':
        # Lógica de salvar/atualizar o registro de troca de óleo
        try:
            if request.form.get('identificacao_original'):
                identificacao_original = request.form['identificacao_original']
                tipo_original = request.form['tipo_original']
                if atualizar_troca_oleo(
                    identificacao_original, 
                    tipo_original, 
                    request.form['data_troca'], 
                    km_troca=request.form.get('km_troca'), 
                    horimetro_troca=request.form.get('horimetro_troca')):
                    flash('Registro de troca de óleo atualizado com sucesso!', 'success')
                else:
                    flash('Erro ao atualizar registro de troca de óleo.', 'danger')
            else:
                if salvar_troca_oleo(
                    request.form['identificacao'], 
                    request.form['tipo'], 
                    request.form['data_troca'], 
                    km_troca=request.form.get('km_troca'), 
                    horimetro_troca=request.form.get('horimetro_troca')):
                    flash('Registro de troca de óleo salvo com sucesso!', 'success')
                else:
                    flash('Erro ao salvar novo registro de troca de óleo.', 'danger')
        except Exception as e:
            flash(f'Erro ao salvar dados: {str(e)}', 'danger')
        return redirect(url_for('frota.metricas_uso'))
    
    trocas_oleo_list = obter_trocas_oleo()
    placas = obter_placas_veiculos()
    identificacoes_equipamentos = obter_identificacoes_equipamentos()
    
    return render_template('metricas_uso.html', 
                         trocas_oleo=trocas_oleo_list,
                         placas=placas,
                         identificacoes_equipamentos=identificacoes_equipamentos,
                         active_page='metricas_uso')

@frota_bp.route('/reajuste-combustiveis', methods=['GET', 'POST'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def reajuste_combustiveis():
    if request.method == 'POST':
        try:
            if 'novo_combustivel' in request.form:
                if criar_combustivel(request.form['novo_combustivel'], request.form['novo_preco']):
                    flash('Novo combustível cadastrado com sucesso!', 'success')
                else:
                    flash('Erro: Combustível já existe ou preço inválido.', 'danger')
            else:
                if atualizar_preco_combustivel(request.form['combustivel'], request.form['novo_preco']):
                    flash('Preço do combustível atualizado com sucesso!', 'success')
                else:
                    flash('Erro ao atualizar preço.', 'danger')
        except Exception as e:
            flash(f'Erro no processamento: {str(e)}', 'danger')
        return redirect(url_for('frota.reajuste_combustiveis'))
    
    precos = obter_precos_combustivel()
    return render_template('reajuste_combustiveis.html', precos=precos, active_page='reajuste_combustiveis')

@frota_bp.route('/pedagios', methods=['GET', 'POST'])
@login_required
def pedagios():
    placas_disponiveis = obter_placas_veiculos()
    if request.method == 'POST':
        filtros = {
            'data_inicio': request.form.get('data_inicio'),
            'data_fim': request.form.get('data_fim'),
            'placa': request.form.get('placa', '').strip() or None
        }
        
        pedagios_list = obter_pedagios_com_filtros(**filtros)
        
        if request.values.get('imprimir'):
            return render_template('relatorio_pedagios_impressao.html', pedagios=pedagios_list, filtros=filtros, data_emissao=datetime.now().strftime('%d/%m/%Y %H:%M'))
    else:
        filtros = {
            'data_inicio': request.args.get('data_inicio', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')), 
            'data_fim': request.args.get('data_fim', datetime.now().strftime('%Y-%m-%d')), 
            'placa': request.args.get('placa', '').strip() or None
        }
        pedagios_list = obter_pedagios_com_filtros(**filtros)
        
    return render_template('pedagios.html', active_page='pedagios', pedagios=pedagios_list, filtros=filtros, placas_disponiveis=placas_disponiveis)

@frota_bp.route('/requisicoes', methods=['GET', 'POST'])
@login_required
def requisicoes():
    if request.method == 'POST':
        try:
            dados = {
                'data_solicitacao': datetime.now().strftime('%Y-%m-%d'),
                'solicitado_por_id': session['user_id'],
                'placa': request.form['placa'],
                'motorista': request.form.get('motorista'),
                'centro_custo': request.form.get('centro_custo'),
                'combustivel': request.form.get('combustivel'),
                'quantidade_estimada': request.form.get('quantidade_estimada')
            }
            criar_requisicao(dados)
            flash('Requisição de abastecimento criada com sucesso!', 'success')
        except Exception as e:
            flash(f'Erro ao criar requisição: {str(e)}', 'danger')
        return redirect(url_for('frota.requisicoes'))

    requisicoes_list = obter_todas_requisicoes()
    return render_template('requisicoes.html', active_page='requisicoes', requisicoes=requisicoes_list)

@frota_bp.route('/requisicao/<int:id>/imprimir')
@login_required
def imprimir_requisicao(id):
    requisicao = obter_requisicao_por_id(id)
    if not requisicao:
        flash('Requisição não encontrada.', 'danger')
        return redirect(url_for('frota.requisicoes'))
    return render_template('requisicao_impressao.html', requisicao=requisicao)    

@frota_bp.route('/requisicao/<int:id>/editar', methods=['POST'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def editar_requisicao(id):
    try:
        dados = request.form.to_dict()
        if atualizar_requisicao(id, dados):
            flash('Requisição atualizada com sucesso!', 'success')
        else:
            flash('Erro ao atualizar. A requisição pode não estar mais pendente.', 'danger')
    except Exception as e:
        flash(f'Ocorreu um erro inesperado: {str(e)}', 'danger')
    return redirect(url_for('frota.requisicoes'))

@frota_bp.route('/requisicao/<int:id>/excluir', methods=['POST'])
@login_required
@roles_required(['Administrador', 'Gestor'])
def excluir_requisicao_route(id):
    try:
        if excluir_requisicao(id):
            flash('Requisição excluída com sucesso.', 'success')
        else:
            flash('Erro ao excluir. A requisição pode já ter sido concluída.', 'warning')
    except Exception as e:
        flash(f'Ocorreu um erro inesperado: {str(e)}', 'danger')
    return redirect(url_for('frota.requisicoes'))

@frota_bp.route('/uploads/<path:filename>')
@login_required
def serve_upload(filename):
    """Serve arquivos da pasta UPLOAD_FOLDER. Usado por módulos como o Dealer."""
    # O UPLOAD_FOLDER foi definido no app.py principal e replicado aqui para contexto
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
    return send_from_directory(UPLOAD_FOLDER, filename)

# --- NOVAS Rotas Notion-Like (LISTAGEM) ---

@frota_bp.route('/frota-notion', methods=['GET'])
@login_required
def frota_notion():
    search_query = request.args.get('search', '')
    pages = get_notion_pages_by_category('frota', search_query if search_query else None)
    
    # Ordem de exibição: Ativa -> Transferida
    order_status = {'Ativa': 1, 'Transferida': 2, 'Manual': 3}
    pages.sort(key=lambda x: order_status.get(x['status'], 99))
    
    return render_template('frota_notion.html', active_page='frota_notion', pages=pages, search_query=search_query)

@frota_bp.route('/historico-notion', methods=['GET'])
@login_required
def historico_notion():
    search_query = request.args.get('search', '')
    pages = get_notion_pages_by_category('historico', search_query if search_query else None)
    
    # Ordem de exibição: mais recente primeiro
    pages.sort(key=lambda x: x['data_registro'], reverse=True)
    
    return render_template('historico_notion.html', active_page='historico_notion', pages=pages, search_query=search_query)

# --- Rota Notion-Like (DETALHE/EDIÇÃO) ---

@frota_bp.route('/notion-page/<id>', methods=['GET'])
@login_required
def notion_page_detail(id):
    # CORREÇÃO: Recebe como string e converte para int aqui
    try:
        page_id = int(id)
    except ValueError:
        flash('ID de página inválido.', 'danger')
        return redirect(url_for('frota.index'))

    page = get_notion_page_by_id(page_id)
    if not page:
        flash('Página não encontrada.', 'danger')
        return redirect(url_for('frota.index'))
    
    # Define o contexto para saber qual menu destacar e quais botões mostrar
    context = page['category']
    active_page = 'frota_notion' if page['category'] == 'frota' else 'historico_notion'
    
    # Renderiza o template de detalhe genérico
    return render_template('notion_page_detail.html', 
                           page=page, 
                           context=context,
                           active_page=active_page)

# --- NOVA Rota de IMPRESSÃO ---
@frota_bp.route('/notion-page/<int:id>/print', methods=['GET'])
@login_required
def notion_page_print(id):
    page = get_notion_page_by_id(id)
    if not page:
        flash('Página não encontrada.', 'danger')
        return redirect(url_for('frota.index'))

    # Renderiza o template de impressão e passa a classe datetime para acesso ao now()
    return render_template('notion_page_print.html', page=page, datetime=datetime)


# --- APIs Frota / Raiz ---

@frota_bp.route('/api/dashboard')
@login_required
def api_dashboard():
    # Lógica original do app.py para /api/dashboard usando sqlite3 diretamente
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

@frota_bp.route('/api/manutencoes', methods=['GET', 'POST'])
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

@frota_bp.route('/api/manutencoes/<int:id>', methods=['GET', 'PUT', 'DELETE'])
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

@frota_bp.route('/api/checklists', methods=['GET', 'POST'])
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

@frota_bp.route('/api/checklists/<int:id>', methods=['GET', 'PUT', 'DELETE'])
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

@frota_bp.route('/api/checklists/<identificacao>')
@login_required
def api_checklists_por_identificacao(identificacao):
    try:
        checklists_list = obter_checklists_por_identificacao(identificacao)
        return jsonify({'success': True, 'checklists': checklists_list, 'identificacao': identificacao})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@frota_bp.route('/api/troca-oleo/<identificacao>/<tipo>', methods=['GET'])
@login_required
def api_obter_troca_oleo(identificacao, tipo):
    try:
        troca = obter_troca_oleo_por_identificacao_tipo(identificacao, tipo)
        if troca: return jsonify({'success': True, 'data': troca})
        return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404
    except Exception as e: return jsonify({'success': False, 'error': str(e)}), 500

@frota_bp.route('/api/trocas-oleo/<identificacao>/<tipo>', methods=['DELETE'])
@login_required
def api_excluir_troca_oleo(identificacao, tipo):
    try:
        if excluir_troca_oleo(identificacao, tipo): return jsonify({'success': True})
        return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404
    except Exception as e: return jsonify({'success': False, 'error': str(e)}), 400

@frota_bp.route('/api/registros', methods=['POST'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador', 'Padrão'])
def api_criar_registro():
    try:
        dados = request.get_json()
        if not all([dados.get('data'), dados.get('placa'), dados.get('combustivel')]):
            return jsonify({'success': False, 'error': 'Dados obrigatórios faltando.'}), 400
        
        litros = float(dados.get('litros') or 0)
        custo_por_litro = float(dados.get('custo_por_litro') or 0)
        desconto = float(dados.get('desconto', 0))
        
        dados['litros'] = litros
        dados['custo_por_litro'] = custo_por_litro
        dados['desconto'] = desconto
        dados['custo_bruto'] = round(litros * custo_por_litro, 2)
        dados['custo_liquido'] = round(dados['custo_bruto'] - desconto, 2)
        
        id = criar_registro(dados)
        return jsonify({'success': True, 'id': id})
    except (ValueError, TypeError) as e:
        return jsonify({'success': False, 'error': f'Valor numérico inválido: {e}'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@frota_bp.route('/api/registros/<int:id>', methods=['GET', 'PUT', 'DELETE'])
@login_required
@roles_required(['Administrador', 'Gestor', 'Comprador', 'Padrão'])
def gerenciar_registro(id):
    if request.method == 'GET':
        registro = obter_registro_por_id(id)
        if registro: return jsonify({'success': True, 'data': registro})
        return jsonify({'success': False, 'error': 'Registro não encontrado'}), 404

    elif request.method == 'PUT':
        try:
            dados = request.get_json()
            # Simplificação, assumindo que os campos corretos estão no payload
            dados['litros'] = float(dados.get('litros') or 0)
            dados['custo_por_litro'] = float(dados.get('custo_por_litro') or 0)
            dados['desconto'] = float(dados.get('desconto', 0))
            
            dados['custo_bruto'] = round(dados['litros'] * dados['custo_por_litro'], 2)
            dados['custo_liquido'] = round(dados['custo_bruto'] - dados['desconto'], 2)

            if atualizar_registro(id, dados):
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Erro ao atualizar'}), 400
        except (ValueError, TypeError) as e:
            return jsonify({'success': False, 'error': f'Valor numérico inválido: {e}'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400

    elif request.method == 'DELETE':
        if excluir_registro(id): return jsonify({'success': True})
        return jsonify({'success': False, 'error': 'Nenhum registro excluído'}), 404

@frota_bp.route('/api/pedagios', methods=['POST'])
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

@frota_bp.route('/api/pedagios/<int:id>', methods=['GET', 'PUT', 'DELETE'])
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

@frota_bp.route('/medias-veiculos-dados')
@login_required
def medias_veiculos_dados():
    try:
        dados = calcular_medias_veiculos()
        return jsonify(dados)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@frota_bp.route('/api/requisicao/<int:id>')
@login_required
@roles_required(['Administrador', 'Gestor'])
def api_obter_requisicao(id):
    """API para buscar os dados de uma requisição para o modal de edição."""
    requisicao = obter_requisicao_por_id(id)
    if requisicao:
        return jsonify(dict(requisicao))
    return jsonify({'error': 'Requisição não encontrada'}), 404

# --- NOVAS APIs Notion-Like ---

@frota_bp.route('/api/notion/page', methods=['POST'])
@login_required
def api_create_page():
    dados = request.get_json()
    try:
        # A categoria deve ser enviada pelo frontend ('frota' ou 'historico')
        page_id = create_notion_page(
            session['user_id'],
            dados['category'],
            dados['title'],
            dados.get('content')
        )
        if page_id: return jsonify({'success': True, 'id': page_id})
        return jsonify({'success': False, 'error': 'Erro ao criar página'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@frota_bp.route('/api/notion/page/<int:id>', methods=['PUT', 'DELETE'])
@login_required
def api_manage_page(id):
    if request.method == 'PUT':
        dados = request.get_json()
        try:
            # Garante que o status seja enviado ou define como 'Ativa' se a página for de frota
            page = get_notion_page_by_id(id)
            if not page:
                return jsonify({'success': False, 'error': 'Página não encontrada'}), 404

            # Mantém o status atual se não for explicitamente enviado no payload
            status = dados.get('status') or page.get('status') 
            
            # Garante que 'Transferida' ou 'Manual' não sejam substituídos por 'Ativa'
            if page.get('category') == 'historico' and status == 'Ativa':
                 status = 'Manual'
            
            if update_notion_page(id, dados['title'], dados['content'], status):
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Erro ao atualizar página'}), 400
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400
    
    elif request.method == 'DELETE':
        try:
            if delete_notion_page(id):
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Página não encontrada'}), 404
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 400

@frota_bp.route('/api/notion/transfer/<int:id>', methods=['POST'])
@login_required
def api_transfer_page(id):
    """API para enviar uma página da Frota para o Histórico de Manutenções."""
    try:
        page = get_notion_page_by_id(id)
        if not page or page['category'] != 'frota':
            return jsonify({'success': False, 'error': 'Página de frota não encontrada ou já transferida'}), 404

        # Transfere a página para a categoria 'historico' e marca como 'Transferida'
        if transfer_notion_page(id, 'historico', 'Transferida'):
            return jsonify({'success': True, 'page_id': id})
        
        return jsonify({'success': False, 'error': 'Erro ao transferir a página'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500