# routes/auth.py

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, g, jsonify
from werkzeug.security import check_password_hash
from database import get_user_by_username, get_all_users, get_user_by_id, create_user, update_user, delete_user
from utils import login_required, roles_required

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if g.user:
        return redirect(url_for('frota.index')) 
    
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
            return redirect(url_for('frota.index')) 
        else:
            flash('Usuário ou senha inválidos.', 'danger')
    
    return render_template('login.html', active_page='login')

@auth_bp.route('/logout')
@login_required
def logout():
    session.clear()
    flash('Você foi desconectado.', 'info')
    return redirect(url_for('auth.login')) 

@auth_bp.route('/admin/users', methods=['GET'])
@login_required
@roles_required(['Gestor'])
def user_management():
    users = get_all_users()
    roles = ['Administrador', 'Gestor', 'Comprador', 'Padrão']
    return render_template('user_management.html', active_page='user_management', users=users, roles=roles)

# --- APIs de Usuário ---

@auth_bp.route('/api/users', methods=['POST'])
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

@auth_bp.route('/api/users/<int:user_id>', methods=['GET', 'PUT', 'DELETE'])
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