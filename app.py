# app.py

from flask import Flask
from database import criar_tabelas
from utils import load_logged_in_user, inject_now
from auth import auth_bp
from frota import frota_bp
from dealer import dealer_bp
import os

# --- Configuração Inicial ---
app = Flask(__name__)
app.secret_key = 'sua_chave_secreta_aqui_123'
app.config['STATIC_FOLDER'] = 'static'

# Define o caminho absoluto para a pasta de uploads (Replica a configuração original)
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# --- Registro de Blueprints (Módulos) ---

# Rotas de Autenticação e Administração (usuários)
# Rotas: /login, /logout, /admin/users, /api/users
app.register_blueprint(auth_bp)

# Rotas do Módulo Dealer/Compras
# Rotas: /dealers/cotacoes-relatorio, /dealers/pedido/<id>, etc.
# O prefixo '/dealers' é definido dentro do Blueprint em routes/dealer.py
app.register_blueprint(dealer_bp)

# Rotas Principais (Frota, Index, APIs de Frota)
# Rotas: /, /relatorios, /manutencoes, /api/dashboard, /api/registros, etc.
app.register_blueprint(frota_bp)

# --- Controles de Acesso e Contexto ---
app.before_request(load_logged_in_user)
app.context_processor(inject_now)

# --- Inicialização ---
if __name__ == '__main__':
    # Cria as tabelas se não existirem
    criar_tabelas() 
    app.run(debug=True, host='0.0.0.0', port='5005')