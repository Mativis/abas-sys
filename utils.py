# utils.py

from flask import g, session, redirect, url_for, flash
from functools import wraps
from database import get_user_by_id
from datetime import datetime

# --- Context Processor ---
def inject_now():
    """Injeta a data/hora atual e o objeto global g nos templates."""
    return {'now': datetime.now(), 'g': g}

# --- Session Management (g.user loading) ---
def load_logged_in_user():
    """
    Carrega o usuário logado para o objeto global 'g'.
    """
    user_id = session.get('user_id')
    g.user = get_user_by_id(user_id) if user_id else None

# --- Decorators ---
def login_required(view):
    """Decorator para exigir que o usuário esteja logado."""
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if g.user is None:
            # Redireciona para a rota de login no novo Blueprint 'auth'
            flash('Você precisa estar logado para acessar esta página.', 'warning')
            return redirect(url_for('auth.login')) 
        return view(*args, **kwargs)
    return wrapped_view

def roles_required(roles):
    """Decorator para restringir acesso a certas roles."""
    def wrapper(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if g.user is None:
                # Redireciona para a rota de login no novo Blueprint 'auth'
                flash('Você precisa estar logado para acessar esta página.', 'warning')
                return redirect(url_for('auth.login')) 
            
            # O Administrador tem acesso a todas as páginas restritas por role
            if g.user['role'] not in roles and g.user['role'] != 'Administrador':
                # Redireciona para a nova rota de "início" (frota.index)
                flash('Acesso negado. Sua função não permite acessar esta página.', 'danger')
                return redirect(url_for('frota.index'))
            return view(*args, **kwargs)
        return wrapped_view
    return wrapper