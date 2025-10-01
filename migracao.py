import sqlite3

def adicionar_coluna():
    conn = None
    try:
        conn = sqlite3.connect('abastecimentos.db')
        cursor = conn.cursor()
        
        print("Adicionando a coluna 'criado_por_id' à tabela 'cotacoes'...")
        
        # O IGNORE previne erro caso a coluna já exista
        cursor.execute("ALTER TABLE cotacoes ADD COLUMN criado_por_id INTEGER REFERENCES users(id)")
        
        conn.commit()
        print("Coluna 'criado_por_id' adicionada com sucesso!")
        
    except sqlite3.OperationalError as e:
        # Se a coluna já existir, o SQLite pode lançar um erro.
        if "duplicate column name" in str(e):
            print("A coluna 'criado_por_id' já existe na tabela 'cotacoes'. Nenhuma alteração foi feita.")
        else:
            print(f"Ocorreu um erro de SQL: {e}")
            
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    adicionar_coluna()