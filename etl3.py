import psycopg2
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from uuid import uuid4

# CONFIGURAÇÕES DE CONEXÃO
postgres_config = {
    'dbname': 'Faculdade',
    'user': 'neon',
    'password': '1234',
    'host': 'localhost',
    'port': 5432
}

cassandra_config = {
    'hosts': ['127.0.0.1'],
    'keyspace': 'universidade',
}

# FUNÇÃO PARA EXTRAIR DADOS DO POSTGRES
def extract_postgres(query):
    with psycopg2.connect(**postgres_config) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            results = []
            for row in cur.fetchall():
                results.append(dict(zip(columns, row)))
            return results

# FUNÇÃO PARA INSERIR DADOS NO CASSANDRA
def insert_cassandra(session, table, columns, values):
    placeholders = ','.join(['%s'] * len(values))
    cql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    session.execute(SimpleStatement(cql), values)

# MAIN
def main():
    # 1. Conecta ao Cassandra
    cluster = Cluster(cassandra_config['hosts'])
    session = cluster.connect(cassandra_config['keyspace'])

    # Exemplo: Migração de alunos
    print("Migrando alunos...")
    alunos = extract_postgres("SELECT * FROM alunos")
    for aluno in alunos:
        # Se precisar gerar UUID novo: aluno_id = uuid4()
        values = (
            aluno['aluno_id'],
            aluno['nome'],
            aluno['data_nascimento'],
            aluno['matricula'],
            aluno['curso_id'],
            aluno['ano_ingresso'],
            aluno['semestre_ingresso']
        )
        insert_cassandra(session, 'alunos',
            ['aluno_id','nome','data_nascimento','matricula','curso_id','ano_ingresso','semestre_ingresso'], 
            values
        )

    print("Migrando professores...")
    professores = extract_postgres("SELECT * FROM professores")
    for prof in professores:
        values = (
            prof['professor_id'],
            prof['nome'],
            prof['departamento_id'],
            prof['chefe_departamento']
        )
        insert_cassandra(session, 'professores',
            ['professor_id','nome','departamento_id','chefe_departamento'],
            values
        )

    print("Migrando departamentos...")
    departamentos = extract_postgres("SELECT * FROM departamentos")
    for dep in departamentos:
        values = (
            dep['departamento_id'],
            dep['nome'],
            dep['chefe_professor_id']
        )
        insert_cassandra(session, 'departamentos', 
            ['departamento_id','nome','chefe_professor_id'],
            values
        )

    print("Migrando cursos...")
    cursos = extract_postgres("SELECT * FROM cursos")
    for curso in cursos:
        values = (
            curso['curso_id'],
            curso['nome'],
            curso['departamento_id']
        )
        insert_cassandra(session, 'cursos', 
            ['curso_id','nome','departamento_id'],
            values
        )

    print("Migrando disciplinas...")
    disciplinas = extract_postgres("SELECT * FROM disciplinas")
    for disc in disciplinas:
        values = (
            disc['disciplina_id'],
            disc['nome'],
            disc['curso_id'],
            disc['departamento_id'],
            disc['semestre_oferta']
        )
        insert_cassandra(session, 'disciplinas',
            ['disciplina_id','nome','curso_id','departamento_id','semestre_oferta'],
            values
        )

    print("Migrando matriz_curricular...")
    matriz = extract_postgres("SELECT * FROM matriz_curricular")
    for linha in matriz:
        values = (
            linha['curso_id'],
            linha['disciplina_id'],
            linha['obrigatoria'],
            linha['semestre']
        )
        insert_cassandra(session, 'matriz_curricular',
            ['curso_id','disciplina_id','obrigatoria','semestre'],
            values
        )

    print("Migrando histórico dos alunos...")
    historico = extract_postgres("SELECT * FROM historico_aluno")
    for hist in historico:
        values = (
            hist['aluno_id'],
            hist['ano'],
            hist['semestre'],
            hist['disciplina_id'],
            hist['nome_disciplina'],
            hist['nota_final'],
            hist['situacao']
        )
        insert_cassandra(session, 'historico_aluno',
            ['aluno_id','ano','semestre','disciplina_id','nome_disciplina','nota_final','situacao'],
            values
        )

    print("Migrando disciplinas ministradas...")
    ministrada = extract_postgres("SELECT * FROM disciplinas_ministradas")
    for dm in ministrada:
        values = (
            dm['professor_id'],
            dm['ano'],
            dm['semestre'],
            dm['disciplina_id'],
            dm['nome_disciplina'],
        )
        insert_cassandra(session, 'disciplinas_ministradas',
            ['professor_id','ano','semestre','disciplina_id','nome_disciplina'],
            values
        )

    print("Migrando alunos formados...")
    formados = extract_postgres("SELECT * FROM alunos_formados")
    for f in formados:
        values = (
            f['ano_formatura'],
            f['semestre_formatura'],
            f['curso_id'],
            f['aluno_id'],
            f['nome_aluno']
        )
        insert_cassandra(session, 'alunos_formados',
            ['ano_formatura','semestre_formatura','curso_id','aluno_id','nome_aluno'],
            values
        )

    print("Migrando grupos de TCC...")
    grupos = extract_postgres("SELECT * FROM grupos_tcc")
    for g in grupos:
        # Supondo que 'alunos' está como array/lista no Postgres
        values = (
            g['grupo_id'],
            g['titulo'],
            g['orientador_id'],
            g['nome_orientador'],
            g['curso_id'],
            g['ano'],
            g['semestre'],
            g['alunos'],  # Deve ser passado como list/array em Python!
        )
        insert_cassandra(session, 'grupos_tcc',
            ['grupo_id','titulo','orientador_id','nome_orientador','curso_id','ano','semestre','alunos'],
            values
        )

    print("Migração concluída.")

if __name__ == '__main__':
    main()
