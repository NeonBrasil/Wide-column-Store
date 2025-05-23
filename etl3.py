import psycopg2
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
from uuid import uuid4
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def extract_postgres(query):
    """Extrai dados do PostgreSQL com tratamento de erro"""
    try:
        with psycopg2.connect(**postgres_config) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                results = []
                for row in cur.fetchall():
                    results.append(dict(zip(columns, row)))
                logger.info(f"Extraídos {len(results)} registros")
                return results
    except Exception as e:
        logger.error(f"Erro ao extrair dados do PostgreSQL: {e}")
        raise

def insert_cassandra_batch(session, table, columns, rows_data, batch_size=100):
    """Insere dados no Cassandra usando batches para melhor performance"""
    if not rows_data:
        logger.warning(f"Nenhum dado para inserir na tabela {table}")
        return
    
    placeholders = ','.join(['?' for _ in columns])  # Usar ? ao invés de %s
    cql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    prepared = session.prepare(cql)
    
    total_rows = len(rows_data)
    processed = 0
    
    try:
        for i in range(0, total_rows, batch_size):
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch_data = rows_data[i:i + batch_size]
            
            for row_data in batch_data:
                batch.add(prepared, row_data)
            
            session.execute(batch)
            processed += len(batch_data)
            logger.info(f"Inseridos {processed}/{total_rows} registros em {table}")
            
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {table}: {e}")
        raise

def migrate_table(session, table_name, postgres_query, columns, data_transformer=None):
    """Função genérica para migrar uma tabela"""
    logger.info(f"Migrando {table_name}...")
    
    try:
        # Extrair dados do PostgreSQL
        data = extract_postgres(postgres_query)
        
        if not data:
            logger.warning(f"Nenhum dado encontrado para {table_name}")
            return
        
        # Transformar dados se necessário
        rows_data = []
        for item in data:
            if data_transformer:
                row_values = data_transformer(item)
            else:
                row_values = tuple(item[col] for col in columns)
            rows_data.append(row_values)
        
        # Inserir no Cassandra
        insert_cassandra_batch(session, table_name, columns, rows_data)
        logger.info(f"Migração de {table_name} concluída com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro na migração de {table_name}: {e}")
        raise

def main():
    cluster = None
    session = None
    
    try:
        # Conectar ao Cassandra
        cluster = Cluster(cassandra_config['hosts'])
        session = cluster.connect(cassandra_config['keyspace'])
        logger.info("Conectado ao Cassandra")
        
        # Definir migrações
        migrations = [
            {
                'table': 'alunos',
                'query': "SELECT * FROM alunos",
                'columns': ['aluno_id','nome','data_nascimento','matricula','curso_id','ano_ingresso','semestre_ingresso']
            },
            {
                'table': 'professores',
                'query': "SELECT * FROM professores",
                'columns': ['professor_id','nome','departamento_id','chefe_departamento']
            },
            {
                'table': 'departamentos',
                'query': "SELECT * FROM departamentos",
                'columns': ['departamento_id','nome','chefe_professor_id']
            },
            {
                'table': 'cursos',
                'query': "SELECT * FROM cursos",
                'columns': ['curso_id','nome','departamento_id']
            },
            {
                'table': 'disciplinas',
                'query': "SELECT * FROM disciplinas",
                'columns': ['disciplina_id','nome','curso_id','departamento_id','semestre_oferta']
            },
            {
                'table': 'matriz_curricular',
                'query': "SELECT * FROM matriz_curricular",
                'columns': ['curso_id','disciplina_id','obrigatoria','semestre']
            },
            {
                'table': 'historico_aluno',
                'query': "SELECT * FROM historico_aluno",
                'columns': ['aluno_id','ano','semestre','disciplina_id','nome_disciplina','nota_final','situacao']
            },
            {
                'table': 'disciplinas_ministradas',
                'query': "SELECT * FROM disciplinas_ministradas",
                'columns': ['professor_id','ano','semestre','disciplina_id','nome_disciplina']
            },
            {
                'table': 'alunos_formados',
                'query': "SELECT * FROM alunos_formados",
                'columns': ['ano_formatura','semestre_formatura','curso_id','aluno_id','nome_aluno']
            }
        ]
        
        # Executar migrações
        for migration in migrations:
            migrate_table(
                session, 
                migration['table'], 
                migration['query'], 
                migration['columns']
            )
        
        # Migração especial para grupos_tcc (devido ao array)
        logger.info("Migrando grupos_tcc...")
        grupos = extract_postgres("SELECT * FROM grupos_tcc")
        
        if grupos:
            rows_data = []
            for g in grupos:
                # Garantir que 'alunos' seja uma lista
                alunos_list = g['alunos']
                if isinstance(alunos_list, str):
                    # Se vier como string, converter para lista
                    alunos_list = alunos_list.strip('{}').split(',') if alunos_list else []
                
                values = (
                    g['grupo_id'],
                    g['titulo'],
                    g['orientador_id'],
                    g['nome_orientador'],
                    g['curso_id'],
                    g['ano'],
                    g['semestre'],
                    alunos_list
                )
                rows_data.append(values)
            
            insert_cassandra_batch(
                session, 
                'grupos_tcc',
                ['grupo_id','titulo','orientador_id','nome_orientador','curso_id','ano','semestre','alunos'],
                rows_data
            )
        
        logger.info("Migração concluída com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro durante a migração: {e}")
        raise
        
    finally:
        # Fechar conexões
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()
        logger.info("Conexões fechadas")

if __name__ == '__main__':
    main()
