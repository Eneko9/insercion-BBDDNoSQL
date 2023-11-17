from neo4j import GraphDatabase
import csv
import uuid

# Conexión a la base de datos Neo4j
uri = "bolt://localhost:7687"  
username = "neo4j"  
password = "master22"
driver = GraphDatabase.driver(uri, auth=(username, password))

csv_file_path = 'books.csv'

def load_data_to_neo4j(tx, isbn, titulo, autor, anio_edicion):
    # Crear nodo Autor
    tx.run("MERGE (a:Autor {id_autor: $id_autor, nombre_autor: $nombre_autor})", id_autor=str(uuid.uuid4()), nombre_autor=autor)

    # Crear nodo Libro
    tx.run("CREATE (l:Libro {isbn: $isbn, titulo: $titulo, anio_edicion: $anio_edicion})", isbn=isbn, titulo=titulo, anio_edicion=anio_edicion)

    # Crear relación entre Autor y Libro
    tx.run("MATCH (a:Autor), (l:Libro) WHERE a.nombre_autor = $nombre_autor AND l.isbn = $isbn CREATE (a)-[:ESCRIBIO]->(l)", nombre_autor=autor, isbn=isbn)

with open(csv_file_path, 'r', encoding='utf-8', errors='replace') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=';')
    next(csvreader)  
    for i in range(100):  # Insertar los primeros 100 registros
        row = next(csvreader)

        isbn, titulo, autor, anio_edicion, *_ = row

        with driver.session() as session:
            session.write_transaction(load_data_to_neo4j, isbn, titulo, autor, int(anio_edicion))

# Cerrar la conexión
driver.close()
