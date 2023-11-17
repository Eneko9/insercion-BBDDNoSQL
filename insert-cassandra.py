from cassandra.cluster import Cluster
import csv
import uuid

cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('libros')

# Ruta al archivo CSV
csv_file_path = 'books.csv'

# Leer datos desde el archivo CSV e insertar en Cassandra
with open(csv_file_path, 'r', encoding='utf-8', errors='replace') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=';')
    next(csvreader)  # Saltar la fila de encabezado
    for i in range(100):  # Insertar los primeros 100 registros
        row = next(csvreader)

        # Los valores en 'row' están separados por ';'
        isbn, titulo, autor, anio_edicion, *_ = row

        # Generar un UUID para el autor
        id_autor = str(uuid.uuid4())

        # Insertar autor
        session.execute("INSERT INTO autor (id_autor, nombre_autor) VALUES ({0}, '{1}')".format(id_autor, autor.replace("'", "''")))

        # Insertar libro
        session.execute("INSERT INTO libro (isbn, titulo, anio_edicion) VALUES ({0}, '{1}', {2})".format(int(isbn), titulo.replace("'", "''"), int(anio_edicion)))

        # Insertar relación autor-libro
        session.execute("INSERT INTO autor_isbn (id_autor, isbn) VALUES ({0}, {1})".format(id_autor, int(isbn)))

# Cerrar la conexión
cluster.shutdown()
