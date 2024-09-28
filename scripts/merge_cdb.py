import requests
import sqlite3
import os
import glob
import shutil

# URLs de los repositorios
cards_cdb_url = "https://github.com/purerosefallen/ygopro/raw/server/cards.cdb"
cdb_repo_url = "https://code.moenext.com/mycard/pre-release-database-cdb"

# Directorios
download_dir = "./downloads"
merged_db_path = "./merged_updates.cdb"
cards_db_path = f"{download_dir}/cards.cdb"

# Crear directorio de descargas si no existe
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Función para descargar un archivo
def download_file(url, dest_path):
    print(f"Descargando {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, "wb") as f:
            shutil.copyfileobj(response.raw, f)
        print(f"Archivo descargado: {dest_path}")
    else:
        print(f"Error al descargar {url}: {response.status_code}")

# Descargar el archivo cards.cdb
download_file(cards_cdb_url, cards_db_path)

# Descargar todos los archivos del segundo repositorio
# Esto es un ejemplo. En este caso necesitas descargar manualmente los archivos de https://code.moenext.com/mycard/pre-release-database-cdb
# Si los archivos están accesibles de forma directa puedes automatizar la descarga.

# Unir todas las bases de datos en una
def merge_databases(cards_db, output_db, cdb_files):
    # Conexión a la base de datos principal
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()

    # Crear las tablas en la nueva base de datos si no existen
    cursor.execute('''CREATE TABLE IF NOT EXISTS datas (
                        id INTEGER PRIMARY KEY,
                        ot INTEGER,
                        alias INTEGER,
                        setcode INTEGER,
                        type INTEGER,
                        atk INTEGER,
                        def INTEGER,
                        level INTEGER,
                        race INTEGER,
                        attribute INTEGER,
                        category INTEGER
                    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS texts (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        desc TEXT,
                        str1 TEXT,
                        str2 TEXT,
                        str3 TEXT,
                        str4 TEXT,
                        str5 TEXT,
                        str6 TEXT,
                        str7 TEXT,
                        str8 TEXT,
                        str9 TEXT,
                        str10 TEXT,
                        str11 TEXT,
                        str12 TEXT,
                        str13 TEXT,
                        str14 TEXT,
                        str15 TEXT,
                        str16 TEXT
                    )''')

    # Copiar los datos del archivo cards.cdb
    copy_data_from_db(cards_db, conn, cursor)

    # Mergear los datos de otros archivos .cdb
    for cdb_file in cdb_files:
        print(f"Mergeando datos desde {cdb_file}...")
        copy_data_from_db(cdb_file, conn, cursor)

    # Guardar y cerrar la conexión
    conn.commit()
    conn.close()

def copy_data_from_db(source_db_path, dest_conn, dest_cursor):
    source_conn = sqlite3.connect(source_db_path)
    source_cursor = source_conn.cursor()

    # Obtener todos los ids de la tabla `datas` de la base de datos destino para evitar duplicados
    dest_cursor.execute("SELECT id FROM datas")
    existing_ids = set(row[0] for row in dest_cursor.fetchall())

    # Insertar datos de la tabla `datas`
    source_cursor.execute("SELECT * FROM datas")
    for row in source_cursor.fetchall():
        if row[0] not in existing_ids:  # Verifica si el ID ya existe
            dest_cursor.execute("INSERT INTO datas VALUES (?,?,?,?,?,?,?,?,?,?,?)", row)

    # Insertar datos de la tabla `texts` relacionados con los IDs de `datas`
    source_cursor.execute("SELECT * FROM texts")
    for row in source_cursor.fetchall():
        if row[0] not in existing_ids:  # Asegurarse de que solo se inserten los textos de los IDs nuevos
            dest_cursor.execute("INSERT INTO texts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)

    source_conn.close()

# Obtener la lista de archivos .cdb descargados
cdb_files = glob.glob(f"{download_dir}/*.cdb")

# Llamar al merge de bases de datos
merge_databases(cards_db_path, merged_db_path, cdb_files)

print(f"Merge completado. Archivo generado: {merged_db_path}")





































