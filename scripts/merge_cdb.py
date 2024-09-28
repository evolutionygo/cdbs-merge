import os
import subprocess
import sqlite3
import requests
import shutil

# URL del repositorio de destino
REPO_URL = "https://x-access-token:{token}@github.com/termitaklk/CDB-MERGES.git"

# Directorios
download_dir = "./downloads"
merged_db_path = "./merged_updates.cdb"
cards_db_url = "https://github.com/purerosefallen/ygopro/raw/server/cards.cdb"
cards_db_path = f"{download_dir}/cards.cdb"

# Crear el directorio de descargas si no existe
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Función para ejecutar comandos de shell
def run_command(command):
    process = subprocess.run(command, shell=True, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if process.returncode != 0:
        print(f"Error al ejecutar el comando: {command}")
        print(process.stderr)
    else:
        print(process.stdout)

# Descargar el archivo cards.cdb
def download_file(url, dest_path):
    print(f"Descargando {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, "wb") as f:
            shutil.copyfileobj(response.raw, f)
        print(f"Archivo descargado: {dest_path}")
    else:
        print(f"Error al descargar {url}: {response.status_code}")

# Merge de todas las bases de datos
def merge_databases(cards_db, output_db, cdb_files):
    conn = sqlite3.connect(output_db)
    cursor = conn.cursor()

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

    copy_data_from_db(cards_db, conn, cursor)

    for cdb_file in cdb_files:
        print(f"Mergeando datos desde {cdb_file}...")
        copy_data_from_db(cdb_file, conn, cursor)

    conn.commit()
    conn.close()

# Copiar datos desde la base de datos fuente a la destino
def copy_data_from_db(source_db_path, dest_conn, dest_cursor):
    source_conn = sqlite3.connect(source_db_path)
    source_cursor = source_conn.cursor()

    dest_cursor.execute("SELECT id FROM datas")
    existing_ids = set(row[0] for row in dest_cursor.fetchall())

    source_cursor.execute("SELECT * FROM datas")
    for row in source_cursor.fetchall():
        if row[0] not in existing_ids:
            dest_cursor.execute("INSERT INTO datas VALUES (?,?,?,?,?,?,?,?,?,?,?)", row)

    source_cursor.execute("SELECT * FROM texts")
    for row in source_cursor.fetchall():
        if row[0] not in existing_ids:
            dest_cursor.execute("INSERT INTO texts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)

    source_conn.close()

# Función para hacer commit y push
def commit_and_push():
    token = os.getenv("CDB_TOKEN")  # Obtener el token de la variable de entorno
    repo_url = REPO_URL.format(token=token)

    run_command("git add merged_updates.cdb")
    run_command('git commit -m "Updated cards.cdb"')

    # Cambiar la URL remota para incluir el token de acceso personal
    run_command(f"git remote set-url origin {repo_url}")

    # Hacer push al repositorio de destino
    run_command("git push origin main")

# Descargar el archivo cards.cdb
download_file(cards_db_url, cards_db_path)

# Obtener los archivos .cdb descargados manualmente en ./downloads
cdb_files = [f for f in os.listdir(download_dir) if f.endswith(".cdb")]

# Mergear los archivos .cdb
merge_databases(cards_db_path, merged_db_path, cdb_files)

# Hacer commit y push del archivo mergeado al repositorio
commit_and_push()






































