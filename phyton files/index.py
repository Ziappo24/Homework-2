import os
import time
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.helpers import BulkIndexError
from docx import Document  # 🔹 Per leggere file Word (.docx)

# --- 1. Configurazione ---
INDEX_NAME = "file_indexer"
DIRECTORY_TO_INDEX = "C:\\Users\\EDOARDO\\Desktop\\UNI\\MAGISTRALE\\SECONDO ANNO\\PRIMO SEMESTRE\\INGEGNERIA DEI DATI\\Homework 2\\test files"

# Connessione a Elasticsearch
try:
    es = Elasticsearch("http://localhost:9200")
    es.info()
except Exception as e:
    print(f"❌ Errore: Impossibile connettersi a Elasticsearch.")
    print("➡️  Assicurati che sia in esecuzione su http://localhost:9200.")
    print(f"Dettagli: {e}")
    exit()


def create_index():
    """
    Crea o ricrea l'indice Elasticsearch con mapping e analyzer personalizzati.
    """
    if es.indices.exists(index=INDEX_NAME):
        print(f"L'indice '{INDEX_NAME}' esiste già. Lo elimino per ricrearlo.")
        es.indices.delete(index=INDEX_NAME)

    print(f"Creo l'indice '{INDEX_NAME}'...")

    index_settings = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "filename_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"]
                    },
                    "content_analyzer_italian": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "asciifolding",
                            "italian_stop",
                            "italian_stemmer"
                        ]
                    }
                },
                "filter": {
                    "italian_stop": {
                        "type": "stop",
                        "stopwords": "_italian_"
                    },
                    "italian_stemmer": {
                        "type": "stemmer",
                        "language": "italian"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "file_name": {
                    "type": "text",
                    "analyzer": "filename_analyzer",
                    
                },
                "content": {
                    "type": "text",
                    "analyzer": "content_analyzer_italian",
                    
                },
                "file_path": {"type": "keyword"},
                "file_size": {"type": "long"},
                "last_modified": {"type": "date"}
            }
        }
    }

    try:
        es.indices.create(index=INDEX_NAME, body=index_settings)
        print("✅ Indice creato con successo.")
    except Exception as e:
        print(f"❌ Errore durante la creazione dell'indice: {e}")
        exit()


def extract_text_from_docx(file_path):
    """
    Estrae tutto il testo da un file Word (.docx)
    """
    try:
        doc = Document(file_path)
        full_text = [p.text for p in doc.paragraphs]
        return "\n".join(full_text)
    except Exception as e:
        print(f"⚠️ Errore durante la lettura del file Word '{file_path.name}': {e}")
        return ""


def generate_actions(directory_path):
    """
    Generatore che legge e indicizza sia .txt che .docx
    """
    root_dir = Path(directory_path)
    file_paths = [p for p in root_dir.rglob("*") if p.suffix.lower() in [".txt", ".docx"]]

    file_count = 0
    for path in file_paths:
        try:
            # Lettura del contenuto a seconda dell'estensione
            if path.suffix.lower() == ".txt":
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
            elif path.suffix.lower() == ".docx":
                content = extract_text_from_docx(path)
            else:
                continue

            if not content.strip():
                print(f"⚠️ File vuoto o non leggibile: {path.name}")
                continue

            file_count += 1
            doc = {
                "_index": INDEX_NAME,
                "_id": str(path),
                "_source": {
                    "file_name": path.name,
                    "content": content,
                    "file_path": str(path.absolute()),
                    "file_size": path.stat().st_size,
                    "last_modified": path.stat().st_mtime * 1000
                }
            }
            yield doc

        except Exception as e:
            print(f"❌ Errore durante la lettura del file {path.name}: {e}")

    if file_count == 0:
        print(f"⚠️ Nessun file .txt o .docx trovato in {directory_path}")
    else:
        print(f"\n📄 Trovati {file_count} file (.txt e .docx) da indicizzare.")
    return file_count


def main():
    create_index()

    print(f"Avvio scansione dei file da: {DIRECTORY_TO_INDEX}")
    start_time = time.time()

    actions_generator = generate_actions(DIRECTORY_TO_INDEX)

    print("Avvio indicizzazione bulk in Elasticsearch...")
    try:
        success, failed = bulk(es, actions_generator, chunk_size=500, request_timeout=60)
        duration = time.time() - start_time
        print("\n--- Risultati Indicizzazione ---")
        print(f"✅ File indicizzati con successo: {success}")
        print(f"❌ File falliti: {failed}")
        print(f"⏱️  Tempo totale di indicizzazione: {duration:.2f} secondi")

    except BulkIndexError as e:
        print("\n❌ Errore durante l'indicizzazione bulk:")
        for error in e.errors:
            doc_info = error.get('index', {})
            path = doc_info.get('_id', 'sconosciuto')
            reason = doc_info.get('error', {}).get('reason', 'motivo non specificato')
            print(f"   ⚠️  {path} → {reason}")


if __name__ == "__main__":
    if not os.path.isdir(DIRECTORY_TO_INDEX):
        print(f"❌ Errore: la directory '{DIRECTORY_TO_INDEX}' non esiste.")
        print("Modifica la variabile 'DIRECTORY_TO_INDEX' nello script.")
    else:
        main()
