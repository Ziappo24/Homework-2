import os
import time
from pathlib import Path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# --- 1. Configurazione ---
INDEX_NAME = "file_indexer"
DIRECTORY_TO_INDEX = "C:\\Users\\astor\\Desktop\\Homework2\\Homework 2 Elasticsearch\\Test Files"

# Connessione a Elasticsearch
try:
    es = Elasticsearch("http://localhost:9200")
    es.info()
except Exception as e:
    print(f"Errore: Impossibile connettersi a Elasticsearch.")
    print("Assicurati che sia in esecuzione su http://localhost:9200.")
    print(f"Dettagli: {e}")
    exit()

def create_index():
    """
    Crea l'indice con mapping semplificato e funzionante
    """
    if es.indices.exists(index=INDEX_NAME):
        print(f"L'indice '{INDEX_NAME}' esiste già. Lo elimino per ricrearlo.")
        es.indices.delete(index=INDEX_NAME)

    print(f"Creo l'indice '{INDEX_NAME}'...")

    # Configurazione SEMPLIFICATA e FUNZIONANTE
    index_settings = {
        "settings": {
            "analysis": {
                "analyzer": {
                    # Analyzer per il nome file
                    "filename_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding"]
                    },
                    # Analyzer per il contenuto italiano
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
                    "fields": {
                        "keyword": {
                            "type": "keyword"  # Per ricerche esatte sul nome
                        }
                    }
                },
                "content": {
                    "type": "text",
                    "analyzer": "content_analyzer_italian",
                    "fields": {
                        "keyword": {
                            "type": "keyword"  # Per ricerche esatte sul contenuto
                        }
                    }
                },
                "file_path": {
                    "type": "keyword"
                },
                "file_size": {
                    "type": "long"
                },
                "last_modified": {
                    "type": "date"
                }
            }
        }
    }

    try:
        es.indices.create(index=INDEX_NAME, body=index_settings)
        print("Indice creato con successo.")
    except Exception as e:
        print(f"Errore durante la creazione dell'indice: {e}")
        exit()
def generate_actions(directory_path):
    """
    Generatore migliorato che include metadati aggiuntivi.
    """
    root_dir = Path(directory_path)
    file_paths = root_dir.rglob("*.txt")
    
    file_count = 0
    for path in file_paths:
        try:
            # Leggi il contenuto e i metadati del file
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            file_count += 1
            
            # Prepara il documento per Elasticsearch
            doc = {
                "_index": INDEX_NAME,
                "_id": str(path),
                "_source": {
                    "file_name": path.name,
                    "content": content,
                    "file_path": str(path.absolute()),
                    "file_size": path.stat().st_size,  # AGGIUNTO
                    "last_modified": path.stat().st_mtime * 1000  # AGGIUNTO (convertito in ms)
                }
            }
            yield doc
            
        except Exception as e:
            print(f"Errore durante la lettura del file {path}: {e}")
            
    if file_count == 0:
        print(f"Attenzione: Nessun file .txt trovato in {directory_path}")
    
    print(f"\nTrovati {file_count} file .txt da indicizzare.")
    return file_count


# --- 4. Esecuzione ---

def main():
    # 1. Crea (o ricrea) l'indice e i suoi mapping
    create_index()

    # 2. Prepara i dati
    print(f"Avvio scansione e preparazione dei file da: {DIRECTORY_TO_INDEX}")
    start_time = time.time()
    
    # Crea il generatore di azioni
    actions_generator = generate_actions(DIRECTORY_TO_INDEX)

    # 3. Esegui l'indicizzazione Bulk (molto più veloce di un file alla volta)
    print("Avvio indicizzazione bulk in Elasticsearch...")
    try:
        # bulk() consuma il generatore e invia i dati
        success, failed = bulk(es, actions_generator, chunk_size=500, request_timeout=60)
        
        end_time = time.time()
        duration = end_time - start_time

        print("--- Risultati Indicizzazione ---")
        print(f"File indicizzati con successo: {success}")
        print(f"File falliti: {failed}")
        print(f"Tempo totale di indicizzazione: {duration:.2f} secondi")

    except Exception as e:
        print(f"Errore durante l'indicizzazione bulk: {e}")


if __name__ == "__main__":
    if not os.path.isdir(DIRECTORY_TO_INDEX):
        print(f"Errore: La directory '{DIRECTORY_TO_INDEX}' non esiste.")
        print("Modifica la variabile 'DIRECTORY_TO_INDEX' nello script.")
    else:
        main()