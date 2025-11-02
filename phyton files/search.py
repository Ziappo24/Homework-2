import sys
import re
import unicodedata
from elasticsearch import Elasticsearch

# --- 1. Configurazione ---
INDEX_NAME = "file_indexer"

# Connessione a Elasticsearch
try:
    es = Elasticsearch("http://localhost:9200")
    if not es.indices.exists(index=INDEX_NAME):
        print(f"❌ Errore: L'indice '{INDEX_NAME}' non esiste.")
        print("➡️  Esegui prima lo script 'index.py' per creare e popolare l'indice.")
        sys.exit()
except Exception as e:
    print(f"❌ Errore: Impossibile connettersi a Elasticsearch.")
    print("➡️  Assicurati che sia in esecuzione su http://localhost:9200.")
    print(f"Dettagli: {e}")
    sys.exit()

# --- 2. Funzioni di supporto ---

def normalize_text(text):
    """Rimuove accenti e converte in minuscolo"""
    text = text.lower()
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return text


def parse_query(query_string):
    """
    Parsing per supportare:
      nome "frase"
      nome parola
      contenuto "frase esatta"
      contenuto parola
    """
    query_string = query_string.strip()
    pattern = r'^(nome|contenuto)\s+(?:"([^"]+)"|(.+))$'
    match = re.match(pattern, query_string, re.IGNORECASE)

    if not match:
        return None, None, None

    command = match.group(1).lower()
    phrase_query = match.group(2)
    normal_query = match.group(3)
    search_term = phrase_query if phrase_query else normal_query.strip()

    if command == "nome":
        field = "file_name"
    elif command == "contenuto":
        field = "content"
    else:
        return None, None, None

    # Se l’utente ha usato virgolette, facciamo una phrase query
    search_type = "phrase" if phrase_query else "match"

    return field, search_term, search_type


def execute_search(field, search_term, search_type):
    """Esegue la ricerca su Elasticsearch"""
    if search_type == "phrase":
        query_body = {
            "query": {"match_phrase": {field: search_term}},
            "highlight": {"fields": {field: {}}}
        }
    else:  # match normale
        query_body = {
            "query": {"match": {field: search_term}},
            "highlight": {"fields": {field: {}}}
        }

    try:
        response = es.search(index=INDEX_NAME, body=query_body)
        return response
    except Exception as e:
        print(f"❌ Errore durante la ricerca: {e}")
        return None


def print_results(response, field, search_term, search_type):
    """Mostra i risultati in modo leggibile"""
    if not response:
        print("⚠️ Nessuna risposta da Elasticsearch.")
        return

    hits = response['hits']['hits']
    total_hits = response['hits']['total']['value']

    print("=" * 60)
    print(f"🔍 TIPO: {search_type.upper()} | CAMPO: {field}")
    print(f"🔎 RICERCA: '{search_term}'")
    print(f"📄 RISULTATI: {total_hits}")
    print("=" * 60)

    if total_hits == 0:
        print("Nessun documento trovato.")
        if search_type == "phrase":
            print("💡 Suggerimento: le phrase query cercano la frase esatta nel testo.")
        return

    for i, hit in enumerate(hits):
        score = hit['_score']
        filename = hit['_source'].get('file_name', 'Sconosciuto')
        filepath = hit['_source'].get('file_path', 'N/D')

        print(f"\n{i+1}. {filename} (score: {score:.4f})")
        print(f"   📁 Percorso: {filepath}")

        if 'highlight' in hit and hit['highlight']:
            print("   🔍 Evidenziazioni:")
            for field_name, snippets in hit['highlight'].items():
                for snippet in snippets:
                    print(f"      ...{snippet}...")


def parse_and_search(query_string):
    """Gestisce parsing e ricerca"""
    field, search_term, search_type = parse_query(query_string)

    if not field:
        print("❌ Sintassi non valida.")
        print("\n📚 SINTASSI CORRETTE:")
        print('  nome "termine"                 - Cerca nel nome del file (es: nome "report")')
        print('  nome parola                    - Cerca nomi contenenti la parola indicata')
        print('  contenuto "frase esatta"        - Cerca una frase precisa nel testo')
        print('  contenuto parola                - Ricerca normale nel testo')
        return

    print(f"🔍 Eseguo ricerca: {search_type} su {field} → '{search_term}'")
    response = execute_search(field, search_term, search_type)
    print_results(response, field, search_term, search_type)


# --- 3. Interfaccia principale ---
def main():
    print("🔎 === SISTEMA DI RICERCA FILE ===")
    print("💡 Digita 'esci' per uscire.")
    print("\n📚 SINTASSI:")
    print('  nome "termine"                 - Cerca nel nome file')
    print('  nome parola                    - Cerca nomi contenenti una parola')
    print('  contenuto "frase"              - Cerca una frase esatta nel contenuto')
    print('  contenuto parola               - Ricerca normale nel contenuto')
    print("-" * 60)

    while True:
        try:
            query = input("\n🔍 Query> ").strip()
            if query.lower() in ['esci', 'exit', 'quit']:
                print("👋 Arrivederci!")
                break
            if not query:
                continue
            parse_and_search(query)

        except KeyboardInterrupt:
            print("\n👋 Interrotto dall'utente. Uscita...")
            break
        except Exception as e:
            print(f"❌ Errore imprevisto: {e}")


if __name__ == "__main__":
    main()
