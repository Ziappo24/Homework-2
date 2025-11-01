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

# --- 2. Funzioni di parsing e ricerca ---

def normalize_text(text):
    """
    Normalizza il testo rimuovendo accenti e rendendolo minuscolo
    (utile per ricerche esatte più flessibili)
    """
    text = text.lower()
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return text


def parse_query(query_string):
    """
    Parsing migliorato per supportare diverse sintassi:
      nome "frase"
      contenuto "frase"
      contenuto parola1 parola2
      esatto "testo"
    """
    query_string = query_string.strip()

    # Permette sia virgolette che parole normali
    pattern = r'^(nome|contenuto|esatto)\s+(?:"([^"]+)"|(.+))$'
    match = re.match(pattern, query_string, re.IGNORECASE)

    if not match:
        return None, None, None

    command = match.group(1).lower()
    phrase_query = match.group(2)
    normal_query = match.group(3)

    # Determina il tipo di campo e ricerca
    if command == "nome":
        field = "file_name"
        search_type = "phrase" if phrase_query else "match"

    elif command == "contenuto":
        field = "content"
        search_type = "phrase" if phrase_query else "match"

    elif command == "esatto":
        field = "content.keyword"
        search_type = "exact"

    else:
        return None, None, None

    # Determina il termine di ricerca
    search_term = phrase_query if phrase_query else (normal_query.strip() if normal_query else "")
    if not search_term:
        return None, None, None

    return field, search_term, search_type


def execute_search(field, search_term, search_type):
    """
    Esegue la ricerca su Elasticsearch in base al tipo:
    - match        → ricerca normale
    - phrase       → frase esatta
    - exact        → corrispondenza letterale (campo keyword)
    """
    # Normalizzazione solo per tipo "exact"
    normalized_term = normalize_text(search_term) if search_type == "exact" else search_term

    if search_type == "phrase":
        query_body = {
            "query": {
                "match_phrase": {field: search_term}
            },
            "highlight": {"fields": {field: {}}}
        }

    elif search_type == "exact":
        # Usa match_phrase sul campo keyword per compatibilità e highlight
        query_body = {
            "query": {
                "match_phrase": {field: normalized_term}
            },
            "highlight": {"fields": {"content": {}}}
        }

    else:  # match normale
        query_body = {
            "query": {
                "match": {field: search_term}
            },
            "highlight": {"fields": {field: {}}}
        }

    try:
        response = es.search(index=INDEX_NAME, body=query_body)
        return response
    except Exception as e:
        print(f"❌ Errore durante la ricerca: {e}")
        return None


def print_results(response, field, search_term, search_type):
    """
    Stampa i risultati della ricerca in modo leggibile
    """
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
        if search_type == "exact":
            print("💡 Suggerimento: la ricerca esatta confronta il testo letterale (sensibile a spazi e punteggiatura).")
        return

    for i, hit in enumerate(hits):
        score = hit['_score']
        filename = hit['_source'].get('file_name', 'Sconosciuto')
        filepath = hit['_source'].get('file_path', 'N/D')

        print(f"\n{i+1}. {filename} (score: {score:.4f})")
        print(f"   📁 Percorso: {filepath}")

        # Evidenziazioni (se presenti)
        if 'highlight' in hit and hit['highlight']:
            print(f"   🔍 Evidenziazioni:")
            for field_name, snippets in hit['highlight'].items():
                for snippet in snippets:
                    print(f"      ...{snippet}...")

        # Contesto testuale aggiuntivo per le ricerche esatte
        if search_type == "exact" and 'content' in hit['_source']:
            content = normalize_text(hit['_source']['content'])
            term = normalize_text(search_term)
            indices = [m.start() for m in re.finditer(term, content)]

            if indices:
                for idx in indices[:2]:  # Mostra al massimo 2 contesti
                    start = max(0, idx - 80)
                    end = min(len(content), idx + len(term) + 80)
                    context = hit['_source']['content'][start:end]
                    print(f"   📍 Contesto: ...{context}...")
            else:
                print("   📍 Nessun contesto rilevato nel testo.")


def parse_and_search(query_string):
    """
    Funzione principale che unisce parsing e ricerca
    """
    field, search_term, search_type = parse_query(query_string)

    if not field:
        print("❌ Sintassi non valida.")
        print("\n📚 SINTASSI CORRETTE:")
        print('  nome "termine"                 - Ricerca per nome file (es: nome "report")')
        print('  contenuto "frase esatta"       - Ricerca frase nel contenuto')
        print('  contenuto termine              - Ricerca normale nel contenuto')
        print('  esatto "testo letterale"       - Ricerca ESATTA nel contenuto')
        print("\n💡 ESEMPI:")
        print('  nome "programmazione_base.txt"')
        print('  contenuto "linguaggio di programmazione"')
        print('  contenuto città')
        print('  esatto "database connection failed"')
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
    print('  contenuto "frase"              - Cerca frase esatta nel contenuto')
    print('  contenuto termine              - Ricerca normale nel contenuto')
    print('  esatto "testo"                 - Ricerca ESATTA letterale')
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
