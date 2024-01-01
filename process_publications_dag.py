from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import psycopg2.extras
import json
import requests
import logging
import time
import os

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
db_params = {
    "database": "dataengineering2",
    "user": "postgres",
    "password": "postgres",
    "host": "172.30.80.1",
    "port": "5432"
}


### DATA INSERTION TASK

def get_or_insert_author_id(cursor, author_name):
    cursor.execute("SELECT id FROM authors WHERE name = %s;", (author_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO authors (name, affiliation) VALUES (%s, 'Unknown') RETURNING id;", (author_name,))
        return cursor.fetchone()[0]

def get_or_insert_category_id(cursor, category_name):
    cursor.execute("SELECT id FROM categories WHERE category_name = %s;", (category_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("INSERT INTO categories (category_name) VALUES (%s) RETURNING id;", (category_name,))
        return cursor.fetchone()[0]

def insert_data():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                # Load and process JSON data
                try:
                    with open('/mnt/c/Users/Autre/Desktop/dataengineering/dataset.json', 'r') as file:
                        data = json.load(file)
                except FileNotFoundError as e:
                    logging.error(f"JSON file not found: {e}")
                    return  # Exit the function if file not found
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON: {e}")
                    return  # Exit the function if JSON is invalid

                # Insert JSON data into the database
                for item in data:
                    try:
                        current_date = datetime.now().date()
                        cursor.execute("""
                            INSERT INTO publications (submitter, title, comments, journal_ref, doi, report_no, categories, license, abstract, update_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (doi) DO NOTHING
                            RETURNING id;
                        """, (item["submitter"], item["title"], item["comments"], item["journal-ref"], item["doi"], item["report-no"], item["categories"], item["license"], item["abstract"], current_date))
                        publication_id = cursor.fetchone()[0] if cursor.rowcount else None

                        if publication_id:
                            authors_data = [(get_or_insert_author_id(cursor, name), publication_id) for name in item["authors"].split(', ')]
                            psycopg2.extras.execute_values(cursor, "INSERT INTO authorship (author_id, publication_id) VALUES %s ON CONFLICT DO NOTHING;", authors_data)
                            
                            categories_data = [(publication_id, get_or_insert_category_id(cursor, category)) for category in item["categories"].split()]
                            psycopg2.extras.execute_values(cursor, "INSERT INTO publication_category (publication_id, category_id) VALUES %s ON CONFLICT DO NOTHING;", categories_data)

                    except psycopg2.Error as e:
                        logging.error(f"Database error while processing item {item['doi']}: {e}")
                        conn.rollback()  # Rolling back the transaction in case of an error
                        continue  # Continue with the next item in case of an error

                conn.commit()
                logging.info("Data insertion completed successfully.")

    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")


### DATA CLEANING


def remove_short_titles_and_empty_authors(cursor):
    try:
        # Deleting publications with short titles
        cursor.execute("""
            DELETE FROM publications 
            WHERE char_length(trim(title)) < 2;
        """)

        # Assuming you want to delete publications without associated authors
        cursor.execute("""
            DELETE FROM publications 
            WHERE id NOT IN (SELECT publication_id FROM authorship);
        """)
    except Exception as e:
        logging.error(f"Error in remove_short_titles_and_empty_authors: {e}")

def drop_abstracts(cursor):
    try:
        cursor.execute("""
            ALTER TABLE publications 
            DROP COLUMN abstract;
        """)
    except Exception as e:
        logging.error(f"Error in drop_abstracts: {e}")



def clean_data():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                remove_short_titles_and_empty_authors(cursor)
                drop_abstracts(cursor)
                conn.commit()
                logging.info("Data cleaning completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during data cleaning: {e}")



### DATA ENRICHMENT TASK



# Cache for DOI data
doi_cache = {}

def make_google_scholar_request(params):
    base_url = 'https://serpapi.com/search'
    for attempt in range(5):
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logging.warning("Rate limit hit. Retrying...")
                time.sleep(2 ** attempt)
            else:
                raise e
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            break
    return None



def select_papers_from_categories(cursor, limit_per_category=2):
    cursor.execute("""
        SELECT category_name FROM categories;
    """)
    categories = cursor.fetchall()
    selected_papers = []

    for category in categories:
        cursor.execute("""
            SELECT id, title, categories FROM publications
            WHERE categories LIKE %s
            ORDER BY id
            LIMIT %s;
        """, ('%' + category[0] + '%', limit_per_category))
        papers = cursor.fetchall()
        selected_papers.extend(papers)

    return selected_papers

# Function to query Google Scholar using serpapi
def query_google_scholar(query):
    api_key = "2fbeb8192002fda3306b06b36e3916985438282825bfc1c67862e7f2811bfb6a"  # API key is read from an environment variable
    base_url = 'https://serpapi.com/search'
    params = {
        'engine': 'google_scholar',
        'q': query,
        'start': 0,
        'num': 2,
        'api_key': api_key,
        'hl': 'en'
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request to SerpApi failed: {e}")
        return None
    

def enrich_publications(cycle=2):
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                for cycle_number in range(cycle):
                    logging.info(f"Starting enrichment cycle {cycle_number + 1}")

                    selected_papers = select_papers_from_categories(cursor)
                    for publication_id, title, categories_str in selected_papers:
                        search_results = query_google_scholar(title)
                        if not search_results or 'organic_results' not in search_results:
                            logging.warning(f"No results or invalid response for title '{title}'")
                            continue

                        for article in search_results['organic_results']:
                            new_title = article.get('title', '').strip()
                            new_link = article.get('link', '').strip()
                            new_doi = article.get('result_id', '').strip()  # Changed from 'doi' to 'result_id'
                            current_date = datetime.now().date()
                            first_author = article.get('publication_info', {}).get('authors', [{}])[0].get('name', 'Unknown')
                        
                            # Insert or update publication data
                            cursor.execute("""
                                INSERT INTO publications (submitter, title, journal_ref, doi, update_date)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (title) DO UPDATE
                                SET journal_ref = EXCLUDED.journal_ref,
                                    doi = COALESCE(EXCLUDED.doi, publications.doi),
                                    update_date = EXCLUDED.update_date
                                RETURNING id;
                            """, (first_author, new_title, new_link, new_doi or None, current_date))

                            new_publication_id = cursor.fetchone()[0]

                            # Handle authors
                            author_list = article.get('publication_info', {}).get('authors', [])
                            for author_info in author_list:
                                author_name = author_info.get('name').strip()
                                if author_name:
                                    author_id = get_or_insert_author_id(cursor, author_name)
                                    cursor.execute("""
                                        INSERT INTO authorship (publication_id, author_id)
                                        VALUES (%s, %s)
                                        ON CONFLICT DO NOTHING;
                                    """, (new_publication_id, author_id))
                            
                            # Handle categories
                            categories = categories_str.split()  # Assuming categories are space-separated
                            for category_name in categories:
                                category_id = get_or_insert_category_id(cursor, category_name)
                                cursor.execute("""
                                    INSERT INTO publication_category (publication_id, category_id)
                                    VALUES (%s, %s)
                                    ON CONFLICT DO NOTHING;
                                """, (new_publication_id, category_id))

                    conn.commit()
                    logging.info(f"Enrichment cycle {cycle_number + 1} completed successfully.")

    except psycopg2.Error as e:
        logging.error(f"Database connection error during publication enrichment: {e}")
        raise


# Update your db_params, select_papers_from_categories, and get_or_insert_author_id functions as required

### DATA TRANSFORMATION TASK


def extract_publication_type(search_results):
    for result in search_results.get('results', []):
        bib_entry = result.get('bib_entry', '')
        if '@article' in bib_entry:
            return 'Journal Article'
        elif '@inproceedings' in bib_entry:
            return 'Conference Paper'
        elif '@book' in bib_entry:
            return 'Book'
        elif '@thesis' in bib_entry:
            return 'Thesis'
        elif '@techreport' in bib_entry:
            return 'Technical Report'
        elif '@manual' in bib_entry:
            return 'Manual'
        elif '@proceedings' in bib_entry:
            return 'Conference Proceedings'
        elif '@unpublished' in bib_entry:
            return 'Unpublished Work'
        elif '@misc' in bib_entry:
            return 'Miscellaneous'
        elif '@phdthesis' in bib_entry:
            return 'PhD Thesis'
        elif '@mastersthesis' in bib_entry:
            return 'Masters Thesis'
        elif '@inbook' in bib_entry:
            return 'Book Chapter'
        elif '@incollection' in bib_entry:
            return 'Collection'
        elif '@patent' in bib_entry:
            return 'Patent'
        elif '@online' in bib_entry:
            return 'Online Resource'
    return 'Unknown'


def resolve_publication_types():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, title FROM publications;")
                publications = cursor.fetchall()

                for publication_id, title in publications:
                    try:
                        publication_type = update_publication_type(cursor, publication_id, title)
                        if publication_type == 'Unknown':
                            logging.warning(f"Unknown publication type for title '{title}' (ID: {publication_id})")
                    except Exception as e:
                        logging.error(f"Error in resolve_publication_types for publication ID {publication_id}: {e}")
                        continue  # Skip to the next publication in case of an error

                conn.commit()
                logging.info("Publication types resolved successfully.")

    except psycopg2.Error as e:
        logging.error(f"Database connection error during resolving publication types: {e}")



def extract_resolved_author_name(search_results):
    for result in search_results.get('results', []):
        author_names = result.get('authors', [])
        if author_names:
            return author_names[0]
    return 'Unknown'


def update_author_name(cursor, author_id, name):
    search_results = query_google_scholar('author:' + name)
    resolved_name = extract_resolved_author_name(search_results)

    update_query = """
        UPDATE authors 
        SET name = %s 
        WHERE id = %s;
    """
    cursor.execute(update_query, (resolved_name, author_id))



def update_publication_type(cursor, publication_id, title):
    try:
        search_results = query_google_scholar(title)
        if not search_results or 'results' not in search_results:
            logging.warning(f"No results or invalid response for title '{title}'")
            return 'Unknown'  # Default value in case of no results

        publication_type = extract_publication_type(search_results)

        update_query = """
            UPDATE publications 
            SET publication_type = %s 
            WHERE id = %s;
        """
        cursor.execute(update_query, (publication_type, publication_id))

    except Exception as e:
        logging.error(f"Error updating publication type for publication ID {publication_id}: {e}")
        raise  # Raising exception to stop the script on failure


def resolve_author_names():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, name FROM authors;")
                authors = cursor.fetchall()

                for author_id, name in authors:
                    try:
                        resolved_name = update_author_name(cursor, author_id, name)
                        if resolved_name == 'Unknown':
                            logging.warning(f"Unable to resolve author name for '{name}' (ID: {author_id})")
                    except Exception as e:
                        logging.error(f"Error updating author name for author ID {author_id}: {e}")
                        continue  # Skip to the next author in case of an error

                conn.commit()
                logging.info("Author names resolved successfully.")
    except psycopg2.Error as e:
        logging.error(f"Database connection error during author name resolution: {e}")




def map_to_normalized_category(category_name):
    category_mapping = {
        'hep-ph': 'High Energy Physics - Phenomenology',
        'quant-ph': 'Quantum Physics',
        'cs.AI': 'Computer Science - Artificial Intelligence',
        'cs.DB': 'Computer Science - Databases',
        'math.AP': 'Mathematics - Analysis of PDEs',
        'bio.GN': 'Biology - Genomics',
        'chem.OC': 'Chemistry - Organic Chemistry',
        'econ.TH': 'Economics - Theory',
        'eng.EL': 'Engineering - Electrical',
        'env.SCI': 'Environmental Science',
        'geo.MET': 'Geoscience - Meteorology',
        'hist': 'History',
        'ling': 'Linguistics',
        'med.ON': 'Medicine - Oncology',
        'phil': 'Philosophy',
        'phy.AP': 'Physics - Astrophysics',
        'pol': 'Political Science',
        'psy.CL': 'Psychology - Clinical',
        'soc.SOC': 'Sociology',
        'vet': 'Veterinary Medicine',
        'edu': 'Education',
    }
    return category_mapping.get(category_name, 'Other')



def normalize_fields_of_study():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, category_name FROM categories;")
                for category_id, category_name in cursor.fetchall():
                    normalized_category = map_to_normalized_category(category_name)

                    update_query = """
                        UPDATE categories 
                        SET category_name = %s 
                        WHERE id = %s;
                    """
                    cursor.execute(update_query, (normalized_category, category_id))
                conn.commit()
                logging.info("Fields of study normalized successfully.")
    except Exception as e:
        logging.error(f"An error occurred during field of study normalization: {e}")


def store_citation_data(cursor, publication_id, citations):
    for citation in citations:
        title = citation.get('title', '').strip()
        link = citation.get('link', '').strip()  # Remove this line if 'link' is not in your table
        snippet = citation.get('snippet', '').strip()  # Remove this line if 'snippet' is not in your table

        # Extract the first author from the summary, if available
        summary = citation.get('publication_info', {}).get('summary', '')
        author = summary.split('-')[0].strip() if '-' in summary else 'Unknown'

        # Insert query for the citations table, adjusted to match your schema
        insert_query = """
            INSERT INTO citations (publication_id, title, author, year) 
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (publication_id, title, author, None))  # 'None' for year as it's not extracted

def query_and_store_citations():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, title FROM publications;")
                for publication_id, title in cursor.fetchall():
                    response = query_google_scholar(title)
                    if response and 'organic_results' in response:
                        store_citation_data(cursor, publication_id, response['organic_results'])
                    else:
                        logging.warning(f"No results or invalid response for title '{title}'")
                conn.commit()
                logging.info("Citation data stored successfully.")
    except Exception as e:
        logging.error(f"An error occurred during citation data storage: {e}")






### DATA VALIDATION


def validate_publications(cursor):
    try:
        # Check for duplicate DOIs
        cursor.execute("SELECT COUNT(doi), doi FROM publications GROUP BY doi HAVING COUNT(doi) > 1;")
        duplicates = cursor.fetchall()
        if duplicates:
            logging.warning(f"Duplicate DOIs found: {duplicates}")
        else:
            logging.info("No duplicate DOIs found.")

        # Check for missing DOIs
        cursor.execute("SELECT COUNT(*) FROM publications WHERE doi IS NULL OR trim(doi) = '';")
        missing_count = cursor.fetchone()[0]
        if missing_count > 0:
            logging.warning(f"Publications with missing DOIs: {missing_count}")
        else:
            logging.info("No publications with missing DOIs.")

    except psycopg2.Error as e:
        logging.error(f"Error during publication validation: {e}")
        raise



def validate_authors(cursor):
    try:
        # Check for authors with empty affiliations
        cursor.execute("SELECT COUNT(*) FROM authors WHERE trim(affiliation) = '';")
        empty_affiliation_count = cursor.fetchone()[0]
        if empty_affiliation_count > 0:
            logging.warning(f"Authors with empty affiliations: {empty_affiliation_count}")
        else:
            logging.info("No authors with empty affiliations.")

    except psycopg2.Error as e:
        logging.error(f"Error during author validation: {e}")
        raise


def validate_data():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                validate_publications(cursor)
                validate_authors(cursor)
                logging.info("Data validation completed successfully.")
    except psycopg2.Error as e:
        logging.error(f"Database connection error during data validation: {e}")
    except Exception as e:
        logging.error(f"An error occurred during data validation: {e}")



# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'publication_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False  # Set to False if you don't want historical backfilling
)

# Task Definitions
insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

enrich_publications_task = PythonOperator(
    task_id='enrich_publications',
    python_callable=enrich_publications,
    dag=dag,
)

# resolve_publication_types_task = PythonOperator(
#     task_id='resolve_publication_types',
#     python_callable=resolve_publication_types,
#     dag=dag,
# )

# resolve_author_names_task = PythonOperator(
#     task_id='resolve_author_names',
#     python_callable=resolve_author_names,
#     dag=dag,
# )

# normalize_fields_of_study_task = PythonOperator(
#     task_id='normalize_fields_of_study',
#     python_callable=normalize_fields_of_study,
#     dag=dag,
# )

query_and_store_citations_task = PythonOperator(
    task_id='query_and_store_citations',
    python_callable=query_and_store_citations,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,  # Now calls both validate_publications and validate_authors
    dag=dag,
)



# Task Dependencies
insert_data_task >> clean_data_task
clean_data_task >> enrich_publications_task
# resolve_publication_types_task >> enrich_publications_task
# resolve_author_names_task >> enrich_publications_task
# normalize_fields_of_study_task >> enrich_publications_task
enrich_publications_task >> query_and_store_citations_task
query_and_store_citations_task >> validate_data_task