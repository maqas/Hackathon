import requests
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
import re

# PostgreSQL connection parameters
DB_HOST = "hachathonpostgres2025.postgres.database.azure.com"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "hackathon@2025"

def get_case_links():
    base_urls = [
    
        
      
         "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-devolution-law"
 
    ]
    #     "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-commercial-law",
    #   "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-contract-law",
    #   "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-corporate-law",
    #  "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-criminal-law",
    #    "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-land-law",
    #    "https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-anti-corruption-law"
     #"https://new.kenyalaw.org/taxonomy/case-indexes/case-indexes-common-law"

     
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    all_case_links = []
    
    for base_url in base_urls:
        response = requests.get(base_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve page: {base_url}. Status code: {response.status_code}")
            continue  # Skip to the next URL if there's an error
        
        soup = BeautifulSoup(response.text, "html.parser")
        case_links = ["https://new.kenyalaw.org" + link["href"] for link in soup.find_all("a", href=True) if "/akn/ke/judgment/" in link["href"]]
        
        all_case_links.extend(case_links)
    
    return all_case_links

def scrape_case_data(case_links):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    results = []
    
    for url in case_links:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve the case page: {url}. Status code: {response.status_code}")
            continue
        
        soup = BeautifulSoup(response.text, "html.parser")

        metadata = {}
    
        for dt in soup.find_all("dt"):
            label = dt.text.strip().lower().replace(":", "")  # Normalize label (lowercase, remove colons)
            dd = dt.find_next_sibling("dd")  # Get corresponding <dd> tag
            if dd:
                metadata[label] = dd.text.strip()

        metadata2 = soup.find("dl", class_="document-metadata-list d-md-flex")
        dd_values = [dd.get_text(strip=True) for dd in metadata2.find_all('dd')] if metadata2 else []
        date_published, case_action, court_division, filing_county = (dd_values + [None] * 4)[:4]

        data = {}
        headings = soup.find_all(class_='akn-heading')
        for heading in headings:
            header_text = heading.get_text(strip=True)
            content = []
            next_element = heading.find_next_sibling()
            while next_element and next_element.name not in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                content.append(next_element.get_text(strip=True))
                next_element = next_element.find_next_sibling()
            data[header_text] = ' '.join(content) if content else None

        #ruling_paragraphs = [soup.find(class_='akn-paragraph', id=f'arguments__para_{i}') for i in range(100)]

        ruling_paragraphs = []
        for i in range(50):  # Starting from 1 is more common for paragraph numbering
            # Check for first pattern
            para1 = soup.find(class_='akn-paragraph', id=f'arguments__para_{i}')
            if para1:
                ruling_paragraphs.append(para1)
            
            # Check for second pattern
            para2 = soup.find(class_='akn-paragraph', id=f'arguments__dvs_nn_1__para_{i}')
            if para2:
                ruling_paragraphs.append(para2)

        # Remove any None values that might have been added
        ruling_paragraphs = [p for p in ruling_paragraphs if p is not None]

        #class="akn-div parties-listing"
        #class="akn-div party-name"
        # parties_div = [soup.find('div', class_='akn-div party-name')]
        # parties_title_div = [soup.find_all('div', class_='akn-div')]

        # parties_text = '\n'.join(
        # div.get_text(strip=True, separator='\n')
        # for div in soup.find_all('div', class_='akn-div')
        # if 'parties-listing' in div.get('class', [])
        # ) or ""

        # Combine all content
        # all_content = []
        # all_content = parties_div + parties_title_div + ruling_paragraphs
        # if parties_div:
        #     all_content.append(parties_div.get_text(strip=True, separator='\n'))
    
        # all_content.extend([p.get_text(strip=True, separator='\n') for p in ruling_paragraphs if p]) or None

        # Join all content with double newlines for separation
        #combined_text = '\n\n'.join(all_content)

        ruling_content = " ".join([para.get_text(strip=True) for para in ruling_paragraphs if para]) or None
        
        

        data["Ruling"] = ruling_content

        # print(metadata)

        scraped_data = {
                "Citation": metadata.get("citation"),
                "Court": metadata.get("court"),
                "Court_Station": metadata.get("court station"),
                "Outcome": metadata.get("outcome"),
                "Case_Number": metadata.get("case number"),
                "Judges": metadata.get("judges"),
                "Judgement_Date": metadata.get("judgment date"),
                "Language": metadata.get("language"),
                "Type": metadata.get("type"),
                "Date_Published": date_published,
                "Case_Action": case_action,
                "Court_Division": court_division,
                "Filing_County": filing_county,
                "URL": url
            }
        
        scraped_data.update(data)
        results.append(scraped_data)
    
    return pd.DataFrame(results)

def connect_db():
    """Connects to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None
    
def create_table(sample_data):
    """Creates the kenya_judgments table dynamically based on the columns in sample_data."""
    conn = connect_db()
    if conn is None:
        return
    
    # Extract column names dynamically from sample_data
    column_names = sample_data.columns.tolist()  

    # Define column structure with an "id" as SERIAL PRIMARY KEY and ensure safe column naming
    column_definitions = ["id SERIAL PRIMARY KEY"] + [f'"{col}" TEXT' for col in column_names]

    # SQL queries to drop and create the table
    drop_query = "DROP TABLE IF EXISTS kenya_judgments CASCADE;"
    create_query = f"CREATE TABLE kenya_judgments ({', '.join(column_definitions)});"

    try:
        with conn.cursor() as cur:
            cur.execute(drop_query)  # Drop table if it exists
            cur.execute(create_query)  # Create new table
            conn.commit()
        print("Table dropped (if existed) and created successfully.")
    except Exception as e:
        print(f"Failed to create table: {e}")
    finally:
        conn.close()

def insert_data(dataframe):
    """Inserts data into the kenya_judgments table."""
    conn = connect_db()
    if conn is None:
        return
    
    try:
        with conn.cursor() as cur:
            for _, row in dataframe.iterrows():
                # 🔹 Extract non-empty columns dynamically
                columns = [col for col in dataframe.columns if pd.notna(row[col])]
                values = [row[col] for col in columns]
                
                if not columns:
                    print("Skipping empty row.")
                    continue  # Avoid inserting empty rows

                # 🔹 Safely format column names and placeholders
                columns_str = ', '.join(f'"{col}"' for col in columns)  # Ensure proper SQL syntax
                placeholders = ', '.join(['%s'] * len(values))  # Create (%s, %s, ...) based on column count
                
                query = f'INSERT INTO kenya_judgments ({columns_str}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING;'
                
                cur.execute(query, values)
            
            conn.commit()
            print("Data inserted successfully.")
    
    except Exception as e:
        print(f"Failed to insert data: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    case_links = get_case_links()
    if case_links:
        df = scrape_case_data(case_links)
        create_table(df)
        print("Before cleaning:", df.head())  # Debugging step

        # Clean NaN values and standardize column names
        df = df.where(pd.notnull(df), None) 
        # df.rename(columns=COLUMN_MAPPING, inplace=True)
        # df = df[list(COLUMN_MAPPING.values())]  # Keep only mapped columns
        df.to_csv("kenya_law_cases.csv", index=False) 

        print("After cleaning:", df.head())  # Debugging step
        insert_data(df)
        print("Data saved and inserted successfully.")