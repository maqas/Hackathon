from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential
import pandas as pd
from pyspark.sql import SparkSession

# Azure OpenAI configuration
ENDPOINT = "https://mango-bush-0a9e12903.5.azurestaticapps.net/api/v1"
API_KEY = "805387ca-461f-4cb8-af1b-e235da89e5a8"
API_VERSION = "2024-02-01"
MODEL_NAME = "gpt-4o-mini-kenya-hack"

def get_data_from_lakehouse():
    """Retrieve data from Fabric Lakehouse using Delta path"""
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("LakehouseQuery") \
            .getOrCreate()
        
        # Read data from Delta table using the lakehouse path
        #df = spark.read.format("delta").load("/lakehouse/default/tables/SilverTable")
        df = spark.sql("select * from SilverTable where Case_Number not in (select Case_Number from casesummaries)")

        # Convert to pandas DataFrame (if needed)
        pandas_df = df.limit(10).toPandas()  # Get top 10 rows
        
        return pandas_df
    
    except Exception as e:
        print(f"Error connecting to Lakehouse: {str(e)}")
        return None

def generate_summaries(df, text_column):
    """Generate summaries for each row using Azure OpenAI"""
    client = AzureOpenAI(
        azure_endpoint=ENDPOINT,
        api_key=API_KEY,
        api_version=API_VERSION,
    )
    
    summaries = []
    
    for index, row in df.iterrows():
        prompt = f"""
        Analyze the following case data and generate a concise summary:
        
        Case ID: {row['Case_Number']}
        Details: {row[text_column]}
        
        Please provide a 2-3 sentence summary highlighting key points.
        """
        
        try:
            completion = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that summarizes case data."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3  # Lower temperature for more factual responses
            )
            
            summary = completion.choices[0].message.content
            summaries.append(summary)
            
        except Exception as e:
            print(f"Error generating summary for Case ID {row['Case_Number']}: {str(e)}")
            summaries.append("Summary unavailable")
    
    return summaries


def save_to_lakehouse_table(df, table_name="CaseSummaries"):
    """Save DataFrame to a new table in the Lakehouse using Spark SQL"""
    try:
        # Create Spark session if not already exists
        spark = SparkSession.builder.getOrCreate()
        
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        # Save as a new table in the Lakehouse
        spark_df.write.mode("append").format("delta").saveAsTable(table_name)
        
        print(f"Successfully saved data to table: {table_name}")
        
    except Exception as e:
        print(f"Error saving to Lakehouse table: {str(e)}")


def main():
    # Step 1: Get data from Lakehouse
    df = get_data_from_lakehouse()
    
    if df is None or df.empty:
        print("No data retrieved from Lakehouse.")
        return
    
    # Step 2: Specify which column contains the text to summarize
    text_column = "Ruling"  # Replace with your actual column name
    
    # Step 3: Generate summaries
    print("Generating summaries...")
    df['Summary'] = generate_summaries(df, text_column)
    
    # Step 4: Output or save results
    print("\nGenerated Summaries:")
    for _, row in df.iterrows():
        print(f"\nCase ID: {row['Case_Number']}")
        print(f"Summary: {row['Summary']}")
        print("-" * 50)
    
    # Optionally save to CSV
    # df.to_csv('case_summaries.csv', index=False)

    save_to_lakehouse_table(df)

if __name__ == "__main__":
    main()