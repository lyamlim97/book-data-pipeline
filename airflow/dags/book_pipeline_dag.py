import os
import datetime as dt
import logging
import pandas as pd
import pyarrow.fs as pafs
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BOOK_BUCKET = os.environ.get('GCP_BOOK_BUCKET')
BOOK_WH_EXT = os.environ.get('GCP_BOOK_WH_EXT_DATASET')
BOOK_WH = os.environ.get('GCP_BOOK_WH_DATASET')
GCP_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')


# Airflow directory to store parquet files
dataset_download_path = f'{AIRFLOW_HOME}/book-dataset/'
parquet_store_path = f'{dataset_download_path}pq/'

# GCS directory to store parquet files
gcs_store_dir = '/book-pq'
gcs_pq_store_path = f'{BOOK_BUCKET}{gcs_store_dir}'

book_datasets = ['books', 'ratings', 'users']

book_dtype = {
    'isbn': pd.StringDtype(),
    'book_title': pd.StringDtype(),
    'book_author': pd.StringDtype(),
    'year_of_publication': pd.Int64Dtype(),
    'publisher': pd.StringDtype(),
}

user_dtype = {
    'user_id': pd.Int64Dtype(),
    'location': pd.StringDtype(),
    'age': pd.Int64Dtype(),
}

rating_dtype = {
    'user_id': pd.Int64Dtype(),
    'isbn': pd.StringDtype(),
    'rating': pd.Int64Dtype(),
}

def _clean_to_parquet():

    # Create Airflow directory to store parquet files
    if not os.path.exists(parquet_store_path):
        os.makedirs(parquet_store_path)

    # Read and convert files to parquet
    for filename in os.listdir(dataset_download_path):
        if filename.endswith('.csv'):
            dataset_df = pd.read_csv(f'{dataset_download_path}{filename}')

            # Drop image URLs for books
            if filename.startswith('Books'):
                dataset_df = dataset_df.drop(columns=[
                    'Image-URL-S',
                    'Image-URL-M',
                    'Image-URL-L'
                ], axis='columns')

                dataset_df = dataset_df.rename(mapper={
                    'Book-Title': 'book_title',
                    'Book-Author': 'book_author',
                    'Year-Of-Publication': 'year_of_publication',
                    'Publisher': 'publisher',
                    'ISBN': 'isbn'
                }, axis='columns')

                dataset_df['year_of_publication'] = pd.to_numeric(dataset_df['year_of_publication'], errors='coerce')
                dataset_df = dataset_df.dropna(subset=['year_of_publication'])
                dataset_df = dataset_df.astype(book_dtype)
            elif filename.startswith('Users'):
                dataset_df = dataset_df.rename(mapper={
                    'User-ID': 'user_id',
                    'Age': 'age',
                    'Location': 'location'
                }, axis='columns')

                dataset_df = dataset_df.astype(user_dtype)

                # Split location data into city, state and country
                dataset_df['location_data'] = dataset_df['location'].apply(lambda x: [x.strip() for x in x.split(',')]) # Split by a comma and trim
                dataset_df['location_data'] = dataset_df['location_data'].apply(lambda values: [val for val in reversed(values) if val is not None][:3][::-1])

                dataset_df[['city', 'state', 'country']] = pd.DataFrame(dataset_df['location_data'].tolist())
                dataset_df.drop(columns=['location', 'location_data'], inplace=True)

                # Fill missing age data with -1
                dataset_df['age'].fillna(-1, inplace=True)
            elif filename.startswith('Ratings'):
                dataset_df = dataset_df.rename(mapper={
                    'User-ID': 'user_id',
                    'ISBN': 'isbn',
                    'Book-Rating': 'rating'
                }, axis='columns')

                dataset_df.astype(rating_dtype)

                dataset_df['rating'].fillna(0, inplace=True)
            else:
                continue


            print('dataset_df.columns', dataset_df.columns)
            parquet_filename = filename.lower().replace('.csv', '.parquet')
            parquet_loc = f'{parquet_store_path}{parquet_filename}'

            dataset_df.reset_index(drop=True, inplace=True)
            dataset_df.to_parquet(parquet_loc)

            logging.info('Done creating parquet files.')


def _upload_to_gcs():
    gcs = pafs.GcsFileSystem()
    dir_info = gcs.get_file_info(gcs_pq_store_path)

    # Delete directory if exist
    if dir_info.type != pafs.FileType.NotFound:
        gcs.delete_dir(gcs_pq_store_path)

    gcs.create_dir(gcs_pq_store_path)
    pafs.copy_files(
        source=parquet_store_path,
        destination=gcs_pq_store_path,
        destination_filesystem=gcs
    )

    logging.info('Uploaded parquet files to GCS.')


default_args = {
    'owner': 'lyamlim',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
    dag_id ='book_data_dag',
    default_args=default_args,
    description='DAG for book dataset',
    user_defined_macros={
        'BOOK_WH_EXT': BOOK_WH_EXT,
        'BOOK_WH': BOOK_WH
    }
) as dag:
    install_pip_packages = BashOperator(
        task_id='install_pip_packages',
        bash_command='pip install kaggle'
    )

    download_dataset = BashOperator(
        task_id='download_dataset',
        bash_command=f'kaggle datasets download arashnic/book-recommendation-dataset --path {dataset_download_path} --unzip'
    )

    clean_to_parquet = PythonOperator(
        task_id='clean_to_parquet',
        python_callable=_clean_to_parquet
    )

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=_upload_to_gcs
    )

    with TaskGroup('create_external_tables_group') as create_external_tables_group:
        for dataset in book_datasets:
            BigQueryCreateExternalTableOperator(
                 task_id=f'bq_external_{dataset}_table',
                 table_resource={
                     'tableReference': {
                         'projectId': GCP_PROJECT_ID,
                         'datasetId': BOOK_WH_EXT,
                         'tableId': dataset,
                     },
                     'externalDataConfiguration': {
                         'autodetect': True,
                         'sourceFormat': 'PARQUET',
                         'sourceUris': [f'gs://{gcs_pq_store_path}/{dataset}.parquet'],
                     },
                 },
             )

    delete_dataset_store = BashOperator(
        task_id='delete_dataset_store',
        bash_command=f"rm -rf {dataset_download_path}"
    )

    uninstall_pip_packages = BashOperator(
        task_id='uninstall_pip_packages',
        bash_command=f"pip uninstall --yes kaggle"
    )

install_pip_packages >> download_dataset >> clean_to_parquet >> upload_to_gcs >> create_external_tables_group >> delete_dataset_store >> uninstall_pip_packages