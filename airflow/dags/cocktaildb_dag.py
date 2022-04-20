import shutil
import time
from datetime import timedelta, datetime

import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import os
import json

from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Blake Ordway',
    'depends_on_past': False,
    'email': ['blakeordway2@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def request_url(method, url, headers=None, data=None):
    tries = 1
    while tries < 5:
        try:
            res = requests.request(method, url, headers=headers, data=data)
            return res
        except Exception as e:
            print(e.__str__())
            time.sleep(5)
            tries += 1

    return None


def get_drinks_by_first_letter():
    drinks = []
    # Get a subset of possible drinks (could search all alphabet and numerics)
    for char in "alnr":
        res = request_url('GET', f"https://thecocktaildb.com/api/json/v1/1/search.php?f={char}")
        if res is None:
            raise AirflowFailException('Failed to get drinks by first letter. Try again later')
        drinks.extend(res.json()['drinks'])

    return drinks


def get_drink_alterations(ti):
    alterations = []
    drinks = ti.xcom_pull(key='return_value', task_ids=['get_all_drinks'])[0]

    for drink in drinks:
        name = drink['strDrink']
        res = request_url('GET', f'https://thecocktaildb.com/api/json/v1/1/search.php?s={name}')
        if res is None:
            raise AirflowFailException('Failed to search for drink. Try again later')
        json_result = res.json()
        print(json_result)
        alterations.extend(json_result['drinks'])

    file_loc = '/tmp/data/all_drinks.json'
    with open(file_loc, 'w') as outfile:
        json.dump(alterations, outfile)

    print(f'Wrote {len(drinks)} drinks to {file_loc}')
    return file_loc


def validate_drinks(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['get_drink_alterations'])[0]
    print(file_loc)

    res = requests.post("http://cocktaildb_api:5000/drinks/validate", json={"drink_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    print('Successfully validated drinks')


def filter_new_drinks(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['get_drink_alterations'])[0]
    print(file_loc)

    res = requests.post("http://cocktaildb_api:5000/drinks/filter", json={"drink_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    if 'file_location' not in res_json:
        raise AirflowFailException("No filtered file location returned from API. Check logs and API return value")

    filtered_file_loc = res_json['file_location']
    print('Finished filtering drinks')
    ti.xcom_push(key='file_location', value=filtered_file_loc)
    return filtered_file_loc


def transform_drinks(ti):
    file_loc = ti.xcom_pull(key='file_location', task_ids=['filter_drinks'])[0]
    print(file_loc)

    res = requests.post("http://cocktaildb_api:5000/drinks/transform", json={"drink_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    if 'file_location' not in res_json:
        raise AirflowFailException("No filtered file location returned from API. Check logs and API return value")

    filtered_file_loc = res_json['file_location']
    print('Successfully transformed drinks')
    return filtered_file_loc


def store_drinks(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['drink_tl.transform_drinks'])[0]
    print(file_loc)

    res = requests.post("http://cocktaildb_api:5000/drinks/store", json={"drink_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])


def get_unique_ingredients(ti):
    file_loc = ti.xcom_pull(key='file_location', task_ids=['filter_drinks'])[0]
    print(file_loc)

    res = requests.post("http://cocktaildb_api:5000/ingredients/unique", json={"drink_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    if 'ingredients' not in res_json:
        raise AirflowFailException("No ingredients returned from API. Check logs and API return value")

    ingredients = res_json['ingredients']
    debug_str = ','.join(ingredients)
    print(f'Got ingredients to search: {debug_str}')
    return ingredients


def search_ingredients(ti):
    ingredients_strs = ti.xcom_pull(key='return_value', task_ids=['ingredients_tl.get_unique_ingredients'])[0]
    print(ingredients_strs)

    all_ingredients = []
    for ingredient_str in ingredients_strs:
        res = request_url('GET', f"https://thecocktaildb.com/api/json/v1/1/search.php?i={ingredient_str}")
        if res is None:
            raise AirflowException('Failed to get ingredients. Try again later')
        data_json = res.json()
        if data_json is None or 'ingredients' not in data_json:
            raise AirflowException(f'Error finding ingredient: {ingredient_str}')
        print(data_json)
        if data_json['ingredients'] is None:
            print(f"No entry for {ingredient_str} exists in cocktaildb")
            continue
        all_ingredients.extend(data_json['ingredients'])

    file_loc = '/tmp/data/all_ingredients.json'
    with open(file_loc, 'w') as outfile:
        json.dump(all_ingredients, outfile)

    print(f'Wrote {len(all_ingredients)} ingredients to {file_loc}')
    return file_loc


def filter_ingredients(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['ingredients_tl.search_ingredients'])[0]
    print(file_loc)
    res = requests.post("http://cocktaildb_api:5000/ingredients/filter", json={"ingredients_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    if 'file_location' not in res_json:
        raise AirflowFailException("No filtered file location returned from API. Check logs and API return value")

    filtered_file_loc = res_json['file_location']
    print('Finished filtering ingredients')
    return filtered_file_loc


def transform_ingredients(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['ingredients_tl.filter_ingredients'])[0]
    print(file_loc)
    res = requests.post("http://cocktaildb_api:5000/ingredients/transform", json={"ingredients_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    if 'file_location' not in res_json:
        raise AirflowFailException("No filtered file location returned from API. Check logs and API return value")

    filtered_file_loc = res_json['file_location']
    print('Successfully transformed ingredients')
    return filtered_file_loc


def store_ingredients(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['ingredients_tl.transform_ingredients'])[0]
    print(file_loc)
    res = requests.post("http://cocktaildb_api:5000/ingredients/store", json={"ingredients_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])


def link_drinks_to_ingredients(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['filter_drinks'])[0]
    print(file_loc)
    res = requests.post("http://cocktaildb_api:5000/drink/link/ingredients/transform", json={"drink_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])

    if 'file_location' not in res_json:
        raise AirflowFailException("No filtered file location returned from API. Check logs and API return value")

    filtered_file_loc = res_json['file_location']
    print('Successfully linked drinks and ingredients')
    return filtered_file_loc


def store_links(ti):
    file_loc = ti.xcom_pull(key='return_value', task_ids=['create_links'])[0]
    print(file_loc)
    res = requests.post("http://cocktaildb_api:5000/drink/link/ingredients/store", json={"link_file": file_loc})
    res_json = res.json()
    print(res_json)
    if res_json['status'] != 'success':
        raise AirflowFailException(res_json['msg'])


def backup_files(ti):
    xcom = ti.xcom_pull(key='return_value',
                        task_ids=['ingredients_tl.transform_ingredients', 'drink_tl.transform_drinks'])
    ingredients_file = xcom[0]
    drinks_file = xcom[1]
    print(ingredients_file)
    print(drinks_file)

    folder = time.time()
    new_drink_file = f'/backup/data/{folder}/drinks.json'
    new_ingredient_file = f'/backup/data/{folder}/ingredients.json'
    os.makedirs(os.path.dirname(new_drink_file), exist_ok=True)

    shutil.copyfile(drinks_file, new_drink_file)
    shutil.copyfile(ingredients_file, new_ingredient_file)

    os.remove(ingredients_file)
    os.remove(drinks_file)


def delete_tmp_files():
    # If this fails, it will raise an error and the task will fail so we'll know about it
    shutil.rmtree('/tmp/data/*', ignore_errors=False)


with DAG('cocktaildb', default_args=default_args,
         description="Automate pulls from cocktailDB",
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 4, 12),
         catchup=False,
         concurrency=2,
         tags=['example', 'cocktail']) as dag:
    dag.doc_md = """
    # Cocktail DB scraper
    Example airflow dag for pulling daily data from "TheCocktailDB".
    ## Notes
    1. Set up to run daily
    2. Requires mysql docker container
    3. Requires cocktaildb_api container
    """

    t1 = PythonOperator(
        task_id='get_all_drinks',
        python_callable=get_drinks_by_first_letter
    )

    t1.doc_md = """
    Call TheCocktailDB API to get drinks that start with a specific letter. Letters are hardcoded for now.
    
    *Xcom*:
    
    - key: 'return_value'
    - value: list
        - Returns a list of json objects (dict)  
    """

    t2 = PythonOperator(
        task_id='get_drink_alterations',
        python_callable=get_drink_alterations
    )

    t2.doc_md = """
    Call TheCocktailDB API to get alterations of drinks. Pulls 'return_value' from the task: `get_all_drinks`.
    """

    t3 = PythonOperator(
        task_id='validate_drinks',
        python_callable=validate_drinks
    )

    t3.doc_md = """
    Call our API to validate the schema of the drinks. Pulls 'return_value' from the task: `get_drink_alterations`.
    Fails the DAG if this task fails.
    """

    t4 = PythonOperator(
        task_id='filter_drinks',
        python_callable=filter_new_drinks
    )

    t4.doc_md = """
    Call the API to find only new drinks that aren't currently in the database. Uses the return value from the 
    `get_drink_alterations` task for the file location.
    """

    # Task group for working with drinks
    with TaskGroup(group_id='drink_tl') as tg1:
        t5 = PythonOperator(
            task_id='transform_drinks',
            python_callable=transform_drinks
        )

        t5.doc_md = """
        Call the API to read the JSON data of new drinks and turn it into records in the format for the MySQL database.
        """

        t6 = PythonOperator(
            task_id='store_drinks',
            python_callable=store_drinks
        )

        t6.doc_md = """
        Call the API to read a CSV file that has drink records.
        """

        t5 >> t6

    # Task group for working with ingredients
    with TaskGroup(group_id='ingredients_tl') as tg2:
        t7 = PythonOperator(
            task_id='get_unique_ingredients',
            python_callable=get_unique_ingredients
        )

        t7.doc_md = """
        Call the API to read the ingredients from the `filter_drinks` return value file and get only unique (no need to
        search for ingredients twice)
        """
        t8 = PythonOperator(
            task_id='search_ingredients',
            python_callable=search_ingredients
        )

        t8.doc_md = """
        Search TheCocktailDB for the ingredients returned by the API
        """
        t9 = PythonOperator(
            task_id='filter_ingredients',
            python_callable=filter_ingredients
        )

        t9.doc_md = """
        Call the API to filter returned ingredients to remove duplicates by ID that are already stored in the database
        """

        t10 = PythonOperator(
            task_id='transform_ingredients',
            python_callable=transform_ingredients
        )

        t10.doc_md = """
        Call the API to read the JSON data of new ingredients and turn it into CSV records for the database.
        """
        t11 = PythonOperator(
            task_id='store_ingredients',
            python_callable=store_ingredients
        )

        t11.doc_md = """
        Call the API to read the CSV data and store ingredients into the database
        """

        t7 >> t8 >> t9 >> t10 >> t11

    t12 = PythonOperator(
        task_id='create_links',
        python_callable=link_drinks_to_ingredients
    )

    t12.doc_md = """
    Call the API to create drink to ingredient mapping. Stored as a CSV file
    """

    t13 = PythonOperator(
        task_id='store_links',
        python_callable=store_links
    )

    t13.doc_md = """
    Call the API to store the CSV linking data in the database.
    """

    t14 = PythonOperator(
        task_id='backup_staged_files',
        python_callable=backup_files
    )

    t14.doc_md = """Store the filtered ingredients and drinks files in the database"""

    t15 = PythonOperator(
        task_id='delete_files',
        python_callable=delete_tmp_files
    )

    t15.doc_md = """Delete the files in the tmp directory"""

    t1 >> t2 >> t3 >> t4 >> [tg1, tg2] >> t12 >> t13 >> t14 >> t15
