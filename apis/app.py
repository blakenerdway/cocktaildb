import csv
import json
import re
import MySQLdb

from flask import Flask, request
import os
import uuid
import logging

app = Flask(__name__)

required_fields = {"idDrink", "strDrink", "strTags", "strCategory", "strGlass", "strInstructions", "strDrinkThumb"}


def remove_accents(old):
    new = old.lower()
    new = re.sub(r'[àáâãäå]', 'a', new)
    new = re.sub(r'[èéêë]', 'e', new)
    new = re.sub(r'[ìíîï]', 'i', new)
    new = re.sub(r'[òóôõö]', 'o', new)
    new = re.sub(r'[ùúûü]', 'u', new)
    return new


def read_file(file_loc: str) -> dict:
    logging.info(f'Attempting to read file: {file_loc}...')
    if not os.path.exists(file_loc):
        raise FileNotFoundError
    with open(file_loc, encoding='utf-8') as fh:
        all_drinks = json.load(fh)

    logging.info(all_drinks)
    return all_drinks


def write_csv(orig_file_loc: str, type_prefix: str, data_dict: list, keys: list) -> str:
    write_file = os.path.join(os.path.dirname(orig_file_loc), f"{type_prefix}_load_{uuid.uuid4()}.csv")

    with open(write_file, 'w', encoding='utf-8', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(data_dict)

    return write_file


def store_csv_to_db(file_loc: str, db_table: str) -> str:
    query = f"LOAD DATA INFILE '{file_loc}' IGNORE INTO TABLE {db_table} FIELDS TERMINATED BY ',' " \
            f"OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;"
    logging.info(query)
    conn = MySQLdb.connect(host="mysql",
                           user="root",
                           passwd="password!",
                           db="cocktail_db",
                           auth_plugin='mysql_native_password')
    cursor = conn.cursor()
    cursor.execute(query)
    result = conn.info()
    conn.commit()
    conn.close()

    return result


@app.route("/drinks/validate", methods=['POST'])
def validate_drinks():
    logging.info(f'Validating drinks')
    request_json = request.json
    if request_json is None or 'drink_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"drink_file\" parameter to specify what file to read"}, 400

    file_loc = request_json['drink_file']
    try:
        all_drinks = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Drink file does not exist"}, 400

    invalid_drinks = []
    for drink in all_drinks:
        missing_fields = [x for x in required_fields if x not in drink.keys()]
        if len(missing_fields) > 0:
            invalid_drinks.append({'id': drink.get('drinkId', 'UNKNOWN'), 'missing_fields': missing_fields})

    if len(invalid_drinks) > 0:
        return {"status": "fail",
                "msg": "Validation check on data failed. Check that the expected fields are available still",
                "invalid_drinks": invalid_drinks}, 200
    else:
        return {"status": "success", "msg": "Validation check on data passed"}, 200


@app.route("/drinks/filter", methods=['POST'])
def find_new_drinks():
    logging.info(f'Filtering for only new drinks')
    request_json = request.json
    if request_json is None or 'drink_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"drink_file\" parameter to specify what file to read"}, 400

    file_loc = request_json['drink_file']
    logging.info(request_json)
    try:
        all_drinks = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Drink file does not exist"}, 400

    dupes_removed = {x['idDrink']: x for x in all_drinks}
    all_drinks = list(dupes_removed.values())

    logging.info(f'Loaded drink file')
    logging.info('Fetching cached drinks from database')

    db = MySQLdb.connect(host="mysql",
                         user="root",
                         passwd="password!",
                         db="cocktail_db",
                         auth_plugin='mysql_native_password')
    cursor = db.cursor()

    cursor.execute("SELECT id FROM drinks;")
    columns = [col[0] for col in cursor.description]
    stored_drinks = [dict(zip(columns, row)) for row in cursor.fetchall()]
    db.close()

    cached_drinks = [x['id'] for x in stored_drinks]
    new_drinks = [x for x in all_drinks if int(x['idDrink']) not in cached_drinks]

    filtered_drinks_loc = os.path.join(os.path.dirname(file_loc), f"filtered_drinks_{uuid.uuid4()}.json")
    with open(filtered_drinks_loc, 'w') as outfile:
        json.dump(new_drinks, outfile)

    return {"status": "success", "msg": "Filtered out old drinks",
            "results": {"new_drinks": len(new_drinks)}, "file_location": filtered_drinks_loc}, 200


@app.route("/drinks/transform", methods=['POST'])
def transform_drinks():
    logging.info(f'Transforming drinks')
    request_json = request.json
    if request_json is None or 'drink_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"drink_file\" parameter to specify what file to read"}, 400

    logging.info(request_json)
    file_loc = request_json['drink_file']
    try:
        filtered_drinks = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Drink file does not exist"}, 400

    logging.info(f'Loaded drink file')

    transformed_drinks = []
    for drink in filtered_drinks:
        try:
            transformed_drink = {'id': int(drink['idDrink']),
                                 'name': drink['strDrink'],
                                 'tags': drink['strTags'],
                                 'type': drink['strCategory'],
                                 'glassType': drink['strGlass'],
                                 'instructions': drink['strInstructions'],
                                 'imageUrl': drink['strDrinkThumb']}

            # Remove any white space at the front or end of the strings and any new lines
            for key in transformed_drink.keys():
                if isinstance(transformed_drink[key], str):
                    transformed_drink[key] = transformed_drink[key].strip().replace('\n', '').replace('\r', '')

        except KeyError as e:
            return {"status": "fail", "msg": e.__str__(), "drink": drink.get('idDrink', 'MISSING_ID')}

        transformed_drinks.append(transformed_drink)

    write_file = write_csv(file_loc, "drinks", transformed_drinks, ['id', 'name', 'tags', 'type', 'glassType',
                                                                    'instructions', 'imageUrl'])

    return {"status": "success", "msg": "Drinks ready to be loaded to database", "file_location": write_file}, 200


@app.route("/drinks/store", methods=['POST'])
def store_new_drinks():
    logging.info(f'Storing drinks into database')
    request_json = request.json
    if request_json is None or 'drink_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"drink_file\" parameter to specify what file to load"}, 400

    logging.info(request_json)
    file_loc = request_json['drink_file']
    result = store_csv_to_db(file_loc, "drinks")
    return {"status": "success", "msg": result}, 200


@app.route("/ingredients/unique", methods=['POST'])
def find_unique_ingredients():
    logging.info(f'Returning all unique ingredients')
    request_json = request.json
    if request_json is None or 'drink_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"drink_file\" parameter to specify what file to read"}, 400

    logging.info(request_json)
    file_loc = request_json['drink_file']
    try:
        all_drinks = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Drink file does not exist"}, 400

    logging.info('Finding unique ingredients')
    all_ingredients = set()
    for drink in all_drinks:
        for i in range(1, 16):
            ingredient = drink[f'strIngredient{i}']
            if ingredient is not None and ingredient != '':
                # Had to encode then decode so that certain characters would be rendered properly
                ingredient = ingredient.encode("iso-8859-1").decode('utf-8')
                ingredient = remove_accents(ingredient)
                all_ingredients.add(ingredient)

    return {"status": "success", "msg": "Found unique ingredients in current drinks", "ingredients": list(all_ingredients)}, \
           200


@app.route("/ingredients/filter", methods=['POST'])
def filter_new_ingredients():
    logging.info(f'Returning all unique ingredients')
    request_json = request.json
    if request_json is None or 'ingredients_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"ingredients_file\" parameter to specify what file to read"}, 400

    logging.info(request_json)
    file_loc = request_json['ingredients_file']
    try:
        all_ingredients = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Ingredients file does not exist"}, 400

    logging.info('Filtering ingredients')

    db = MySQLdb.connect(host="mysql",
                         user="root",
                         passwd="password!",
                         db="cocktail_db",
                         auth_plugin='mysql_native_password')
    cursor = db.cursor()

    cursor.execute("SELECT id FROM ingredients;")
    columns = [col[0] for col in cursor.description]
    # Get the results with as a dictionary rather than a tuple
    stored_ingredients = [dict(zip(columns, row)) for row in cursor.fetchall()]
    db.close()

    cached_ingredients = [x['id'] for x in stored_ingredients]
    new_ingredients = [x for x in all_ingredients if int(x['idIngredient']) not in cached_ingredients]

    filtered_drinks_loc = os.path.join(os.path.dirname(file_loc), f"filtered_ingredients_{uuid.uuid4()}.json")
    with open(filtered_drinks_loc, 'w') as outfile:
        json.dump(new_ingredients, outfile)

    return {"status": "success", "msg": "Filtered out old ingredients",
            "results": {"new_ingredients": len(new_ingredients)}, "file_location": filtered_drinks_loc}, 200


@app.route("/ingredients/transform", methods=['POST'])
def transform_ingredients():
    logging.info(f'Transforming ingredients')
    request_json = request.json
    if request_json is None or  'ingredients_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"ingredients_file\" parameter to specify what file to read"}, 400

    logging.info(request_json)
    file_loc = request_json['ingredients_file']
    try:
        all_ingredients = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Drink file does not exist"}, 400

    logging.info(f'Loaded drink file')

    transformed_ingredients = []
    for ingredient in all_ingredients:
        try:
            transformed_ingredient = {'id': int(ingredient['idIngredient']),
                                      'name': ingredient['strIngredient'],
                                      'description': ingredient['strDescription']}

            # Remove any white space at the front or end of the strings
            for key in transformed_ingredient.keys():
                if isinstance(transformed_ingredient[key], str):
                    transformed_ingredient[key] = transformed_ingredient[key].strip().replace('\n', '').replace('\r', '')

        except KeyError as e:
            return {"status": "fail", "msg": e.__str__(), "drink": ingredient.get('idDrink', 'MISSING_ID')}

        transformed_ingredients.append(transformed_ingredient)

    write_file = write_csv(file_loc, "ingredients", transformed_ingredients, ['id', 'name', 'description'])

    return {"status": "success", "msg": "Ingredients ready to be loaded to database", "file_location": write_file}, 200


@app.route("/ingredients/store", methods=['POST'])
def store_ingredients():
    logging.info(f'Storing ingredients into database')
    request_json = request.json
    if request_json is None or 'ingredients_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"ingredients_file\" parameter to specify what file to load"}, 400

    logging.info(request_json)
    file_loc = request_json['ingredients_file']
    result = store_csv_to_db(file_loc, "ingredients")
    return {"status": "success", "msg": result}, 200


@app.route("/drink/link/ingredients/transform", methods=['POST'])
def link_drinks_to_ingredients():
    logging.info(f'Linking ingredients')
    request_json = request.json
    if request_json is None or 'drink_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"drink_file\" parameter to specify what file to read"}, 400
    logging.info(request_json)

    file_loc = request_json['drink_file']
    try:
        all_drinks = read_file(file_loc)
    except FileNotFoundError as e:
        return {"status": "fail", "msg": "Drink file does not exist"}, 400

    db = MySQLdb.connect(host="mysql",
                         user="root",
                         passwd="password!",
                         db="cocktail_db",
                         auth_plugin='mysql_native_password')
    cursor = db.cursor()

    cursor.execute(f"SELECT id, name FROM ingredients;")
    name_to_id_ingredients = {row[1]: row[0] for row in cursor.fetchall()}
    case_insensitive_set = {k.lower(): k for k in name_to_id_ingredients.keys()}
    db.close()

    drink_ingredients = []

    for drink in all_drinks:
        for i in range(1, 16):
            ingredient = drink[f'strIngredient{i}']
            if ingredient is not None and ingredient != '':
                ingredient = ingredient.encode("iso-8859-1").decode('utf-8')
                ingredient = remove_accents(ingredient)

                if ingredient.lower() in case_insensitive_set:
                    actual_key = case_insensitive_set[ingredient.lower()]
                    drink_ingredients.append({'drinkId': drink['idDrink'],
                                              'ingredientId': name_to_id_ingredients[actual_key]})
                else:
                    logging.warning(f'Unstored ingredient: {ingredient}. It\'s possible no entry was found in cocktaildb')
                    continue

    # Remove any duplicate drink -> ingredients (a drink may have 2 of the same ingredients listed for some reason)
    drink_ingredients = [dict(t) for t in {tuple(d.items()) for d in drink_ingredients}]

    write_file = write_csv(file_loc, "drink_link_ingredients", drink_ingredients, ['drinkId', 'ingredientId'])

    return {"status": "success", "msg": "Links ready to be loaded to database", "file_location": write_file}, 200


@app.route("/drink/link/ingredients/store", methods=['POST'])
def store_drinks_to_ingredients_links():
    logging.info('Storing drink ingredient links')
    request_json = request.json
    if request_json is None or 'link_file' not in request_json:
        return {"status": "fail", "msg": "Add a \"link_file\" parameter to specify what file to load"}, 400
    logging.info(request_json)

    file_loc = request_json['link_file']
    result = store_csv_to_db(file_loc, "drink_ingredients_link")
    return {"status": "success", "msg": result}, 200


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 6000)))
