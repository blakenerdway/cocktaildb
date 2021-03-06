import json

import requests

unique_test = [        "banana liqueur",
                       "egg white",
                       "orange bitters",
                       "rum",
                       "irish cream",
                       "dry vermouth",
                       "grenadine",
                       "everclear",
                       "blended whiskey",
                       "cherry heering",
                       "sour mix",
                       "creme de mure",
                       "sloe gin",
                       "jägermeister",
                       "triple sec",
                       "lemon juice",
                       "prosecco",
                       "ice",
                       "maraschino liqueur",
                       "green chartreuse",
                       "lime",
                       "mountain dew",
                       "peach schnapps",
                       "surge",
                       "lemon",
                       "bitters",
                       "baileys irish cream",
                       "lemon peel",
                       "pineapple juice",
                       "orange juice",
                       "hot chocolate",
                       "powdered sugar",
                       "rye whiskey",
                       "midori melon liqueur",
                       "scotch",
                       "tomato juice",
                       "151 proof rum",
                       "worcestershire sauce",
                       "tabasco sauce",
                       "blue curacao",
                       "angostura bitters",
                       "bacardi limon",
                       "maraschino cherry",
                       "galliano",
                       "cherry",
                       "sugar syrup",
                       "gin",
                       "kahlua",
                       "sambuca",
                       "southern comfort",
                       "sweet vermouth",
                       "corona",
                       "lime juice",
                       "grand marnier",
                       "wormwood",
                       "goldschlager",
                       "lemonade",
                       "champagne",
                       "vodka",
                       "passion fruit juice"]
if __name__ == '__main__':
    data = []
    for ingredient in unique_test:
        res = requests.get(f'https://thecocktaildb.com/api/json/v1/1/search.php', params={'i': ingredient})
        data.append(res.json()['ingredients'][0])

    with open('../data/temp/all_ingredients.json', 'w') as outfile:
        json.dump(data, outfile)