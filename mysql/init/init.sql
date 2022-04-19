CREATE DATABASE IF NOT EXISTS cocktail_db;
USE cocktail_db;

CREATE TABLE IF NOT EXISTS drinks
(
    id int PRIMARY KEY,
    name varchar(256),
    tags varchar(256), # Comma delimited Tags
    type varchar(32),
    glassType varchar(32),
    instructions longtext,
    imageUrl varchar(256)
);

CREATE TABLE IF NOT EXISTS ingredients
(
  id int PRIMARY KEY,
  name varchar(256),
  description longtext
);

CREATE TABLE IF NOT EXISTS drink_ingredients_link
(
    drinkId int,
    ingredientId int,
    PRIMARY KEY (drinkId, ingredientId),
    FOREIGN KEY (drinkId) references drinks(id),
    FOREIGN KEY (ingredientId) references ingredients(id)
);