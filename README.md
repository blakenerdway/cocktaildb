# Example CocktailDB pipeline
Example pipeline to pull in data from thecocktaildb.com

## Running
Ensure docker is installed on your machine. If it's not, [install it](https://docs.docker.com/get-docker/). Once docker
is installed, we can 

## CocktailDB API workflow
In order to pull all the data from TheCocktailDB, there are a few endpoints to hit to get all the data. The following
section describes these endpoints in order of when to check them. At any point, if you would like to see the full API
documentation provided by TheCocktailDB, check it out [here](https://www.thecocktaildb.com/api.php).

### 1. Search for drinks based on first character
This endpoint is reached at https://thecocktaildb.com/api/json/v1/1/search.php?f=$CHARACTER. By going through the alphabet
and numerics, we can find all major drinks that are listed in the database. From what I saw, this does not list alterations
of the drinks (i.e., only "Mojito" is returned, not "Mango Mojito"). Because alterations are not listed, we need to 
return those as well somehow. Luckily, there's another endpoint we can hit for that

### 2. Search for alterations of drinks
After finding the name of the drink, we can search for alterations of it at: www.thecocktaildb.com/api/json/v1/1/search.php?s=$DRINK_NAME.
The resulting list of drinks is stored in the database (after they have been processed of course).

### 3. Search for ingredients
Each drink has a list of up to 15 ingredients it can have. These ingredients are not normalized strings (any type of 
accents can be used on the characters, and they can be upper or lower cased in any way). The ingredients
need to be cleaned up and then searched for in TheCocktailDB so that the information about these ingredients
can be stored in the database. The endpoint we will use to search for ingredients will be
www.thecocktaildb.com/api/json/v1/1/search.php?i=$INGREDIENT_NAME

## Processing data
After reading the previous section, it's apparent that there will need to be some data cleaning done in order to
facilitate relational modelling on the data. In general, the data should only have relevant fields and should be
normalized. Every time the data is pulled, it should be validated and then compared to current data to ensure that there
are no duplicates being stored in the database.

### Architecture
For this example, the pipeline focuses on Airflow to call the proper endpoints at TheCocktailDB, and calls custom 
"microservices" hosted by Flask in another Docker container. In actual production scenario, a data lake could be used to
trigger different services to work, rather than have the onus on Airflow to call these endpoints to do the work. 

###



## Future work
1. More visibility on pipeline run (time, how many records stored, etc...)
2. More error checking
3. Pulling all drinks rather than a subset
   1. Possibly using a dynamically generated dag based on parameters passed in (pass in the letters/numbers to search for)
4. Only drinks that haven't been seen yet are pulled. Update to check the modified date field in the returned object 

    
