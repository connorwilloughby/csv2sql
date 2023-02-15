# csv2sql
 
Just a small module build to convert a directory of csv targets to a psql create script! 

## install dependencies

```console
python -m venv venv
.\/venv/scripts/activate
python -m pip install --upgrade pip
python -m pip install --upgrade wheel
python -m pip install -r req.txt

```

## getting started

To use this tool just create a folder in the root of this project called `imports`, by placing one or many files in this dir you can then run the script. After running the script youll see a new file created called finished sql. 

## todos

- create a sql safe col lookup
- create a csv lookup from the above transformation
- create documentation
- convert to cmd function
- performance bump by only pulling headers?
