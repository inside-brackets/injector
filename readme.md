# Description 
The python script is designed to inject/update carriers data into mongo-db.

The injection logic revolves around searching for existing carrier data in the MongoDB collection based on the MC number. Depending on whether a matching carrier is found and its "up-to-date" status, the script either updates the existing document or creates a new one. This logic ensures that carrier data is effectively managed and injected into the database while handling potential updates and deletions as needed.

# Build
Here is an example on how to build this script:
```
pyinstaller --noconfirm --onedir --console --add-data "path/to/config.yaml;." --add-data "path/to/files;files/"  "path/to/main.py"
```

# File names

**Numeric prefix**: The filenames in the files directory should begin with a numeric prefix. The script uses this prefix to order and process the files in ascending order based on the numeric value.

**CSV Extension**: The filenames must have the .csv extension to be recognized and processed by the script.

```
project
|   script.py
|   config.yaml
|   readme.md  
└───files
    |   0.csv
    |   1.csv
    |   ...
```
