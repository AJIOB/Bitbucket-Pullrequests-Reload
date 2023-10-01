# Bitbucket Server PullRequests Reload

All readme info is available on the head of every file.

## Scripts

|Script|Short Description|
|---|---|
|`data_import.py`| Single repository manupulation with granular options|
|`data_import_multiple.py`| Import all required info for multiple repositories|
|`load_all_diffs.py`| Load all PRs diffs|
|`load_all_images.py`| Dump all images from the BitBucket.org PRs|

You should use `data_import_multiple.py` in the almost all cases for data loading

## Dependencies

* Python 3.10 as a main executable.
* Docker Compose for dumping PR info via Ruby executables.

All Python module dependencies should be installed via calling:

```sh
python3.10 -m pip install -r requirements.txt
```

Ruby dependent repositories with October 2022 required fixes:
* [bitbucket-rest-api](https://github.com/AJIOB/ruby-bitbucket-rest-api)
* [export-pull-requests](https://github.com/AJIOB/export-pull-requests)

Ruby image should be run with the command:
```sh
docker compose up --build .
```
