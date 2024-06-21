# psql-dump
Script for creating daily dumps of the database

Should be run with docker with the following command:
```zsh
docker run -e PSQL_HOST=10.0.0.1 -e PSQL_ROLE=django -v /path/to/local/dumps:/dumps  internethealthreport/psql-dump configs/country_hegemony_v4.json
```

