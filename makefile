to_mysql:
	docker exec -it de_mysql mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}
to_mysql_root:
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}
mysql_create:
	docker exec -it mysql bash -c "mysql -u admin -padmin olist < /tmp/load_dataset/olist.sql"
mysql_load:
	docker exec -it mysql bash -c "mysql --local_infile=1 -u admin -padmin olist < /tmp/load_dataset/load_data.sql"
to_postgres:
	docker exec -it de_postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"
postgres_create:
	docker exec -it de_postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /tmp/load_dataset/postgres_datasource.sql
postgres_load:
	docker exec -it de_postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /tmp/load_dataset/postgres_load.sql
setup_postgres_schema:
	docker exec -it de_postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE SCHEMA IF NOT EXISTS staging;"
	docker exec -it de_postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "CREATE SCHEMA IF NOT EXISTS warehouse;"

