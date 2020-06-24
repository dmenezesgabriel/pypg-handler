# Pull official PostgreSQL image from dockerhub https://hub.docker.com/
FROM postgres
# Set environment variables
ENV POSTGRES_PASSWORD postgres
ENV POSTGRES_DB postgres
# By defaults every script located at /docker-entrypoint-initdb.d/
# will be automatically executed during container startup
COPY init.sql /docker-entrypoint-initdb.d/*.sql