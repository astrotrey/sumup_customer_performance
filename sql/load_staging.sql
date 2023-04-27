/* Empty contents of the staging table */
TRUNCATE tablename;

/* Copy staging table from the extracted csv */
COPY tablename FROM 'csvs/tablename.csv' CSV HEADER;
