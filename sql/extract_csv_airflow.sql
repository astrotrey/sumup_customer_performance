\pset format unaligned
\pset fieldsep ,
\pset footer off
\out 'csvs/{{params.tablename}}.csv'

SELECT *
FROM {{params.tablename}}
;
