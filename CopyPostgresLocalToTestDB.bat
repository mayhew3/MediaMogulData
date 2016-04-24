setlocal
set fullBackupPath=D:\Projects\mean_projects\backups_postgres_local\2016_04_22_0_09

set PGPASSFILE=%postgres_pgpass_local%
echo %PGPASSFILE%

echo %fullBackupPath%
echo Restoring

call pg_restore --host=localhost --username=postgres --dbname=tv_copy --clean --verbose --format=custom %fullBackupPath%.dump

endlocal