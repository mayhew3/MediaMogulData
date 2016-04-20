setlocal
For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c_%%a_%%b)
For /f "tokens=1-2 delims=/: " %%a in ("%TIME%") do (set mytime=%%a_%%b)
set fulldatetime=%mydate%_%mytime%
set fullBackupPath=D:\Projects\mean_projects\backups_postgres_local\%fulldatetime%

set PGPASSFILE=%postgres_pgpass_local%
echo %PGPASSFILE%

echo %fullBackupPath%
echo Backing up

call pg_dump --host=localhost --dbname=tv --username=postgres --file=%fullBackupPath%.dump --format=custom

endlocal