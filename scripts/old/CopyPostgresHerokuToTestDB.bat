setlocal
For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c_%%a_%%b)
For /f "tokens=1-2 delims=/: " %%a in ("%TIME%") do (set mytime=%%a_%%b)
set fulldatetime=%mydate%_%mytime%
set fullBackupPath=%BACKUP_DIR_HEROKU%\%fulldatetime%

set PGPASSFILE=%postgres_pgpass_heroku%
echo %PGPASSFILE%

echo %fullBackupPath%
echo Backing up

call pg_dump --host=ec2-23-23-199-72.compute-1.amazonaws.com --dbname=dlo6ch087btcf --username=hwubispysgwomv --file=%fullBackupPath%.dump --format=custom

set PGPASSFILE=%postgres_pgpass_local%
echo %PGPASSFILE%

echo Restoring backup

call pg_restore --host=localhost --username=postgres --dbname=tv --clean --verbose --format=custom %fullBackupPath%.dump

endlocal