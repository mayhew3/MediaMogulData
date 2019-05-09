setlocal
set fullBackupPath=D:\Projects\mean_projects\backups_postgres_local\2016_11_23_13_20

echo %fullBackupPath%
echo Restoring

aws s3 cp %fullBackupPath%.dump s3://mediamogulbackups --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

endlocal