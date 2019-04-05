setlocal
:Running
echo Full Updater (Heroku) is running
java -classpath "D:\Projects\mean_projects\GamesDBUtil\out\artifacts\GamesDBUtil_jar\GamesDBUtil.jar" -Dwebdriver.chrome.driver="C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe" com.mayhew3.mediamogul.games.SteamGameUpdateRunner LogToFile Heroku
java -classpath "D:\Projects\mean_projects\GamesDBUtil\out\artifacts\GamesDBUtil_jar\GamesDBUtil.jar" com.mayhew3.mediamogul.games.MetacriticGameUpdateRunner LogToFile Heroku
java -classpath "D:\Projects\mean_projects\GamesDBUtil\out\artifacts\GamesDBUtil_jar\GamesDBUtil.jar" com.mayhew3.mediamogul.tv.TiVoLibraryUpdater FullMode LogToFile Heroku
endlocal