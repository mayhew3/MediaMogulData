package com.mayhew3.mediamogul.model;

import com.mayhew3.mediamogul.db.DatabaseEnvironments;
import com.mayhew3.mediamogul.db.ExecutionEnvironment;
import com.mayhew3.mediamogul.db.ExecutionEnvironments;
import com.mayhew3.mediamogul.exception.MissingEnvException;
import com.mayhew3.postgresobject.dataobject.DataSchema;
import com.mayhew3.postgresobject.model.PostgresSchemaTest;

public class SchemaHerokuStagingTest extends PostgresSchemaTest {

  @Override
  public DataSchema getDataSchema() {
    return MediaMogulSchema.schema;
  }

  @Override
  public String getDBConnectionString() {
    try {
      ExecutionEnvironment thisEnvironment = ExecutionEnvironments.getThisEnvironment();
      return DatabaseEnvironments.environments.get("heroku-staging").getDatabaseUrl(thisEnvironment);
    } catch (MissingEnvException e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }
}