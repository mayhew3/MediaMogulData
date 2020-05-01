package com.mayhew3.mediamogul.model;

import com.mayhew3.postgresobject.EnvironmentChecker;
import com.mayhew3.postgresobject.dataobject.DataSchema;
import com.mayhew3.postgresobject.exception.MissingEnvException;
import com.mayhew3.postgresobject.model.PostgresSchemaTest;

public class SchemaTestTest extends PostgresSchemaTest {

  @Override
  public DataSchema getDataSchema() {
    return MediaMogulSchema.schema;
  }

  @Override
  public String getDBConnectionString() {
    try {
      return EnvironmentChecker.getOrThrow("postgresURL_local_test");
    } catch (MissingEnvException e) {
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }
}