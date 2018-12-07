package com.mayhew3.mediamogul.dataobject;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValue;
import com.mayhew3.postgresobject.dataobject.FieldValueInteger;
import com.mayhew3.postgresobject.dataobject.FieldValueString;
import com.mayhew3.postgresobject.db.PostgresConnection;
import com.mayhew3.mediamogul.model.tv.Episode;
import com.mayhew3.mediamogul.model.tv.TiVoEpisode;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class EpisodeTest {

  private Episode episode;
  private Integer INITIAL_ID = 5;

  @Before
  public void setUp() {
    episode = new Episode();
    episode.id.changeValue(INITIAL_ID);
  }

  @Test
  public void testAddToTiVoEpisodes() {

  }

  @Test
  public void testGetTiVoEpisodes() throws SQLException {
    PostgresConnection postgresConnection = mock(PostgresConnection.class);
    ResultSet resultSet = mock(ResultSet.class);

    when(postgresConnection.prepareAndExecuteStatementFetch(anyString(), eq(INITIAL_ID), anyInt())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);

    List<TiVoEpisode> tiVoEpisodes = episode.getTiVoEpisodes(postgresConnection);

    assertThat(tiVoEpisodes)
        .hasSize(1);

    DataObject tiVoEpisode = tiVoEpisodes.get(0);

    verify(resultSet).getInt("id");

    for (FieldValue fieldValue : tiVoEpisode.getAllFieldValues()) {
      if (fieldValue instanceof FieldValueString) {
        verify(resultSet).getString(fieldValue.getFieldName());
      } else if (fieldValue instanceof FieldValueInteger) {
        verify(resultSet).getInt(fieldValue.getFieldName());
      }
      // todo: assert other types
//      verify(resultSet).getObject(fieldValue.getFieldName());
    }
  }
}