package com.mayhew3.gamesutil.model.tv;

import com.mayhew3.gamesutil.dataobject.DataObject;
import com.mayhew3.gamesutil.dataobject.FieldValueForeignKey;
import com.mayhew3.gamesutil.dataobject.FieldValueInteger;
import com.mayhew3.gamesutil.dataobject.Nullability;

public class EdgeTiVoEpisode extends DataObject {

  public FieldValueForeignKey episodeId = registerForeignKey("episode_id", new Episode(), Nullability.NOT_NULL);
  public FieldValueForeignKey tivoEpisodeId = registerForeignKey("tivo_episode_id", new TiVoEpisode(), Nullability.NOT_NULL);

  @Override
  protected String getTableName() {
    return "edge_tivo_episode";
  }

  @Override
  public String toString() {
    return id.getValue() + ": " + tivoEpisodeId.getValue() + " " + episodeId.getValue();
  }
}