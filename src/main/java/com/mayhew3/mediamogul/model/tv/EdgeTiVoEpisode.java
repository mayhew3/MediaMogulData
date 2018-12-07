package com.mayhew3.mediamogul.model.tv;

import com.mayhew3.postgresobject.dataobject.DataObject;
import com.mayhew3.postgresobject.dataobject.FieldValueForeignKey;
import com.mayhew3.postgresobject.dataobject.Nullability;

public class EdgeTiVoEpisode extends DataObject {

  public FieldValueForeignKey episodeId = registerForeignKey(new Episode(), Nullability.NOT_NULL);
  public FieldValueForeignKey tivoEpisodeId = registerForeignKey(new TiVoEpisode(), Nullability.NOT_NULL);

  @Override
  public String getTableName() {
    return "edge_tivo_episode";
  }

  @Override
  public String toString() {
    return id.getValue() + ": " + tivoEpisodeId.getValue() + " " + episodeId.getValue();
  }
}
