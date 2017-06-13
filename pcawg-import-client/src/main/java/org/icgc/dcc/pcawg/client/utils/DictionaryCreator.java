package org.icgc.dcc.pcawg.client.utils;

import com.fasterxml.jackson.databind.ObjectReader;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.submission.dictionary.model.Dictionary;
import org.icgc.dcc.submission.dictionary.model.FileSchema;

import java.net.URL;

import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_M_TYPE;
import static org.icgc.dcc.common.core.model.FileTypes.FileType.SSM_P_TYPE;

public class DictionaryCreator {
  private static final ObjectReader DICTIONARY_SCHEMA_READER = Jackson.DEFAULT.reader(Dictionary.class);

  public static DictionaryCreator newDictionaryCreator(String dictionaryUrl, String dictionaryVersion){
    return new DictionaryCreator(dictionaryUrl, dictionaryVersion);
  }

  public static DictionaryCreator newDictionaryCreator(String dictionaryCurrentUrl){
    return new DictionaryCreator(dictionaryCurrentUrl);
  }

  @SneakyThrows
  private static Dictionary readDictionary(URL dictionaryURL) {
    return (Dictionary)DICTIONARY_SCHEMA_READER.readValue(dictionaryURL);
  }

  @SneakyThrows
  private static URL getDictionaryUrl(String url, String version){
    val stringUrl = url+"/"+version;
    return new URL(stringUrl);
  }

  @SneakyThrows
  public static  Dictionary create(String urlString, String versionString){
    val url = getDictionaryUrl(urlString, versionString);
    return create(url);
  }
  @SneakyThrows
  public static  Dictionary create(URL url){
    return readDictionary(url);
  }

  private final Dictionary dictionary;

  public DictionaryCreator(@NonNull String dictionaryUrl, @NonNull String dictionaryVersion){
    this.dictionary = create(dictionaryUrl, dictionaryVersion);
  }

  @SneakyThrows
  public DictionaryCreator(@NonNull String dictionaryCurrentUrl){
    this.dictionary = create(new URL(dictionaryCurrentUrl));
  }


  public FileSchema getSSMPrimaryFileSchema(){
    return dictionary.getFileSchema(SSM_P_TYPE);
  }

  public FileSchema getSSMMetadataFileSchema(){
    return dictionary.getFileSchema(SSM_M_TYPE);
  }

}
