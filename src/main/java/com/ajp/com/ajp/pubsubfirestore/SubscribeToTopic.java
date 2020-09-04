/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ajp.com.ajp.pubsubfirestore;

// [START functions_pubsub_subscribe]

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.*;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.ajp.com.ajp.pubsubfirestore.eventpojos.PubSubMessage;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

@Data
@AllArgsConstructor
@NoArgsConstructor
class AuditObject {
  private String correlationId;
  private String source;
  private String clientIPAddress;
  private String timeStamp;

}

public class SubscribeToTopic implements BackgroundFunction<PubSubMessage> {
  private static final Logger logger = Logger.getLogger(SubscribeToTopic.class.getName());

  private static final String TYPE = "Type";
  private static final String SERVICE_NAME = "ServiceName";

  @Override
  public void accept(PubSubMessage message, Context context) throws IOException, ExecutionException, InterruptedException {
    if (message.getData() == null) {
      logger.info("No message provided");
      return;
    }

    String collectionName = null;
//    String documentName = message.getMessageId();

    Map<String, String> attributes = message.getAttributes();
    if(attributes==null || attributes.isEmpty()) {
      logger.info("Type attribute needed");
      return;
    }

    collectionName = attributes.get(TYPE);


    logger.info("collectionName: "+collectionName);
//    logger.info("documentName: "+documentName);

    String messageString = new String(
        Base64.getDecoder().decode(message.getData().getBytes(StandardCharsets.UTF_8)),
        StandardCharsets.UTF_8);
    AuditObject auditObject = new ObjectMapper().readValue(messageString, AuditObject.class);

    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    String projectId="ionic-angular-app-982ab";
    FirestoreOptions firestoreOptions =
            FirestoreOptions.getDefaultInstance().toBuilder()
                    .setProjectId(projectId)
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .build();
    Firestore db = firestoreOptions.getService();

    CollectionReference collection = db.collection(collectionName);
//    DocumentReference docRef = collection.document(documentName);
//asynchronously write data
//    ApiFuture<WriteResult> result = docRef.set(auditObject);
    ApiFuture<DocumentReference> add = collection.add(auditObject);
    logger.info("Added Path : " + add.get().getPath());

    logger.info(messageString);
  }
}
// [END functions_pubsub_subscribe]
