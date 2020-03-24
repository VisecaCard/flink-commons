/*
   Copyright 2020 Viseca Card Services SA

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package ch.viseca.flink.connectors.kafka.schemaRegistry

import java.util

import ch.viseca.flink.connectors.kafka.schemaRegistry.messages.{CompatibilityCheckResponse1, ConfigUpdateRequest1, Schema1, SchemaString1}
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.{CompatibilityCheckResponse, RegisterSchemaRequest, RegisterSchemaResponse}
import io.confluent.kafka.schemaregistry.client.rest.entities.{Config, Schema, SchemaString}
import javax.ws.rs.NotAuthorizedException
import javax.ws.rs.client._
import javax.ws.rs.core.MediaType
import org.apache.flink.annotation.Experimental
import org.glassfish.jersey.client.filter.EncodingFilter
import org.glassfish.jersey.jackson.JacksonFeature
import org.glassfish.jersey.message.GZipEncoder

import collection.JavaConverters._

/** A REST client implementation to connect to the schema registry proxy provided by Lenses
  *
  * @note This implementation is not production grade yet and only used for development.
  *
  * @param urls list of urls
  *
  * */
@Experimental
class SchemaRegistryRestClient(urls: String*) extends Serializable {

  /** Http client (lazy initialization) */
  @transient protected lazy val client: Client = createClient()

  /** Lenses authentication token */
  @transient protected var token: String = null

  var baseUrls = new util.ArrayList[String](urls.asJava)

  def getBaseUrls = baseUrls

  protected var credentials: SchemaRegistryClientCredentials = null

  /** Sets the credentials used to authenticate with Lenses schmea registry proxy */
  def setCredentials(credentials: SchemaRegistryClientCredentials) = this.credentials = credentials

  /** A Web request frame that initializes a [[javax.ws.rs.client.WebTarget]] and applies the request method
    * <p>
    * The [[javax.ws.rs.client.WebTarget]] is preconfigured for common use by the specific REST methods. It takes care
    * of Lenses authetication (i.e. if we get an not authorized response, performs authetication and retries the call)
    * </p>
    * <p>
    * Override this function if you
    * <ul>
    * <li>need other authentication scheme</li>
    * <li>need to switch between endpoints for fallback and reliability </li>
    * </ul>
    * </p>
    * */
  protected def tryRequest[T](request: WebTarget => T): T = {
    val currentServer = baseUrls.get(0)
    var target = client.target(currentServer)
    var srProxyTarget = target.path("proxy-sr")
    try {
      request(srProxyTarget)
    } catch {
      case e: NotAuthorizedException if credentials != null => {
        var loginTarget = target.path("login")
        token =
          loginTarget
            .request(MediaType.TEXT_PLAIN_TYPE)
            .post(Entity.entity(credentials, MediaType.APPLICATION_JSON_TYPE), classOf[String])
        request(srProxyTarget)
      }
    }
  }

  def getLatestVersion(subject: String) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}/versions/latest")
          .request(MediaType.WILDCARD_TYPE)
          .header("x-kafka-lenses-token", token)
          .get(classOf[Schema1])
      }
    )
  }

  def lookUpSubjectVersion(schemaString: String, subject: String) = {
    val request = new RegisterSchemaRequest
    request.setSchema(schemaString)
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), classOf[Schema1])
      }
    )
  }

  def lookUpSubjectVersion(schemaString: String, subject: String, lookupDeletedSchema: Boolean) = {
    val request = new RegisterSchemaRequest
    request.setSchema(schemaString)
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}")
          .queryParam("deleted", lookupDeletedSchema.toString)
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), classOf[Schema1])
      }
    )
  }

  def registerSchema(schemaString: String, subject: String) = {
    val request = new RegisterSchemaRequest
    request.setSchema(schemaString)
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}/versions")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), classOf[RegisterSchemaResponse])
          .getId
      }
    )
  }

  def testCompatibility(schemaString: String, subject: String, version: String) = {
    val request = new RegisterSchemaRequest
    request.setSchema(schemaString)
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/compatibility/subjects/${subject}/versions/${version}")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), classOf[CompatibilityCheckResponse1])
          .getIsCompatible
      }
    )
  }

  def getConfig(subject: String) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(if (subject == null) "/config" else s"/config/${subject}")
          .request(MediaType.WILDCARD_TYPE)
          .header("x-kafka-lenses-token", token)
          .get(classOf[Config])
      }
    )
  }

  def updateConfig(subject: String, compatibility: String) = {
    val request = new ConfigUpdateRequest1
    request.setCompatibility(compatibility)
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(if (subject == null) "/config" else s"/config/${subject}")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), classOf[ConfigUpdateRequest1])
      }
    )
  }

  def getId(id: Int) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/schemas/ids/${id}")
          .request(MediaType.WILDCARD_TYPE)
          .header("x-kafka-lenses-token", token)
          .get(classOf[SchemaString1])
          .getSchema
      }
    )
  }

  def getVersion(subject: String, version: Int) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}/versions/${version}")
          .request(MediaType.WILDCARD_TYPE)
          .header("x-kafka-lenses-token", token)
          .get(classOf[Schema1])
      }
    )
  }

  def getAllVersions(subject: String) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}/versions")
          .request(MediaType.WILDCARD_TYPE)
          .header("x-kafka-lenses-token", token)
          .get(classOf[java.util.List[Int]])
      }
    )
  }

  def getAllSubjects = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects")
          .request(MediaType.WILDCARD_TYPE)
          .header("x-kafka-lenses-token", token)
          .get(classOf[java.util.List[String]])
      }
    )
  }

  def deleteSchemaVersion(subject: String, version: String) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}/versions/${version}")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .delete(classOf[Int])
      }
    )
  }

  def deleteSubject(subject: String) = {
    tryRequest(
      (srTarget: WebTarget) => {
        srTarget
          .path(s"/subjects/${subject}")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .header("x-kafka-lenses-token", token)
          .delete(classOf[java.util.List[Int]])
      }
    )
  }

  /** Initializes the HTTP [[Client]] and configures encoding scheme (GZip encoding for Lenses schema registry proxy) */
  protected def createClient(): Client = {
    val client = ClientBuilder
      .newBuilder()
      .register(classOf[EncodingFilter])
      .register(classOf[GZipEncoder])
      .register(classOf[JacksonFeature])
      .build()
    client
  }
}

