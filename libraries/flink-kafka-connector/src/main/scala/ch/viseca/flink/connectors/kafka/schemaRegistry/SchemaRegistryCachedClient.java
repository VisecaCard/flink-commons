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
package ch.viseca.flink.connectors.kafka.schemaRegistry;

import ch.viseca.flink.connectors.kafka.schemaRegistry.messages.ConfigUpdateRequest1;
import ch.viseca.flink.connectors.kafka.schemaRegistry.messages.Schema1;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/** A cloned implementation of {@link io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient } that
 * allows to inject a {@link SchemaRegistryRestClient} instead of connection details
 * to connect to a Confluent schema registry.
 * <p>
 *     The original implementation is closed for extensions and adjustments, e.g. is not compatible with
 *     the schema registry proxy provided by Lenses (same REST API different encoding scheme)
 * </p>
 * */
public class SchemaRegistryCachedClient implements SchemaRegistryClient, Serializable {
    private final SchemaRegistryRestClient restService;
    private int identityMapCapacity;
    private final Map<String, Map<Schema, Integer>> schemaCache;
    private final Map<String, Map<Integer, Schema>> idCache;
    private final Map<String, Map<Schema, Integer>> versionCache;

    public SchemaRegistryCachedClient(SchemaRegistryRestClient restService, int identityMapCapacity) {
        this.identityMapCapacity = identityMapCapacity;
        this.schemaCache = new HashMap<String, Map<Schema, Integer>>();
        this.idCache = new HashMap<String, Map<Integer, Schema>>();
        this.versionCache = new HashMap<String, Map<Schema, Integer>>();
        this.restService = restService;
        this.idCache.put((String) null, new HashMap<Integer, Schema>());
    }

    public SchemaRegistryCachedClient(SchemaRegistryRestClient restService) {
        this.identityMapCapacity = -1;
        this.schemaCache = new HashMap<String, Map<Schema, Integer>>();
        this.idCache = new HashMap<String, Map<Integer, Schema>>();
        this.versionCache = new HashMap<String, Map<Schema, Integer>>();
        this.restService = restService;
        this.idCache.put((String) null, new HashMap<Integer, Schema>());
    }

    public SchemaRegistryRestClient getRestService() {
        return this.restService;
    }

    public void setIdentityMapCapacity(int identityMapCapacity) {
        this.identityMapCapacity = identityMapCapacity;
    }

    private int registerAndGetId(String subject, Schema schema) throws IOException {
        return this.restService.registerSchema(schema.toString(), subject);
    }

    private Schema getSchemaByIdFromRegistry(int id) throws IOException {
        String restSchema = this.restService.getId(id);
        return (new Schema.Parser()).parse(restSchema);
    }

    private int getVersionFromRegistry(String subject, Schema schema) throws IOException {
        Schema1 response = this.restService.lookUpSubjectVersion(schema.toString(), subject, true);
        return response.getVersion();
    }

    public synchronized int register(String subject, Schema schema) throws IOException {
        Map<Schema, Integer> schemaIdMap;
        if (this.schemaCache.containsKey(subject)) {
            schemaIdMap = (Map<Schema, Integer>) this.schemaCache.get(subject);
        } else {
            schemaIdMap = new IdentityHashMap<Schema, Integer>();
            this.schemaCache.put(subject, schemaIdMap);
        }

        if (((Map) schemaIdMap).containsKey(schema)) {
            return (Integer) ((Map) schemaIdMap).get(schema);
        } else if (((Map) schemaIdMap).size() >= this.identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        } else {
            int id = this.registerAndGetId(subject, schema);
            ((Map<Schema, Integer>) schemaIdMap).put(schema, id);
            ((Map<Integer, Schema>) this.idCache.get((Object) null)).put(id, schema);
            return id;
        }
    }

    @Deprecated
    public Schema getByID(int id) throws IOException {
        return this.getById(id);
    }

    public synchronized Schema getById(int id) throws IOException {
        return this.getBySubjectAndId((String) null, id);
    }

    @Deprecated
    public Schema getBySubjectAndID(String subject, int id) throws IOException {
        return this.getBySubjectAndId(subject, id);
    }

    public synchronized Schema getBySubjectAndId(String subject, int id) throws IOException {
        Map<Integer, Schema> idSchemaMap;
        if (this.idCache.containsKey(subject)) {
            idSchemaMap = (Map<Integer, Schema>) this.idCache.get(subject);
        } else {
            idSchemaMap = new HashMap<Integer, Schema>();
            this.idCache.put(subject, idSchemaMap);
        }

        if (((Map<Integer, Schema>) idSchemaMap).containsKey(id)) {
            return (Schema) ((Map<Integer, Schema>) idSchemaMap).get(id);
        } else {
            Schema schema = this.getSchemaByIdFromRegistry(id);
            ((Map<Integer, Schema>) idSchemaMap).put(id, schema);
            return schema;
        }
    }

    public SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException {
        Schema1 response = this.restService.getVersion(subject, version);
        int id = response.getId();
        String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    public synchronized SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException {
        Schema1 response = this.restService.getLatestVersion(subject);
        int id = response.getId();
        int version = response.getVersion();
        String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    public synchronized int getVersion(String subject, Schema schema) throws IOException {
        Map<Schema, Integer> schemaVersionMap;
        if (this.versionCache.containsKey(subject)) {
            schemaVersionMap = (Map<Schema, Integer>) this.versionCache.get(subject);
        } else {
            schemaVersionMap = new IdentityHashMap<Schema, Integer>();
            this.versionCache.put(subject, schemaVersionMap);
        }

        if (((Map<Schema, Integer>) schemaVersionMap).containsKey(schema)) {
            return (Integer) ((Map<Schema, Integer>) schemaVersionMap).get(schema);
        } else if (((Map<Schema, Integer>) schemaVersionMap).size() >= this.identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        } else {
            int version = this.getVersionFromRegistry(subject, schema);
            ((Map<Schema, Integer>) schemaVersionMap).put(schema, version);
            return version;
        }
    }

    public boolean testCompatibility(String subject, Schema schema) throws IOException {
        return this.restService.testCompatibility(schema.toString(), subject, "latest");
    }

    public String updateCompatibility(String subject, String compatibility) throws IOException {
        ConfigUpdateRequest1 response = this.restService.updateConfig(subject, compatibility);
        return response.getCompatibility();
    }

    public String getCompatibility(String subject) throws IOException {
        Config response = this.restService.getConfig(subject);
        return response.getCompatibilityLevel();
    }

    public Collection<String> getAllSubjects() throws IOException {
        return this.restService.getAllSubjects();
    }
}

