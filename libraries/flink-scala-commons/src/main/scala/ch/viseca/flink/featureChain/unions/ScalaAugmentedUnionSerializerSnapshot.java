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
package ch.viseca.flink.featureChain.unions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.*;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import scala.Option;
import scala.Symbol;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ScalaAugmentedUnionSerializerSnapshot} is a convenient serializer snapshot class that can be used by
 * discriminated union serializers which 1) delegates its serialization to multiple nested serializers, and 2) contain
 * some extra static information that needs to be persisted as part of its snapshot.
 *
 * <h2>Snapshot Versioning</h2>
 *
 * <p>This class is modelled after {@link CompositeTypeSerializerSnapshot} which allows to compose nested
 * serializer snapshots but does not allow the set of nested serializers to be extended, assuming the writer
 * snapshot covers a strict subset of nested snapshots compared to the set of nested snapshots of the reader snapshot,
 * and reader and writer snapshots can be correlated resp. by means of the type tag.
 * This yields compatibility-as-is.
 * </p>
 *
 * <p>This class has its own versioning for the format in which it writes the outer snapshot and the
 * nested serializer snapshots. The version of the serialization format of this based class is defined
 * by {@link #getCurrentVersion()}. This is independent of the version in which subclasses writes their outer snapshot,
 * defined by {@link #getCurrentOuterSnapshotVersion()}.
 * This means that the outer snapshot's version can be maintained only taking into account changes in how the
 * outer snapshot is written. Any changes in the base format does not require upticks in the outer snapshot's version.
 *
 * <h2>Serialization Format</hr>
 *
 * <p>The current version of the serialization format of a {@link ScalaAugmentedUnionSerializerSnapshot} is as follows:
 *
 * <pre>{@code
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | CompositeTypeSerializerSnapshot | CompositeTypeSerializerSnapshot |          Outer snapshot         |
 * |           version               |          MAGIC_NUMBER           |              version            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                               Outer snapshot                                        |
 * |                                   #writeOuterSnapshot(DataOutputView out)                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      Delegate MAGIC_NUMBER      |         Delegate version        |     Num. nested serializers     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                     Nested serializer snapshots                                     |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }</pre>
 *
 * @param <U> The data type that the originating serializer of this snapshot serializes.
 * @param <S> The type of the originating serializer.
 */

public class ScalaAugmentedUnionSerializerSnapshot<U extends UnionBase, S extends ScalaUnionSerializer<U>>
        implements TypeSerializerSnapshot<U> {
    /**
     * Magic number for integrity checks during deserialization.
     */
    private static final int MAGIC_NUMBER = 51324589;

    /**
     * Current version of the base serialization format.
     *
     * <p>NOTE: We start from version 3. This version is represented by the {@link #getCurrentVersion()} method.
     * Previously, this method was used to represent the outer snapshot's version (now, represented
     * by the {@link #getCurrentOuterSnapshotVersion()} method).
     *
     * <p>To bridge this transition, we set the starting version of the base format to be at least
     * larger than the highest version of previously defined values in implementing subclasses,
     * which was {@link #HIGHEST_LEGACY_READ_VERSION}. This allows us to identify legacy deserialization paths,
     * which did not contain versioning for the base format, simply by checking if the read
     * version of the snapshot is smaller than or equal to {@link #HIGHEST_LEGACY_READ_VERSION}.
     */
    private static final int VERSION = 3;

    private static final int HIGHEST_LEGACY_READ_VERSION = 2;

    private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

    @VisibleForTesting
    final Class<S> correspondingSerializerClass;
    @VisibleForTesting
    Class<U> unionClass;
    @VisibleForTesting
    String[] unionTags;

    /**
     * Constructor to be used for read instantiation (class loader nullary constructor invocation).
     * Otherwise unused
     */
    @SuppressWarnings("unchecked")
    public ScalaAugmentedUnionSerializerSnapshot() {
        this.correspondingSerializerClass = (Class<S>) (Class<? extends TypeSerializer>) ScalaUnionSerializer.class;
    }

    /**
     * Constructor to be used for writing the snapshot.
     *
     * @param serializerInstance an instance of the originating serializer of this snapshot.
     */
    @SuppressWarnings("unchecked")
    public ScalaAugmentedUnionSerializerSnapshot(S serializerInstance) {
        checkNotNull(serializerInstance);
        this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(getNestedSerializers(serializerInstance));
        this.correspondingSerializerClass = (Class<S>) serializerInstance.getClass();
        this.unionClass = serializerInstance.unionClass();
        this.unionTags = serializerInstance.unionTags();
    }

    @Override
    public final int getCurrentVersion() {
        return VERSION;
    }

    private void checkInitialized() {
        checkNotNull(nestedSerializersSnapshotDelegate);
        checkNotNull(correspondingSerializerClass);
        checkNotNull(unionTags);
        checkNotNull(unionClass);
    }

    @Override
    public final void writeSnapshot(DataOutputView out) throws IOException {
        checkInitialized();
        internalWriteOuterSnapshot(out);
        nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
    }

    @Override
    public final void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        checkState(readVersion > HIGHEST_LEGACY_READ_VERSION, "readVersion must be greater than 2");
        if (readVersion > HIGHEST_LEGACY_READ_VERSION) {
            internalReadOuterSnapshot(in, userCodeClassLoader);
        }
        //there was no legacy version ever serialized
        //        else {
        //            legacyInternalReadOuterSnapshot(readVersion, in, userCodeClassLoader);
        //        }
        this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(in, userCodeClassLoader);
    }

    @Override
    public final TypeSerializerSchemaCompatibility<U> resolveSchemaCompatibility(TypeSerializer<U> newSerializer) {
        return internalResolveSchemaCompatibility(newSerializer, nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots());
    }

    @Internal
    TypeSerializerSchemaCompatibility<U> internalResolveSchemaCompatibility(
            TypeSerializer<U> newSerializer,
            TypeSerializerSnapshot<?>[] snapshots) {
        if (newSerializer.getClass() != correspondingSerializerClass) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        S newUnionSerializer = correspondingSerializerClass.cast(newSerializer);

        // check that outer configuration is compatible; if not, short circuit result
        if (!isOuterSnapshotCompatible(newUnionSerializer)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        //we'll replace reconfigured serializers, if we need to
        Map<Symbol, TypeSerializer<?>> nestedNewSerializerMap = newUnionSerializer.serializers();

        // check nested serializers for compatibility
        boolean nestedSerializerRequiresMigration = false;
        boolean hasReconfiguredNestedSerializers = false;
        for (int tagIndex = 0; tagIndex < unionTags.length; tagIndex++) {
            Symbol tag = Symbol.apply(unionTags[tagIndex]);
            Option<TypeSerializer<?>> newNestedSerializer = nestedNewSerializerMap.get(tag);
            if (newNestedSerializer.isEmpty()) {
                //new serializer removed a tag:
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            TypeSerializerSchemaCompatibility<?> compatibility = resolveCompatibility(newNestedSerializer.get(), snapshots[tagIndex]);
            if (compatibility.isIncompatible()) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }
            if (compatibility.isCompatibleAfterMigration()) {
                nestedSerializerRequiresMigration = true;
            } else if (compatibility.isCompatibleWithReconfiguredSerializer()) {
                hasReconfiguredNestedSerializers = true;
                nestedNewSerializerMap = nestedNewSerializerMap.updated(tag, compatibility.getReconfiguredSerializer());
            } else if (compatibility.isCompatibleAsIs()) {
            } else {
                throw new IllegalStateException("Undefined compatibility type.");
            }
        }

        if (nestedSerializerRequiresMigration) {
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        if (hasReconfiguredNestedSerializers) {
            int nestedCount = nestedNewSerializerMap.size();
            String[] reconfiguredUnionTags = new String[nestedCount];
            TypeSerializer<?>[] reconfiguredTypeSerializers = new TypeSerializer<?>[nestedCount];

            Iterator<Tuple2<Symbol, TypeSerializer<?>>> iter = nestedNewSerializerMap.iterator();
            int reconfIndex = 0;
            while (iter.hasNext()) {
                Tuple2<Symbol, TypeSerializer<?>> elem = iter.next();
                reconfiguredUnionTags[reconfIndex] = elem._1.name();
                reconfiguredTypeSerializers[reconfIndex] = elem._2;
                reconfIndex++;
            }


            TypeSerializer<U> reconfiguredCompositeSerializer = ScalaUnionSerializer.createSerializerFromSnapshot(unionClass, reconfiguredUnionTags, reconfiguredTypeSerializers);
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(reconfiguredCompositeSerializer);
        }

        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    @SuppressWarnings("unchecked")
    <E> TypeSerializerSchemaCompatibility<E> resolveCompatibility(
            TypeSerializer<?> serializer,
            TypeSerializerSnapshot<?> snapshot) {

        TypeSerializer<E> typedSerializer = (TypeSerializer<E>) serializer;
        TypeSerializerSnapshot<E> typedSnapshot = (TypeSerializerSnapshot<E>) snapshot;

        return typedSnapshot.resolveSchemaCompatibility(typedSerializer);
    }

    @Override
    public final TypeSerializer<U> restoreSerializer() {
        checkInitialized();
        @SuppressWarnings("unchecked")
        TypeSerializer<U> serializer = (TypeSerializer<U>)
                createOuterSerializerWithNestedSerializers(nestedSerializersSnapshotDelegate.getRestoredNestedSerializers());

        return serializer;
    }

    // ------------------------------------------------------------------------------------------
    //  Outer serializer access methods
    // ------------------------------------------------------------------------------------------

    /**
     * Returns the version of the current outer snapshot's written binary format.
     *
     * @return the version of the current outer snapshot's written binary format.
     */
    protected int getCurrentOuterSnapshotVersion() {
        return VERSION;
    }

    /**
     * Gets the nested serializers from the outer serializer.
     *
     * @param outerSerializer the outer serializer.
     * @return the nested serializers.
     */
    protected TypeSerializer<?>[] getNestedSerializers(ScalaUnionSerializer<U> outerSerializer) {
        return outerSerializer.typeSerializers();
    }

    /**
     * Creates an instance of the outer serializer with a given array of its nested serializers.
     *
     * @param nestedSerializers array of nested serializers to create the outer serializer with.
     * @return an instance of the outer serializer.
     */
    protected ScalaUnionSerializer<U> /*S*/ createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
        checkState(unionClass != null, "unionClass can not be NULL");
        checkState(unionTags != null, "unionTags can not be NULL");
        checkState(unionTags.length == nestedSerializers.length, "unionTags.length and nestedSerializers.length should be equal");
        return ScalaUnionSerializer.createSerializerFromSnapshot(unionClass, unionTags, nestedSerializers);
    }

    /**
     * Writes the outer snapshot, i.e. any information beyond the nested serializers of the outer serializer.
     *
     * @param out the {@link DataOutputView} to write the outer snapshot to.
     */
    protected void writeOuterSnapshot(DataOutputView out) throws IOException {
        checkState(unionClass != null, "unionClass can not be NULL");
        checkState(unionTags != null, "unionTags can not be NULL");
        out.writeUTF(unionClass.getName());
        out.writeInt(unionTags.length);
        for (String tag : unionTags) {
            out.writeUTF(tag);
        }
    }

    /**
     * Reads the outer snapshot, i.e. any information beyond the nested serializers of the outer serializer.
     *
     * @param readOuterSnapshotVersion the read version of the outer snapshot.
     * @param in                       the {@link DataInputView} to read the outer snapshot from.
     * @param userCodeClassLoader      the user code class loader.
     */
    protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        checkState(readOuterSnapshotVersion == getCurrentOuterSnapshotVersion(), "version conflict of reader/writer outer snapshot version");
        unionClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
        int tagCount = in.readInt();
        unionTags = new String[tagCount];
        for (int tagIndex = 0; tagIndex < tagCount; tagIndex++) {
            unionTags[tagIndex] = in.readUTF();
        }
    }

    /**
     * Checks whether the outer snapshot is compatible with a given new serializer.
     *
     * @param newSerializer the new serializer, which contains the new outer information to check against.
     * @return a flag indicating whether or not the new serializer's outer information is compatible with the one
     * written in this snapshot.
     */
    protected boolean isOuterSnapshotCompatible(ScalaUnionSerializer<U> newSerializer) {
        return Objects.equals(unionClass, newSerializer.unionClass());
    }

    // ------------------------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------------------------

    private void internalWriteOuterSnapshot(DataOutputView out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(getCurrentOuterSnapshotVersion());

        writeOuterSnapshot(out);
    }

    private void internalReadOuterSnapshot(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(String.format("Corrupt data, magic number mismatch. Expected %8x, found %8x",
                    MAGIC_NUMBER, magicNumber));
        }

        final int outerSnapshotVersion = in.readInt();
        readOuterSnapshot(outerSnapshotVersion, in, userCodeClassLoader);
    }

}
