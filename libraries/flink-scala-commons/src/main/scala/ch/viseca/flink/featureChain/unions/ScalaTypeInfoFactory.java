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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class ScalaTypeInfoFactory<T extends WithScalaTypeInfoFactory> extends TypeInfoFactory<T> {
    @SuppressWarnings({"unchecked", "unsafe"})
    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> map) {
        if (registeredScalaTypeInfo.containsKey(t)) {
            return (TypeInformation<T>) registeredScalaTypeInfo.get(t);
        } else if (registeredScalaTypeInfoFactories.containsKey(t)) {
            return ((TypeInfoFactory<T>) registeredScalaTypeInfoFactories.get(t)).createTypeInfo(t, map);
        } else {
            throw new InvalidTypesException(
                    "No TypeInfoFactory registered for scala type '"
                            + t.getTypeName()
                            + "' Remember to call ScalaTypeInfoFactory.registerFactory(clazz, factory) for generic types, or"
                            + " ScalaTypeInfoFactory.registerTypeInfo(clazz, typeInfo) before evoking type info factory for the type."
            );
        }
    }

    private static Map<Type, TypeInfoFactory<WithScalaTypeInfoFactory>> registeredScalaTypeInfoFactories = new HashMap<Type, TypeInfoFactory<WithScalaTypeInfoFactory>>();
    private static Map<Type, TypeInformation<? extends WithScalaTypeInfoFactory>> registeredScalaTypeInfo = new HashMap<Type, TypeInformation<? extends WithScalaTypeInfoFactory>>();

    @SuppressWarnings({"unchecked", "unsafe"})
    public static void registerFactory(Type t, TypeInfoFactory factory) {
        Preconditions.checkNotNull(t, "Type parameter must not be null.");
        Preconditions.checkNotNull(factory, "Factory parameter must not be null.");
        registeredScalaTypeInfoFactories.put(t, factory);
    }

    @SuppressWarnings({"unchecked", "unsafe"})
    public static void registerTypeInfo(Type t, TypeInformation<? extends WithScalaTypeInfoFactory> typeInfo) {
        Preconditions.checkNotNull(t, "Type parameter must not be null.");
        Preconditions.checkNotNull(typeInfo, "typeInfo parameter must not be null.");
        registeredScalaTypeInfo.put(t, typeInfo);
    }
}
