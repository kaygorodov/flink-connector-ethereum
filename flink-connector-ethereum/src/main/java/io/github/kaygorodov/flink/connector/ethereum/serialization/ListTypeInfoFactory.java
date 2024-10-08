/*
 * Copyright © 2024 Andrei Kaigorodov (andreykaygorodov@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.kaygorodov.flink.connector.ethereum.serialization;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

@Internal
public class ListTypeInfoFactory<E> extends TypeInfoFactory<List<E>> {

  @Override
  public TypeInformation<List<E>> createTypeInfo(Type te,
      Map<String, TypeInformation<?>> genericParameters) {

    TypeInformation<?> listElementType = genericParameters.get("E");

    if (listElementType == null) {
      throw new InvalidTypesException(
          "Cannot extract type information for List because the element type is unknown");
    }

    return Types.LIST((TypeInformation<E>) listElementType);
  }
}
