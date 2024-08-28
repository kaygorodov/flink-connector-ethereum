package org.kaigorodov.flink.connector.ethereum.serialization;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

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
