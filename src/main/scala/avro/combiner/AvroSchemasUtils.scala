package avro.combiner

import play.api.libs.json._

/**
 * User: vgordon
 * Date: 1/11/13
 * Time: 1:51 PM
 */

// class that is basically a JsArray represented as JsObject
case class JsObjectArray(array: Seq[JsValue]) extends JsObject(Seq("elements" -> JsArray(array))) {

}

trait AvroTraversal {
  def recordInitialValue(schema: JsValue): Option[JsValue] = None
  def traverseEnumType(schema: JsValue): JsObject = JsObject(Nil)
  def traverseFixedType(schema: JsValue): JsObject = JsObject(Nil)
  def traversePrimitiveType(schema: JsValue): JsObject = JsObject(Nil)
  def traverseReferenceType(schema: JsValue): JsObject = JsObject(Nil)
}

object InternallyDefinedSchemasExtractor extends AvroTraversal {
  override def recordInitialValue(schema: JsValue) = Some(schema)
  override def traverseEnumType(schema: JsValue) = JsObjectArray(Seq(schema))
  override def traverseFixedType(schema: JsValue) = JsObjectArray(Seq(schema))
}

object DependenciesExtractor extends AvroTraversal {
  override def traverseReferenceType(schema: JsValue) = JsObjectArray(Seq(schema))
}

object AvroSchemasUtils {
  implicit def toSeq(objectArray: JsObjectArray) = (objectArray \ "elements").as[JsArray].value
  implicit def toObjectArray(seq: Seq[JsValue]) = JsObjectArray(seq)

  val Primitive = "(null|boolean|int|long|float|double|bytes|string)".r

  def getNameWithNamespace(schema: JsObject) : String = {
    val namespace = (schema \ "namespace").as[String]
    val name = (schema \ "name").as[String]
    getNameWithNamespace(namespace, name)
  }

  def getNameWithNamespace(namespace: String, name: String) : String = {
    if (name.contains("."))
      name
    else
      namespace + "." + name
  }

  def extractValue(obj: JsObject) = {
    obj match {
      case JsObjectArray(x) => x
      case JsObject(Nil) => Nil
      case x => Seq(x)
    }
  }

  def traverseSchema(schema: JsValue, strategy: AvroTraversal) : JsObject = {
    def parseByType(schema: JsValue) : JsObject = {
      val schemaType = (schema \ "type")
      schemaType match {
        // union definition
        case JsArray(types) => types.map(traverseSchema(_, strategy)).flatMap(extractValue)
        // built-in type
        case JsString(str) => parseString(str)
        // type definition
        case JsObject(_) => traverseSchema(schemaType, strategy)
        case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
      }
    }

    def parseString(schemaType: String) : JsObject = {
      schemaType match {
        case "record" => {
          val recordFields = (schema \ "fields").as[JsArray].value
          val initialValue = strategy.recordInitialValue(schema) match {
            case Some(value) => Seq(value)
            case None => Nil
          }
          initialValue ++ recordFields.map(traverseSchema(_, strategy)).flatMap(extractValue)
        }
        case "array" => traverseSchema(schema \ "items", strategy)
        case "map" => traverseSchema(schema \ "values", strategy)
        case "enum" => strategy.traverseEnumType(schema)
        case "fixed" => strategy.traverseFixedType(schema)
        case Primitive(_) => strategy.traversePrimitiveType(schema)
        case _ => strategy.traverseReferenceType(JsString(schemaType)) // class reference
      }
    }

    schema match {
      case JsArray(elements) => elements.flatMap(traverseSchema(_, strategy).asInstanceOf[JsObjectArray])
      case JsObject(_) => parseByType(schema)
      case JsString(str) => parseString(str)
      case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
    }
  }

  def flattenSchema(schema: JsObject) : JsObject = {
    def modifySchemaElement(schema: JsObject, key: String, newVal: JsValue) = schema - key + (key -> newVal)
    def modifySchema(schema: JsValue) : JsValue = {
      def parseByType(schema: JsObject) : JsValue = {
        val schemaType = schema \ "type"
        schemaType match {
          // union definition
          case JsArray(types) => modifySchemaElement(schema, "type", JsArray(types.map(element => modifySchema(element))))
          // built-in type
          case JsString(str) => {
            str match {
              case "record" => JsString(getNameWithNamespace(schema))
              case "array" => modifySchemaElement(schema, "items", modifySchema(schema \ "items"))
              case "map" => modifySchemaElement(schema, "values", modifySchema(schema \ "values"))
              case "enum" => JsString(getNameWithNamespace(schema))
              case "fixed" => JsString(getNameWithNamespace(schema))
              case Primitive(_) => schema
              case _ => schema // class reference
            }
          }
          // type definition
          case JsObject(_) => modifySchemaElement(schema, "type", modifySchema(schemaType))
          case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
        }
      }

      schema match {
        case JsArray(elements) => JsArray(elements.map(modifySchema))
        case JsObject(_) => parseByType(schema.as[JsObject])
        case JsString(str) => schema
        case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
      }
    }

    val schemaType = (schema \ "type").as[String]
    schemaType match {
      case "record" => modifySchemaElement(schema, "fields", modifySchema(schema \ "fields"))
      case "enum" => schema
      case "fixed" => schema
      case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
    }
  }

  def namespaceSchema(schema: JsObject) : JsObject = {
    def modifySchema(schema: JsValue, namespace:String) : JsValue = {
      def modifySchemaElement(schema: JsValue, key: String, newVal: JsValue) = schema.as[JsObject] - key + (key -> newVal)
      def addNamespace(schema: JsValue) = schema.as[JsObject] + ("namespace" -> JsString(namespace))
      def parseByType(schema: JsValue) : JsValue = {
        val schemaType = schema \ "type"
        schemaType match {
          // union definition
          case JsArray(types) => modifySchemaElement(schema, "type", JsArray(types.map(element => modifySchema(element, namespace))))
          // built-in type
          case JsString(str) => parseStringType(str)
          // type definition
          case JsObject(_) => modifySchemaElement(schema, "type", modifySchema(schemaType, namespace))
          case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
        }
      }

      def parseStringType(schemaType: String) : JsValue = {
        schemaType match {
          case "record" => addNamespace(modifySchemaElement(schema, "fields", modifySchema(schema \ "fields", namespace)))
          case "array" => modifySchemaElement(schema, "items", modifySchema(schema \ "items", namespace))
          case "map" => modifySchemaElement(schema, "values", modifySchema(schema \ "values", namespace))
          case "enum" => addNamespace(schema)
          case "fixed" => addNamespace(schema)
          case Primitive(_) => schema
          case _ => modifySchemaElement(schema, "type", JsString(getNameWithNamespace(namespace, schemaType))) // class reference
        }
      }

      def parseString(str: String) : JsValue = {
        str match {
          case Primitive(_) => schema
          case _ => JsString(getNameWithNamespace(namespace, str)) // class reference
        }
      }

      schema match {
        case JsArray(elements) => JsArray(elements.map(element => modifySchema(element, namespace)))
        case JsObject(_) => parseByType(schema)
        case JsString(str) => parseString(str)
        case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
      }
    }

    modifySchema(schema, (schema \ "namespace").as[String]).as[JsObject]
  }

  def getNestedSchemas(schema: JsValue) = traverseSchema(schema, InternallyDefinedSchemasExtractor).asInstanceOf[JsObjectArray]
  def getDependencies(schema: JsValue) = {
    val x = traverseSchema(schema, DependenciesExtractor)
    AvroSchemasUtils.extractValue(x)
  }
}
