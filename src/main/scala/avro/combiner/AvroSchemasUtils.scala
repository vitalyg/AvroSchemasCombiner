package avro.combiner

import play.api.libs.json._

/**
 * User: vgordon
 * Date: 1/11/13
 * Time: 1:51 PM
 */
trait AvroTraversal {
  def recordInitialValue(schema: JsValue): Option[JsValue] = None
  def traverseEnumType(schema: JsValue): Seq[JsValue] = Seq()
  def traverseFixedType(schema: JsValue): Seq[JsValue] = Seq()
  def traversePrimitiveType(schema: JsValue): Seq[JsValue] = Seq()
  def traverseReferenceType(schema: JsValue): Seq[JsValue] = Seq()
}

object InternallyDefinedSchemasExtractor extends AvroTraversal {
  override def recordInitialValue(schema: JsValue) = Some(schema)
  override def traverseEnumType(schema: JsValue) = Seq(schema)
  override def traverseFixedType(schema: JsValue) = Seq(schema)
}

object DependenciesExtractor extends AvroTraversal {
  override def traverseReferenceType(schema: JsValue) = Seq(schema)
}

object AvroSchemasUtils {
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

  def traverseSchema(schema: JsValue, strategy: AvroTraversal) : Seq[JsValue] = {
    def parseByType(schema: JsValue) : Seq[JsValue] = {
      val schemaType = (schema \ "type")
      schemaType match {
        // union definition
        case JsArray(types) => types.flatMap(element => traverseSchema(element, strategy))
        // built-in type
        case JsString(str) => parseString(str)
        // type definition
        case JsObject(_) => traverseSchema(schemaType, strategy)
        case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
      }
    }

    def parseString(schemaType: String) : Seq[JsValue] = {
      schemaType match {
        case "record" => {
          val recordFields = (schema \ "fields").as[JsArray].value
          strategy.recordInitialValue(schema) match {
            case Some(value) => value +: recordFields.flatMap(field => traverseSchema(field, strategy))
            case None => recordFields.flatMap(field => traverseSchema(field, strategy))
          }
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
      case JsArray(elements) => elements.flatMap(el => traverseSchema(el, strategy))
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

  def getNestedSchemas(schema: JsValue) = traverseSchema(schema, InternallyDefinedSchemasExtractor)
  def getDependencies(schema: JsValue) = traverseSchema(schema, DependenciesExtractor).diff(Seq(JsString(getNameWithNamespace(schema.as[JsObject]))))
}
