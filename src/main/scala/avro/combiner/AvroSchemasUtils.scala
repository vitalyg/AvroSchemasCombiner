package avro.combiner

import play.api.libs.json._
import scala.annotation.tailrec
import AvroSchemasUtils._

/**
 * User: vgordon
 * Date: 1/11/13
 * Time: 1:51 PM
 */

// class that is basically a JsArray represented as JsObject
//case class JsObjectArray(array: Seq[JsValue]) extends JsObject(Seq("elements" -> JsArray(array)))
//case class JsObjectString(string: JsValue) extends JsObject(Seq("key" -> string))

trait AvroTraversal {
  def traverseRecordType(schema: JsValue, recordResults: JsValue): JsValue = recordResults
  def traverseObjectType(schema: JsValue, objectResults: JsValue): JsValue = objectResults
  def traverseArray(schema: JsValue, arrayResults: JsValue) : JsValue = arrayResults
  def traverseArrayType(schema: JsValue, arrayResults: JsValue) : JsValue = arrayResults
  def traverseMapType(schema: JsValue, mapResults: JsValue) : JsValue = mapResults
  def traverseEnumType(schema: JsValue): JsValue = JsObject(Nil)
  def traverseFixedType(schema: JsValue): JsValue = JsObject(Nil)
  def traversePrimitiveType(schema: JsValue): JsValue = JsObject(Nil)
  def traverseReferenceType(schema: JsValue): JsValue = JsObject(Nil)
}

object InternallyDefinedSchemasExtractor extends AvroTraversal {
  override def traverseRecordType(schema: JsValue, recordResults: JsValue) = JsArray(Seq(schema) ++ recordResults.as[JsArray].value)
  override def traverseEnumType(schema: JsValue) = schema.as[JsObject]
  override def traverseFixedType(schema: JsValue) = schema.as[JsObject]
}

object DependenciesExtractor extends AvroTraversal {
  override def traverseReferenceType(schema: JsValue) = schema
}

object FlattenStrategy extends AvroTraversal {
  def modifySchemaElement(schema: JsValue, key: String, newVal: JsValue) = schema.as[JsObject] - key + (key -> newVal)
  override def traverseRecordType(schema: JsValue, recordResults: JsValue): JsValue = JsString(getNameWithNamespace(schema.as[JsObject]))
  override def traverseObjectType(schema: JsValue, objectResults: JsValue): JsValue = modifySchemaElement(schema, "type", objectResults)
  override def traverseArray(schema: JsValue, arrayResults: JsValue) : JsValue = modifySchemaElement(schema, "type", arrayResults)
  override def traverseArrayType(schema: JsValue, arrayResults: JsValue) : JsValue = modifySchemaElement(schema, "items", arrayResults)
  override def traverseMapType(schema: JsValue, mapResults: JsValue) : JsValue = modifySchemaElement(schema, "values", mapResults)
  override def traverseEnumType(schema: JsValue): JsValue = JsString(getNameWithNamespace(schema.as[JsObject]))
  override def traverseFixedType(schema: JsValue): JsValue = JsString(getNameWithNamespace(schema.as[JsObject]))
  override def traversePrimitiveType(schema: JsValue): JsValue = schema
  override def traverseReferenceType(schema: JsValue): JsValue = schema
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

  def traverseSchema(schema: JsValue, strategy: AvroTraversal) : JsValue = {
    def mapAndFilter(elements: Seq[JsValue], mapping: JsValue => JsValue) = {
      JsArray(elements.map(mapping).filter(e => !e.equals(JsObject(Nil)) && !e.equals(JsArray(Nil))))
    }
    def parseByType(schema: JsValue) : JsValue = {
      val schemaType = (schema \ "type")
      schemaType match {
        // union definition
        case JsArray(types) => strategy.traverseArray(schema, mapAndFilter(types, traverseSchema(_, strategy)))
        // built-in type
        case JsString(str) => parseString(str)
        // type definition
        case JsObject(_) => strategy.traverseObjectType(schema, traverseSchema(schemaType, strategy))
        case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
      }
    }

    def parseString(schemaType: String) : JsValue = {
      schemaType match {
        case "record" => strategy.traverseRecordType(schema, traverseSchema(schema \ "fields", strategy))
        case "array" => strategy.traverseArrayType(schema, traverseSchema(schema \ "items", strategy))
        case "map" => strategy.traverseMapType(schema, traverseSchema(schema \ "values", strategy))
        case "enum" => strategy.traverseEnumType(schema)
        case "fixed" => strategy.traverseFixedType(schema)
        case Primitive(_) => strategy.traversePrimitiveType(schema)
        case _ => strategy.traverseReferenceType(JsString(schemaType)) // class reference
      }
    }

    schema match {
      case JsArray(elements) => mapAndFilter(elements, traverseSchema(_, strategy))
      case JsObject(_) => parseByType(schema)
      case JsString(str) => parseString(str)
      case _ => throw new Exception("Unrecognizable JSON element\n" + schema)
    }
  }

  def flattenSchema(schema: JsValue) : JsObject = {
    val flatSchema = schema \ "type" match {
      case JsString("record") => FlattenStrategy.modifySchemaElement(schema, "fields", traverseSchema(schema \ "fields", FlattenStrategy))
      case _ => traverseSchema(schema, FlattenStrategy)
    }
    flatSchema.as[JsObject]
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

  def getNestedSchemas(schema: JsValue) = traverseSchema(schema, InternallyDefinedSchemasExtractor).as[JsArray].value
  def getDependencies(schema: JsValue) = traverseSchema(schema, DependenciesExtractor)
  def getDependencyGraph(schemas: Map[String, JsObject]) : Map[String, Set[String]] = {
    schemas.mapValues(schema => getDependencies(schema).as[JsArray].value.map(_.as[String]).toSet)
  }

  @tailrec
  def topologicalSort(graph: Map[String, Set[String]], sortedGraph: List[String] = List()) : List[String] = {
    if (graph.isEmpty)
      return sortedGraph.distinct.reverse

    val node = graph.find(_._2.isEmpty)
    node match {
      case Some(schema) => {
        val key = schema._1
        val newGraph = (graph - key).mapValues(depends => depends - key)
        topologicalSort(newGraph, key :: sortedGraph)
      }
      case None => throw new Exception("Circular dependency in schema")
    }
  }
}
