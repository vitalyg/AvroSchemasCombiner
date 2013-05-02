package avro.combiner

import play.api.libs.json._
import scala.annotation.tailrec
import AvroSchemasUtils._

/**
 * User: vgordon
 * Date: 1/11/13
 * Time: 1:51 PM
 */

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

trait ModifyAvroTraversal extends AvroTraversal {
  def modifySchemaElement(schema: JsValue, key: String, newVal: JsValue) = schema.as[JsObject] - key + (key -> newVal)
  override def traverseObjectType(schema: JsValue, objectResults: JsValue): JsValue = modifySchemaElement(schema, "type", objectResults)
  override def traverseArray(schema: JsValue, arrayResults: JsValue) : JsValue = modifySchemaElement(schema, "type", arrayResults)
  override def traverseArrayType(schema: JsValue, arrayResults: JsValue) : JsValue = modifySchemaElement(schema, "items", arrayResults)
  override def traverseMapType(schema: JsValue, mapResults: JsValue) : JsValue = modifySchemaElement(schema, "values", mapResults)
  override def traversePrimitiveType(schema: JsValue): JsValue = schema
  override def traverseReferenceType(schema: JsValue): JsValue = schema
}

object InternallyDefinedSchemasExtractor extends AvroTraversal {

  override def traverseRecordType(schema: JsValue, recordResults: JsValue) = {
    def valueToSeq(value: JsValue) = {
      value match {
        case JsArray(x) => x
        case _ => Seq(value)
      }
    }
    JsArray(Seq(schema) ++ recordResults.as[JsArray].value.flatMap(valueToSeq))
  }
  override def traverseEnumType(schema: JsValue) = JsArray(Seq(schema.as[JsObject]))
  override def traverseFixedType(schema: JsValue) = JsArray(Seq(schema.as[JsObject]))
}

object DependenciesExtractor extends AvroTraversal {
  override def traverseReferenceType(schema: JsValue) = schema
  override def traverseEnumType(schema: JsValue) = JsArray(Nil)
  override def traverseFixedType(schema: JsValue) = JsArray(Nil)
}

//object FlattenStrategy extends ModifyAvroTraversal {
//  override def traverseRecordType(schema: JsValue, recordResults: JsValue): JsValue = JsString(getNameWithNamespace(schema.as[JsObject]))
//  override def traverseEnumType(schema: JsValue): JsValue = JsString(getNameWithNamespace(schema.as[JsObject]))
//  override def traverseFixedType(schema: JsValue): JsValue = JsString(getNameWithNamespace(schema.as[JsObject]))
//}

object FlattenStrategy extends ModifyAvroTraversal {
  override def traverseRecordType(schema: JsValue, recordResults: JsValue): JsValue = schema \ "name"
  override def traverseEnumType(schema: JsValue): JsValue = schema \ "name"
  override def traverseFixedType(schema: JsValue): JsValue = schema \ "name"
}

case class NamespaceStrategy(namespace: String) extends ModifyAvroTraversal {
//  def addNamespace(schema: JsValue) = schema.as[JsObject] + ("namespace" -> JsString(namespace))
  def addNamespace(schema: JsValue) = {
    schema match {
      case s: JsObject => modifySchemaElement(s, "name", JsString(getNameWithNamespace(namespace, (s \ "name").as[String])))
      case JsString(s) => JsString(getNameWithNamespace(namespace, s))
      case _ => throw new Exception("Bad Type\n" + schema)
    }
  }
  override def traverseRecordType(schema: JsValue, recordResults: JsValue): JsValue = addNamespace(modifySchemaElement(schema, "fields", recordResults))
  override def traverseEnumType(schema: JsValue): JsValue = addNamespace(schema)
  override def traverseFixedType(schema: JsValue): JsValue = addNamespace(schema)
  override def traverseReferenceType(schema: JsValue): JsValue = addNamespace(schema)
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
      val els = elements
              .map(mapping)
              .filter(e => !e.equals(JsObject(Nil)) && !e.equals(JsArray(Nil)))
              .flatMap{ case JsArray(e) => e; case e => Seq(e) }
      JsArray(els)
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
      case JsString("enum") => schema
      case JsString("fixed") => schema
      case _ => traverseSchema(schema, FlattenStrategy)
    }
    flatSchema.as[JsObject]
  }

  def namespaceSchema(schema: JsObject) : JsValue = traverseSchema(schema, NamespaceStrategy((schema \ "namespace").as[String]))
  def getNestedSchemas(schema: JsValue) = traverseSchema(schema, InternallyDefinedSchemasExtractor).as[JsArray].value
  def getDependencies(schema: JsValue) = traverseSchema(schema, DependenciesExtractor)
  def getDependencyGraph(schemas: Map[String, JsObject]) : Map[String, Set[String]] = {
    val x = schemas.mapValues(schema => getDependencies(schema)).map(identity)
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
