package avro.combiner

import org.specs2.mutable._
import play.api.libs.json._
import AvroSchemasUtils._

/**
 * User: vgordon
 * Date: 4/23/13
 * Time: 4:11 PM
 */
class AvroSchemasUtilsTest extends SpecificationWithJUnit {
  val schema = Json.parse("{\n   \"namespace\" : \"example.avro\",\n   \"type\" : \"record\",\n   \"name\" : \"User\",\n   \"fields\" :\n      [\n         {\n            \"name\" : \"name\",\n            \"type\" : \"string\"\n         },\n         {\n            \"name\" : \"favorite_number\",\n            \"type\" :\n               [\n                  \"int\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"favorite_color\",\n            \"type\" :\n               [\n                  \"string\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"address\",\n            \"type\" :\n               {\n                  \"type\" : \"map\",\n                  \"values\" : \"Address\"\n               }\n         },\n         {\n            \"name\" : \"title\",\n            \"type\" :\n               {\n                  \"type\" : \"enum\",\n                  \"name\" : \"title\",\n                  \"symbols\" :\n                     [\n                        \"Mr\",\n                        \"Miss\",\n                        \"Mrs\",\n                        \"Ms\"\n                     ]\n               }\n         },\n         {\n            \"name\" : \"phones\",\n            \"type\" :\n               {\n                  \"type\" : \"array\",\n                  \"items\" : \"example.phones.Phone\"\n               }\n         },\n         {\n            \"name\" : \"hash\",\n            \"type\" :\n               {\n                  \"type\" : \"fixed\",\n                  \"size\" : 16,\n                  \"name\" : \"md5\"\n               }\n         }\n      ]\n}").as[JsObject]

  "Concatenating namespaces should" should {
    "combine namespace and name with a dot" in {
      getNameWithNamespace("avro", "combiner") mustEqual "avro.combiner"
    }
    "ignore namespace if name has a dot" in {
      getNameWithNamespace("avro", "avro.combiner") mustEqual "avro.combiner"
    }
  }

  def stringMatch(value: JsValue, fieldName: String) = {
    value match {
      case JsString(x) => x.equals(fieldName)
      case _ => false
    }
  }
  val namespacedSchema = namespaceSchema(schema)
  "namespaceSchema" should {
    def getRecordField(record: JsObject, fieldName: String) = {
      (record \ "fields")
              .as[JsArray]
              .value
              .filter(f => stringMatch(f \ "name", fieldName))
              .head
    }

    "add namespaces field to the title enum" in {
      getRecordField(namespacedSchema, "title") \ "namespace" mustNotEqual null
    }
    "add namespaces field to the hash fixed" in {
      getRecordField(namespacedSchema, "hash") \ "namespace" mustNotEqual null
    }
  }

  "traverseSchemas with InternallyDefinedSchemasExtractor" should {
    "check that simple object returns empty JsObject" in {
      val simpleObject = JsObject(Seq("name" -> JsString("name"), "type" -> JsString("string")))
      traverseSchema(simpleObject, InternallyDefinedSchemasExtractor) mustEqual JsObject(Nil)
    }
    val simpleArray = JsArray(Seq(JsString("int"), JsString("null")))
    "check that simple array return empty JsArray" in {
      traverseSchema(simpleArray, InternallyDefinedSchemasExtractor) mustEqual JsArray(Nil)
    }
    "check that compound array still returns empty JsObjectArray" in {
      val compoundArray = JsArray(Seq(JsObject(Seq("name" -> JsString("name"), "type" -> JsString("string"))),
                                      JsObject(Seq("name" -> JsString("number"), "type" -> simpleArray))))
      traverseSchema(compoundArray, InternallyDefinedSchemasExtractor) mustEqual JsArray(Nil)
    }
    "check that map returns empty JsObject" in {
      val avroMap = JsObject(Seq("name" -> JsString("address"),
                                 "type" -> JsObject(Seq("type" -> JsString("Map"),
                                                        "values" -> JsString("example.Avro.Address")))))
      traverseSchema(avroMap, InternallyDefinedSchemasExtractor) mustEqual JsObject(Nil)
    }
    "check that enum does not return a JsArray" in {
      val enum = JsObject(Seq("name" -> JsString("title"),
                              "type" -> JsObject(Seq("type" -> JsString("enum"),
                                                     "name" -> JsString("title"),
                                                     "symbols" -> JsArray(Seq(JsString("a"), JsString("b")))))))
      traverseSchema(enum, InternallyDefinedSchemasExtractor) \ "type" mustEqual JsString("enum")
    }
  }

  "getNestedSchemas" should {
    def hasElement(elements: Seq[JsValue], name: String) = {
      elements.filter(el => stringMatch(el \ "name", name)).size > 0
    }
    "check that title enum is one of the nested schemas" in {
      val nestedSchemas = getNestedSchemas(namespacedSchema)
      hasElement(nestedSchemas, "title") mustEqual true
    }
    "check that hash fixed is one of the nested schemas" in {
      val nestedSchemas = getNestedSchemas(namespacedSchema)
      hasElement(nestedSchemas, "md5") mustEqual true
    }
  }

  "flattenSchemas" should {
    def elementIsString(record: JsValue, fieldName: String) = {
      val el = (record \ "fields")
              .as[JsArray]
              .value
              .filter(f => stringMatch(f \ "name", fieldName))
              .head
      (el \ "type").isInstanceOf[JsString]
    }
    val flatSchemas = flattenSchema(namespacedSchema)
    "check that title enum is flat" in {
      val enum = JsObject(Seq("name" -> JsString("title"),
                              "type" -> JsObject(Seq("type" -> JsString("enum"),
                                                     "name" -> JsString("title"),
                                                     "namespace" -> JsString("example.avro"),
                                                     "symbols" -> JsArray(Seq(JsString("a"), JsString("b")))))))
      flattenSchema(enum) \ "type" mustEqual JsString("example.avro.title")
    }
    "check that all fields are objects" in {
      (flatSchemas \ "fields").as[JsArray].value.filter(!_.isInstanceOf[JsObject]).size mustEqual 0
    }
    "check that title enum is flat" in {
      elementIsString(flatSchemas, "title") mustEqual true
    }
    "check that hash fixed is flat" in {
      elementIsString(flatSchemas, "hash") mustEqual true
    }
  }


  "getDependencyGraph" should {
    val dependencies = getDependencyGraph(Map("User" -> flattenSchema(namespacedSchema)))
    val simpleArray = JsArray(Seq(JsString("int"), JsString("null")))
    "check that simple array return empty JsObjectArray" in {
      traverseSchema(simpleArray, DependenciesExtractor) mustEqual JsArray(Nil)
    }
    "check that compound array still returns empty JsObjectArray" in {
      val compoundArray = JsArray(Seq(JsObject(Seq("name" -> JsString("name"), "type" -> JsString("string"))),
                                      JsObject(Seq("name" -> JsString("number"), "type" -> simpleArray))))
      traverseSchema(compoundArray, DependenciesExtractor) mustEqual JsArray(Nil)
    }
    def isDependency(depends: Map[String, Set[String]], name: String) = depends("User")(name)
    "check that title is a dependency" in {
      isDependency(dependencies, "example.avro.title") mustEqual true
    }
    "check that md5 is a dependency" in {
      isDependency(dependencies, "example.avro.md5") mustEqual true
    }
  }

  "topologicalSort" should {
    val order = topologicalSort(Seq("A" -> Set[String](), "B" -> Set("A")).toMap)
    "A should appear before B" in {
      order.head mustEqual "A"
      order.tail.head mustEqual "B"
    }
  }
}
