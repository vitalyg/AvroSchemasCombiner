package avro.combiner

import org.specs2.mutable._
import play.api.libs.json._
import play.api.libs.json.JsArray
import play.api.libs.json.JsObject
import play.api.libs.json.JsString


/**
 * User: vgordon
 * Date: 4/23/13
 * Time: 4:11 PM
 */
class AvroSchemasUtilsTest extends SpecificationWithJUnit {
  val schema = Json.parse("{\n   \"namespace\" : \"example.avro\",\n   \"type\" : \"record\",\n   \"name\" : \"User\",\n   \"fields\" :\n      [\n         {\n            \"name\" : \"name\",\n            \"type\" : \"string\"\n         },\n         {\n            \"name\" : \"favorite_number\",\n            \"type\" :\n               [\n                  \"int\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"favorite_color\",\n            \"type\" :\n               [\n                  \"string\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"address\",\n            \"type\" :\n               {\n                  \"type\" : \"map\",\n                  \"values\" : \"Address\"\n               }\n         },\n         {\n            \"name\" : \"title\",\n            \"type\" :\n               {\n                  \"type\" : \"enum\",\n                  \"name\" : \"title\",\n                  \"symbols\" :\n                     [\n                        \"Mr\",\n                        \"Miss\",\n                        \"Mrs\",\n                        \"Ms\"\n                     ]\n               }\n         },\n         {\n            \"name\" : \"phones\",\n            \"type\" :\n               {\n                  \"type\" : \"array\",\n                  \"items\" : \"example.phones.Phone\"\n               }\n         },\n         {\n            \"name\" : \"hash\",\n            \"type\" :\n               {\n                  \"type\" : \"fixed\",\n                  \"size\" : 16,\n                  \"name\" : \"md5\"\n               }\n         }\n      ]\n}").as[JsObject]

  "Concatenating namespaces should" should {
    "combine namespace and name with a dot" in {
      AvroSchemasUtils.getNameWithNamespace("avro", "combiner") mustEqual("avro.combiner")
    }
    "ignore namespace if name has a dot" in {
      AvroSchemasUtils.getNameWithNamespace("avro", "avro.combiner") mustEqual("avro.combiner")
    }
  }

  def stringMatch(value: JsValue, fieldName: String) = {
    value match {
      case JsString(x) => x.equals(fieldName)
      case _ => false
    }
  }
  val namespacedSchema = AvroSchemasUtils.namespaceSchema(schema)
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

  "getNestedSchemas" should {
    def hasElement(elements: JsObjectArray, name: String) = {
      elements.array.filter(el => stringMatch(el \ "name", name)).size > 0
    }
    val nestedSchemas = AvroSchemasUtils.getNestedSchemas(namespacedSchema)
    "check that title enum is one of the nested schemas" in {
      hasElement(nestedSchemas, "title") mustEqual true
    }
    "check that hash fixed is one of the nested schemas" in {
      hasElement(nestedSchemas, "md5") mustEqual true
    }
  }

  val flatSchemas = AvroSchemasUtils.flattenSchema(namespacedSchema)
  "flattenSchemas" should {
    def elementIsString(record: JsObject, fieldName: String) = {
      val el = (record \ "fields")
              .as[JsArray]
              .value
              .filter(f => stringMatch(f \ "name", fieldName))
              .head
//      el
      (el \ "type").isInstanceOf[JsString]
    }
    val flatSchemas = AvroSchemasUtils.flattenSchema(namespacedSchema)
    "check that title enum is flat" in {
      elementIsString(flatSchemas, "title") mustEqual true
    }
    "check that hash fixed is flat" in {
      elementIsString(flatSchemas, "hash") mustEqual true
    }
  }

  val dependencies = AvroSchemasUtils.getDependencyGraph(Map("User" -> flatSchemas))
  "getDependencyGraph" should {
    def isDependency(depends: Map[String, Set[String]], name: String) = depends("User")(name)
    "check that title is a dependency" in {
      isDependency(dependencies, "example.avro.title") mustEqual true
    }
    "check that md5 is a dependency" in {
      isDependency(dependencies, "example.avro.md5") mustEqual true
    }
  }

  "topologicalSort" should {
    val order = AvroSchemasUtils.topologicalSort(Seq("A" -> Set[String](), "B" -> Set("A")).toMap)
    "A should appear before B" in {
      order.head mustEqual "A"
      order.tail.head mustEqual "B"
    }
  }

}
