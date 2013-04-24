package avro.combiner

import org.specs2.mutable._
import play.api.libs.json._


/**
 * User: vgordon
 * Date: 4/23/13
 * Time: 4:11 PM
 */
class AvroSchemasUtilsTest extends Specification {
  val schema = Json.parse("{\n   \"namespace\" : \"example.avro\",\n   \"type\" : \"record\",\n   \"name\" : \"User\",\n   \"fields\" :\n      [\n         {\n            \"name\" : \"name\",\n            \"type\" : \"string\"\n         },\n         {\n            \"name\" : \"favorite_number\",\n            \"type\" :\n               [\n                  \"int\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"favorite_color\",\n            \"type\" :\n               [\n                  \"string\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"address\",\n            \"type\" :\n               {\n                  \"type\" : \"map\",\n                  \"values\" : \"Address\"\n               }\n         },\n         {\n            \"name\" : \"title\",\n            \"type\" :\n               {\n                  \"type\" : \"enum\",\n                  \"name\" : \"title\",\n                  \"symbols\" :\n                     [\n                        \"Mr\",\n                        \"Miss\",\n                        \"Mrs\",\n                        \"Ms\"\n                     ]\n               }\n         },\n         {\n            \"name\" : \"phones\",\n            \"type\" :\n               {\n                  \"type\" : \"array\",\n                  \"items\" : \"example.phones.Phone\"\n               }\n         },\n         {\n            \"name\" : \"hash\",\n            \"type\" :\n               {\n                  \"type\" : \"fixed\",\n                  \"size\" : 16,\n                  \"name\" : \"md5\"\n               }\n         }\n      ]\n}").as[JsObject]

  "Concatenating namespaces should" should {
    "combine namespace and name with a dot" in {
      AvroSchemasUtils.getNameWithNamespace("avro", "combiner") mustEqual("avro.combiner")
    }
    "ignore namespace if name has a dot" in {
      AvroSchemasUtils.getNameWithNamespace("avro", "avro.combiner") mustEqual("avro.combiner")
    }
  }

  def getRecordField(record: JsObject, fieldName: String) = {
    def stringMatch(value: JsValue) = {
      value match {
        case JsString(x) => x.equals(fieldName)
        case _ => false
      }
    }
    (record \ "fields")
            .as[JsArray]
            .value
            .filter(f => stringMatch(f \ "name"))
            .head
  }

  "namespaceSchema" should {
    val namespacedSchema = AvroSchemasUtils.namespaceSchema(schema)
    "add namespaces field to the title enum" in {
      getRecordField(namespacedSchema, "title") \ "namespace" mustNotEqual null
    }
    "add namespaces field to the hash fixed" in {
      getRecordField(namespacedSchema, "hash") \ "namespace" mustNotEqual null
    }
  }

  "getNestedSchemas" should {
    val nestedSchemas = AvroSchemasUtils.getNestedSchemas(schema)
    "check that title enum is one of the nested schemas" in {
      nestedSchemas mustEqual null
    }
  }
}
