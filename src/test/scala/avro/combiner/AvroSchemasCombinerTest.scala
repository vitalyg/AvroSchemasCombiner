package avro.combiner


import org.specs2.mutable._
import play.api.libs.json.{JsObject, Json}


/**
 * User: vgordon
 * Date: 4/24/13
 * Time: 9:39 AM
 */
class AvroSchemasCombinerTest extends SpecificationWithJUnit {

  val schema = Json.parse("{\n   \"namespace\" : \"example.avro\",\n   \"type\" : \"record\",\n   \"name\" : \"User\",\n   \"fields\" :\n      [\n         {\n            \"name\" : \"name\",\n            \"type\" : \"string\"\n         },\n         {\n            \"name\" : \"favorite_number\",\n            \"type\" :\n               [\n                  \"int\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"favorite_color\",\n            \"type\" :\n               [\n                  \"string\",\n                  \"null\"\n               ]\n         },\n         {\n            \"name\" : \"address\",\n            \"type\" :\n               {\n                  \"type\" : \"map\",\n                  \"values\" : \"Address\"\n               }\n         },\n         {\n            \"name\" : \"title\",\n            \"type\" :\n               {\n                  \"type\" : \"enum\",\n                  \"name\" : \"title\",\n                  \"symbols\" :\n                     [\n                        \"Mr\",\n                        \"Miss\",\n                        \"Mrs\",\n                        \"Ms\"\n                     ]\n               }\n         },\n         {\n            \"name\" : \"phones\",\n            \"type\" :\n               {\n                  \"type\" : \"array\",\n                  \"items\" : \"example.phones.Phone\"\n               }\n         },\n         {\n            \"name\" : \"hash\",\n            \"type\" :\n               {\n                  \"type\" : \"fixed\",\n                  \"size\" : 16,\n                  \"name\" : \"md5\"\n               }\n         }\n      ]\n}").as[JsObject]

  "getSchemaName should" should {
    "get the right name" in {
      AvroSchemasCombiner.getSchemaName(schema) mustEqual("User")
    }
  }
}
