package avro.combiner

import java.io.{FileWriter, BufferedWriter, File, FileFilter}
import io.Source
import play.api.libs.json._
import annotation.tailrec
import AvroSchemasUtils._


/**
 * User: vgordon
 * Date: 1/8/13
 * Time: 10:07 AM
 */
object AvroSchemasCombiner {
  val avroSchemaExtension = "avsc"

  def getSchemaName(schema: JsObject) : String = (schema \ "name").as[String]

  def getSchema(file: File) : (String, JsObject) = {
    try {
      val schema = Json.parse(Source.fromFile(file).mkString).as[JsObject]
      (getSchemaName(schema), schema)
    }
    catch {
      case e: Exception => throw new Exception("Bad Avro Schema\n" + file)
    }
  }

  def getAvroSchemas(file: File) : Map[String, JsObject] = {
    val avroFilesFilter = new FileFilter {
      def accept(file: File) = file.isDirectory || file.getName.endsWith(avroSchemaExtension)
    }

    val avroFilesAndDirs = file.listFiles(avroFilesFilter)
    val (dirs, files) = avroFilesAndDirs.partition(_.isDirectory)
    dirs.map(getAvroSchemas).flatten.toMap ++ files.map(getSchema).toMap
  }

  def getSchemasInOrder(schemas: Map[String, JsObject], order: Seq[String]) : JsArray = {
    Json.toJson(order.map(schemaName => schemas(schemaName))).as[JsArray]
  }


  def main(args: Array[String]) {
    val avroDirectory = args(0)
    val outputPath = args(1)

    val inputDir = new File(avroDirectory)
    val outputFile = new File(outputPath)

    if (!outputFile.exists)
      outputFile.createNewFile

    val schemas = getAvroSchemas(inputDir).filter(_._1 == "ProspectDocuments")
    val namespacedSchemas = schemas.values.map(namespaceSchema)
    val nestedSchemas = namespacedSchemas.flatMap(getNestedSchemas)
    val flatSchemas = nestedSchemas.map(x => ((x \ "name").as[String], flattenSchema(x))).toMap
    val order = topologicalSort(getDependencyGraph(flatSchemas))

    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(Json.stringify(getSchemasInOrder(flatSchemas, order)))
    bw.close()
  }
}
