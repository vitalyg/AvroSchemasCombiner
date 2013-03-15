package avro.combiner

import java.io.{FileWriter, BufferedWriter, File, FileFilter}
import io.Source
import play.api.libs.json._
import annotation.tailrec


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

  def getDependencyGraph(schemas: Map[String, JsObject]) : Map[String, Set[String]] = {
    def getSchemaDependencies(schema: JsValue) : Set[String] = AvroSchemasUtils.getDependencies(schema).map(_.as[String]).toSet

    schemas.mapValues(schema => getSchemaDependencies(schema))
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

    val schemas = getAvroSchemas(inputDir)
    val namespacedSchemas = schemas.values.map(x => AvroSchemasUtils.namespaceSchema(x))
    val nestedSchemas = namespacedSchemas.flatMap(x => AvroSchemasUtils.getNestedSchemas(x))
    val flatSchemas = nestedSchemas.map(x => (AvroSchemasUtils.getNameWithNamespace(x.as[JsObject]), AvroSchemasUtils.flattenSchema(x.as[JsObject]))).toMap
    val order = topologicalSort(getDependencyGraph(flatSchemas))

    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(Json.stringify(getSchemasInOrder(flatSchemas, order)))
    bw.close()
  }
}
