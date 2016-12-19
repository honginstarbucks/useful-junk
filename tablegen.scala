import java.io.PrintWriter
import java.time.OffsetDateTime
import java.util.UUID

import sbux.ucp.points.accrual.Database.PointRecord

import scala.reflect.runtime.universe._

object tableGenApp extends App {

  case class GenArtifact(caseClassType: scala.reflect.runtime.universe.Type,
                         databaseName: String) {
    val className = caseClassType.baseClasses.head.name.toString
    val recordName = if (className.endsWith("$")) className.dropRight(1) else className
    val tableName = recordName + "Table"
    val concreteTableName = "Concrete" + recordName + "Table"
  }

  // example: specify the type of the case class (the Scala representation of the tabel model) 
  // and then specify the name of the database
  // val artifact = GenArtifact(typeOf[PointRecord], "PointDatabase")
  val artifact = ???

  genPhantomCode(artifact)

  def genPhantomCode(artifact: GenArtifact) = {
    implicit val builder: StringBuilder = new StringBuilder()

    // package import
    builder.append("import java.util.UUID\n")
    builder.append("import com.outworkers.phantom.connectors.RootConnector\nimport com.outworkers.phantom.dsl._\nimport com.outworkers.phantom.jdk8.dsl._\n")
    builder.append("import scala.concurrent.Future\n")

    // disclaimer
    builder.append("// the following code is generated through [placeholder]\n\n")

    genDatabase(artifact.databaseName)
    genTable(artifact.caseClassType, artifact.className)
    genConcreteTable(artifact.caseClassType)
    writeGenCode()
  }

  def writeGenCode()(implicit builder: StringBuilder) =
    new PrintWriter("table.scala") {
      write(builder.toString())
      close
    }

  def genDatabase(dbName: String)(implicit sb: StringBuilder) = {
    sb.append(s"class $dbName(override val connector: KeySpaceDef) extends Database[$dbName](connector) {\n")
    sb.append(s"  object ${artifact.tableName} extends ${artifact.concreteTableName} with connector.Connector\n")
    sb.append(s"}\n\n")

    // generate a database provider, can be used if needed
    sb.append(s"trait $dbName" + "Provider {\n")
    sb.append(s"  def database: $dbName\n")
    sb.append(s"}\n\n")
  }

  def genConcreteTable(t: Type)(implicit sb: StringBuilder) = {
    sb.append(s"abstract class ${artifact.concreteTableName} extends ${artifact.tableName} with RootConnector {\n\n")

    // add
    sb.append(s"  def create(record: ${artifact.recordName}): Future[ResultSet] = \n")
    sb.append(s"    super.insert\n")
    t.members.filter(!_.isMethod)
      .foreach(sym => sb.append(s"      .value(_.${sym.name.toString.trim}, record.${sym.name.toString.trim})\n"))
    sb.append("      .future()\n\n")

    // get one
    sb.append(s"  def read(id: UUID): Future[Option[${artifact.recordName}]] = \n")
    sb.append(s"    super.select\n")
    sb.append(s"      .where(_.id eqs id)\n")
    sb.append(s"      .one()\n\n")

    // update
    sb.append(s"  def update(record: ${artifact.recordName}): Future[ResultSet] = \n")
    sb.append(s"    super.update\n")
    sb.append(s"      .where(_.id eqs record.id)\n")
    sb.append(s"      .modify(_.")
    val members = t.members.filter(a => !a.isMethod && !a.name.toString.trim.equalsIgnoreCase("id"))
    forEachIsLast(members.iterator) { (line, isLast) =>
      if (isLast)
        sb.append(line.name.toString.trim + " setTo record." + line.name.toString.trim + ")\n")
      else {
        sb.append(line.name.toString.trim + " setTo record." + line.name.toString.trim + ")\n")
        sb.append("      .and(_.")
      }
    }
    sb.append("      .future()\n\n")

    // delete
    sb.append(s"  def delete(id: UUID): Future[ResultSet] = \n")
    sb.append(s"    super.delete\n")
    sb.append(s"    .where(_.id eqs id)\n")
    sb.append(s"    .future()\n\n")

    // exists
    sb.append(s"  def exists(id: UUID): Future[ResultSet] = \n")
    sb.append(s"    super.select\n")
    sb.append(s"      .where(_.id eqs id)\n")
    sb.append(s"      .future()\n\n")

    // getall
    sb.append(s"  def fetchAll(): Future[ResultSet] = \n")
    sb.append(s"    super.select\n")
    sb.append(s"    .all()\n")
    sb.append(s"    .future()\n\n")

    sb.append("}\n")

  }

  def genTable(t: Type, name: String)(implicit sb: StringBuilder) = {
    sb.append(s"sealed abstract class ${artifact.tableName}")
    sb.append(s" extends CassandraTable[${artifact.tableName}, ${artifact.recordName}] {\n")

    genFields(t, sb)
    genFieldRows(t, artifact.recordName, sb)
    sb.append("}\n\n")
  }

  def genFieldRows(t: _root_.scala.reflect.runtime.universe.Type, recordName: String, sb: StringBuilder): StringBuilder = {
    sb.append("  def fromRow(row: Row): " + recordName + " = {\n")
    sb.append(s"    $recordName(\n")
    sb.append(genColumnRows(t))
    sb.append(")\n")
    sb.append("  }\n")
  }

  def genFields(t: Type, sb: StringBuilder): Unit = {
    t.members.filter(!_.isMethod).foreach(sym => genColumn(sym, sb))
  }

  def genColumn(sym: Symbol, sb: StringBuilder): Unit = {
    sb.append(s"  object ${sym.name.toString.trim} extends ")
    sb.append(getColumnType(sym))
    sb.append("(this)")
    sb.append(getKeyIfNeeded(sym))
    sb.append("\n")
  }

  def genColumnRows(t: Type): String = {
    val sb = new StringBuilder()
    t.members.filter(!_.isMethod).foreach(sym => genColumnRow(sym, sb))
    val result = sb.toString.dropRight(2) // the last one should remove \n and ","
    result
  }

  def genColumnRow(sym: Symbol, sb: StringBuilder): Unit = {
    sb.append(s"    ${sym.name.toString.trim} = ${sym.name.toString.trim}(row),\n")
  }

  def getKeyIfNeeded(sym: Symbol): String = {

    if (sym.name.toString.trim.toLowerCase.equals("id")) {
      " with PrimaryKey" // hard-coded
    }
    else {
      ""
    }
  }

  def getColumnType(sym: Symbol): String = {
    sym.typeSignature match {
      case tpe if tpe =:= typeOf[String] => "StringColumn"
      case tpe if tpe =:= typeOf[Int] => "IntColumn"
      case tpe if tpe =:= typeOf[UUID] => "UUIDColumn"
      case tpe if tpe =:= typeOf[Long] => "LongColumn"
      case tpe if tpe =:= typeOf[OffsetDateTime] => "OffsetDateTimeColumn"
      case _ => "UnknownColumn_Update_Template"
    }
  }

  def forEachIsLast[A](iterator: Iterator[A])(operation: (A, Boolean) => Unit): Unit = {
    while (iterator.hasNext) {
      val element = iterator.next()
      val isLast = !iterator.hasNext // if there is no "next", this is the last one
      operation(element, isLast)
    }
  }
}
