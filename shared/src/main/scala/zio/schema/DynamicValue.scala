package zio.schema

import zio.Chunk
import zio.schema.DynamicValue._

sealed trait DynamicValue { self =>

  def validate(schema: Schema[_]): scala.Either[Chunk[String], Unit] =
    validateValue(self, schema)

  private def validateValue(dv: DynamicValue, s: Schema[_]): scala.Either[Chunk[String], Unit] =
    (dv, s) match {
      case (DynamicValue.Primitive(p, t), Schema.Primitive(_, _)) =>
        scala.Right(())
      case (DynamicValue.Record(_, values), schema: Schema.Record[_]) =>
        validateRecord(values, schema.fields)
      case (DynamicValue.Sequence(values), schema: Schema.Sequence[_, _, _]) =>
        val errors = values.flatMap(v => validateValue(v, schema.elementSchema) match {
          case scala.Left(es) => es
          case scala.Right(_) => Chunk.empty
        })
        if (errors.isEmpty) scala.Right(()) else scala.Left(errors)
      case _ => scala.Left(Chunk(s"Value $dv does not match schema $s"))
    }

  private def validateRecord(values: Map[String, DynamicValue], fields: Chunk[Schema.Field[_, _]]): scala.Either[Chunk[String], Unit] = {
    val errors = fields.flatMap { field =>
      values.get(field.name) match {
        case Some(v) => validateValue(v, field.schema) match {
          case scala.Left(es) => es
          case scala.Right(_) => Chunk.empty
        }
        case None => Chunk(s"Field ${field.name} is missing")
      }
    }
    if (errors.isEmpty) scala.Right(()) else scala.Left(errors)
  }
}

