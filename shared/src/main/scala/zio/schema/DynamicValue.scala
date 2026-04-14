package zio.schema

import zio.Chunk
import scala.collection.immutable.ListMap
import zio.schema.meta.Migration

sealed trait DynamicValue { self =>

  def transform(transforms: Chunk[Migration]): scala.Either[String, DynamicValue] =
    transforms.foldRight[scala.Either[String, DynamicValue]](scala.Right(self)) {
      case (transform, scala.Right(value)) => transform.migrate(value)
      case (_, error @ scala.Left(_))      => error
    }

  def validate(schema: Schema[_]): scala.Either[Chunk[String], Unit] = {
    def validateValue(dv: DynamicValue, s: Schema[_]): scala.Either[Chunk[String], Unit] = {
      (dv, s) match {
        case (DynamicValue.Primitive(_, p), Schema.Primitive(p2, _)) if p == p2 =>
          scala.Right(())
        case (DynamicValue.Record(_, values), s: Schema.Record[_]) =>
          validateRecord(values, s.fields)
        case (DynamicValue.Sequence(values), schema: Schema.Sequence[_, _, _]) =>
          accumulateErrors(values.map(v => validateValue(v, schema.elementSchema)))
        case (DynamicValue.Error(message), _) =>
          scala.Left(Chunk(s"DynamicValue error: $message"))
        case _ =>
          scala.Left(Chunk(s"Type mismatch between DynamicValue and Schema"))
      }
    }

    def validateRecord(values: ListMap[String, DynamicValue], structure: Chunk[Schema.Field[_, _]]): scala.Either[Chunk[String], Unit] = {
      accumulateErrors(structure.map { field =>
        values.get(field.name) match {
          case Some(value) => validateValue(value, field.schema)
          case None        => if (field.optional) scala.Right(()) else scala.Left(Chunk(s"Missing: ${field.name}"))
        }
      })
    }

    def accumulateErrors(validations: Iterable[scala.Either[Chunk[String], Unit]]): scala.Either[Chunk[String], Unit] = {
      val errors = Chunk.fromIterable(validations).flatMap {
        case scala.Left(es) => es
        case scala.Right(_) => Chunk.empty
      }
      if (errors.isEmpty) scala.Right(()) else scala.Left(errors)
    }

    validateValue(self, schema)
  }
}
