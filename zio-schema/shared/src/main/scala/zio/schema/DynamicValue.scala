package zio.schema

import zio.Chunk
import java.math.{ BigDecimal, BigInteger }
import java.time._
import java.util.UUID
import scala.collection.immutable.ListMap
import zio.schema.codec.DecodeError
import zio.schema.meta.{ MetaSchema, Migration }
import zio.{ Cause, Unsafe }
import zio.prelude.Validation

sealed trait DynamicValue {
  self =>

  def transform(transforms: Chunk[Migration]): scala.Either[String, DynamicValue] =
    transforms.foldRight[scala.Either[String, DynamicValue]](scala.Right(self)) {
      case (transform, scala.Right(value)) => transform.migrate(value)
      case (_, error @ scala.Left(_))      => error
    }

  @scala.annotation.targetName("toTypedValueWithSchema")
  def toTypedValue[A](implicit schema: Schema[A]): Validation[String, A] =
    toTypedValueLazyError.mapError(_.message)

  def toTypedValueOption[A](implicit schema: Schema[A]): Option[A] =
    toTypedValueLazyError.toOption

  private def toTypedValueLazyError[A](implicit schema: Schema[A]): Validation[DecodeError, A] =
    (self, schema) match {
      case (DynamicValue.Primitive(value, p), Schema.Primitive(p2, _)) if p == p2 =>
        Validation.succeed(value.asInstanceOf[A])
      case (DynamicValue.Record(_, values), s: Schema.Record[A]) =>
        Validation
          .validateAll(s.fields.map { field =>
            values.get(field.name) match {
              case Some(dv) =>
                dv.toTypedValueLazyError(field.schema)
                  .mapError(e => DecodeError.And(DecodeError.Read(Chunk(field.name), "Field error"), e))
              case None => Validation.fail(DecodeError.Read(Chunk(field.name), s"Missing field ${field.name}"))
            }
          })
          .map(fields => s.construct(Chunk.fromIterable(fields)))

      case (DynamicValue.Enumeration(_, (key, value)), s: Schema.Enum[_]) =>
        s.caseOf(key) match {
          case Some(caseValue) =>
            value.toTypedValueLazyError(caseValue.schema).asInstanceOf[Validation[DecodeError, A]]
          case None => Validation.fail(DecodeError.MissingCase(key, s))
        }

      case (DynamicValue.LeftValue(value), Schema.Either(schema1, _, _)) =>
        value.toTypedValueLazyError(schema1).map(Validation.fail(_))

      case (DynamicValue.RightValue(value), Schema.Either(_, schema2, _)) =>
        value.toTypedValueLazyError(schema2).map(Validation.succeed(_))

      case (DynamicValue.Sequence(values), schema: Schema.Sequence[col, t, _]) =>
        values
          .foldLeft[scala.Either[DecodeError, Chunk[t]]](scala.Right[DecodeError, Chunk[t]](Chunk.empty)) {
            case (err @ scala.Left(_), _) => err
            case (scala.Right(values), value) =>
              value.toTypedValueLazyError(schema.elementSchema).map(values :+ _)
          }
          .map(schema.fromChunk)

      case (DynamicValue.SetValue(values), schema: Schema.Set[t]) =>
        values.foldLeft[scala.Either[DecodeError, Set[t]]](scala.Right[DecodeError, Set[t]](Set.empty)) {
          case (err @ scala.Left(_), _) => err
          case (scala.Right(values), value) =>
            value.toTypedValueLazyError(schema.elementSchema).map(values + _)
        }

      case (DynamicValue.SomeValue(value), Schema.Optional(schema: Schema[_], _)) =>
        value.toTypedValueLazyError(schema).map(Some(_))

      case (DynamicValue.NoneValue, Schema.Optional(_, _)) =>
        scala.Right(None)

      case (value, Schema.Transform(schema, f, _, _, _)) =>
        value
          .toTypedValueLazyError(schema)
          .flatMap(value => f(value).left.map(err => DecodeError.MalformedField(schema, err)))

      case (DynamicValue.Dictionary(entries), schema: Schema.Map[k, v]) =>
        entries.foldLeft[scala.Either[DecodeError, Map[k, v]]](scala.Right[DecodeError, Map[k, v]](Map.empty)) {
          case (err @ scala.Left(_), _) => err
          case (scala.Right(map), entry) => {
            for {
              key   <- entry._1.toTypedValueLazyError(schema.keySchema)
              value <- entry._2.toTypedValueLazyError(schema.valueSchema)
            } yield map ++ Map(key -> value)
          }
        }

      case (_, l @ Schema.Lazy(_)) =>
        toTypedValueLazyError(l.schema)

      case (DynamicValue.Error(message), _) =>
        Validation.fail(DecodeError.ReadError(Cause.empty, message))

      case (DynamicValue.Tuple(dyn, DynamicValue.DynamicAst(ast)), _) =>
        val valueSchema = ast.toSchema.asInstanceOf[Schema[Any]]
        dyn.toTypedValueLazyError(valueSchema).map(a => (a -> valueSchema).asInstanceOf[A])

      case (dyn, Schema.Dynamic(_)) => scala.Right(dyn)

      case _ =>
        Validation.fail(DecodeError.CastError(self, schema))
    }

  def validate(schema: Schema[_]): scala.Either[Chunk[String], Unit] = {
    def validateValue(dv: DynamicValue, s: Schema[_]): scala.Either[Chunk[String], Unit] =
      (dv, s) match {
        case (DynamicValue.Primitive(value, p), Schema.Primitive(p2, _)) if p == p2 =>
          scala.Right(())
        case (DynamicValue.Record(_, values), Schema.GenericRecord(_, structure, _)) =>
          validateRecord(values, structure.toChunk)
        case (DynamicValue.Record(_, values), s: Schema.Record[_]) =>
          validateRecord(values, s.fields)
        case (DynamicValue.Enumeration(_, (key, value)), s: Schema.Enum[_]) =>
          s.caseOf(key) match {
            case Some(caseValue) => validateValue(value, caseValue.schema)
            case None            => scala.Left(Chunk(s"Unknown enumeration case: $key"))
          }
        case (DynamicValue.Sequence(values), schema: Schema.Sequence[_, _, _]) =>
          accumulateErrors(values.map(v => validateValue(v, schema.elementSchema)))
        case (DynamicValue.SetValue(values), schema: Schema.Set[_]) =>
          accumulateErrors(values.map(v => validateValue(v, schema.elementSchema)))
        case (DynamicValue.Dictionary(entries), schema: Schema.Map[_, _]) =>
          accumulateErrors(entries.flatMap {
            case (k, v) =>
              scala.List(validateValue(k, schema.keySchema), validateValue(v, schema.valueSchema))
          })
        case (DynamicValue.SomeValue(value), Schema.Optional(optSchema, _)) =>
          validateValue(value, optSchema)
        case (DynamicValue.NoneValue, Schema.Optional(_, _)) =>
          scala.Right(())
        case (DynamicValue.Tuple(leftValue, rightValue), Schema.Tuple2(leftSchema, rightSchema, _)) =>
          accumulateErrors(scala.List(validateValue(leftValue, leftSchema), validateValue(rightValue, rightSchema)))
        case (DynamicValue.LeftValue(value), Schema.Either(schema1, _, _)) =>
          validateValue(value, schema1)
        case (DynamicValue.RightValue(value), Schema.Either(_, schema2, _)) =>
          validateValue(value, schema2)
        case (DynamicValue.Error(message), _) =>
          scala.Left(Chunk(s"DynamicValue error: $message"))
        case _ =>
          scala.Left(Chunk(s"Type mismatch between DynamicValue and Schema"))
      }

    def validateRecord(
      values: scala.collection.immutable.ListMap[String, DynamicValue],
      structure: Chunk[Schema.Field[_, _]]
    ): scala.Either[Chunk[String], Unit] =
      accumulateErrors(structure.map { field =>
        values.get(field.name) match {
          case Some(value) => validateValue(value, field.schema)
          case None =>
            if (field.optional) scala.Right(()) else scala.Left(Chunk(s"Missing required field: ${field.name}"))
        }
      })

    def accumulateErrors(
      validations: Iterable[scala.Either[Chunk[String], Unit]]
    ): scala.Either[Chunk[String], Unit] = {
      val errors = Chunk.fromIterable(validations).flatMap {
        case scala.Left(es) => es
        case scala.Right(_) => Chunk.empty
      }
      if (errors.isEmpty) scala.Right(()) else scala.Left(errors)
    }

    validateValue(self, schema)
  }
}
