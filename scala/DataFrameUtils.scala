// Databricks notebook source
package com.dataframe.utils

object DataFrameUtils extends Serializable {
  import com.databricks.dbutils_v1.{DBUtilsV1}
  import scala.util.{Try, Success, Failure}
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.Column
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.expressions.Window

  import java.util.Base64
  import java.util.UUID
  import java.nio.charset.StandardCharsets
  import java.nio.ByteBuffer
  import javax.crypto.Cipher
  import javax.crypto.SecretKey
  import javax.crypto.spec.SecretKeySpec

  def convertBase64toUUID(columnName: String)(df: DataFrame) = {
    val converterUDF = udf((base64Str: String) => {
      if (base64Str == null) {
        null
      } else {
        val bytes = Base64.getDecoder().decode(base64Str)
        val buffer = ByteBuffer.wrap(bytes)
        val uuid = new UUID(buffer.getLong(), buffer.getLong())
        uuid.toString()
      }
    })
    df.withColumn(s"${columnName}Uuid", converterUDF(col(columnName)))
  }

  def convertGUIDtoUUID(columnName: String)(df: DataFrame) = {
    val converterUDF = udf((guidStr: String) => {
      if (guidStr == null) {
          null
      } else {
        var formattedGuid = guidStr.replaceAll("-", "")
        formattedGuid.replaceAll("(.{8})(.{4})(.{4})(.{4})(.{12})", "$1-$2-$3-$4-$5").replaceAll("(.{2})(.{2})(.{2})(.{2}).(.{2})(.{2}).(.{2})(.{2})(.{18})", "$4$3$2$1-$6$5-$8$7$9")
      }
    })
    df.withColumn(s"${columnName}".replace("Guid", "Uuid"), converterUDF(col(columnName)))
  }

  def convertUUIDtoBase64(columnName: String)(df: DataFrame) = {
    val converterUDF = udf((uuidStr: String) => {
      if (uuidStr == null) {
        null
      } else {
        try{
          val uuid = UUID.fromString(uuidStr)
          val buffer = ByteBuffer.wrap(new Array[Byte](16))
          buffer.putLong(uuid.getMostSignificantBits)
          buffer.putLong(uuid.getLeastSignificantBits)

          val uuidBytes = buffer.array
          Base64.getEncoder.encodeToString(uuidBytes)
        }catch{
          case _: Throwable => null
        }
      }
    })
    df.withColumn(s"${columnName}".replace("Uuid", ""), converterUDF(col(columnName)))
  }

  def convertGuidColumns(df:DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (updatedDf, columnName) =>
      if (columnName.endsWith("Guid")) {
        val columnUuid = columnName.replace("Guid", "Uuid")
        updatedDf
          .transform(convertGUIDtoUUID(columnName))
          .transform(convertUUIDtoBase64(columnUuid))
      } else {
        updatedDf
      }
    }
  }

  def enforceColumn(columnName: String, columnType: String, defaultValue: Column)(df: DataFrame) = {
    Try(df(columnName)) match {
      case Success(_) => df
      case Failure(_) => df.withColumn(columnName, defaultValue.cast(columnType))
    }
  }

  def enforceColumn(columnName: String, columnType: DataType, defaultValue: Column)(df: DataFrame) = {
    Try(df(columnName)) match {
      case Success(_) => df
      case Failure(_) => df.withColumn(columnName, defaultValue.cast(columnType))
    }
  }

  def enforceColumns(columnType: DataType, defaultValue: Column, fields:String*)(dataframe: DataFrame): DataFrame = {
    var df = dataframe
    for (field <- fields) {
      df = df.transform(enforceColumn(field, columnType, defaultValue))
    }
    df
  }

  def trimValues(columns: String*)(df: DataFrame): DataFrame = {
    val trimmedColumns = columns.map(c => trim(col(c)).as(c))
    df.select(trimmedColumns: _*)
  }

  def lowerCaseValues(columns: String*)(df: DataFrame): DataFrame = {
    columns.foldLeft(df) { (tempDF, columnName) =>
      tempDF.withColumn(columnName, lower(col(columnName)))
    }
  }

  def camelCaseToUpperSnakeCase(df: DataFrame): DataFrame = {
    var col_seq: Seq[String] = df.columns.toSeq
    var scol = Seq.empty[String]
    for (c <- col_seq) {
      var r: String = ""
      for (s <- c) {
        if (s == s.toUpper) {
          r = r + "_" + s.toUpper
        } else {
          r += s.toUpper
        }
      }
      scol :+= (r)
    }
    df.toDF(scol: _*)
  }

  def getMaxDateOfField(dataframe: DataFrame, fieldName: String, format: String) : String = {
    dataframe.agg(date_format(max(col(fieldName)), format)).first.get(0).toString
  }

  def getMinDateOfField(dataframe: DataFrame, fieldName: String, format: String) : String = {
    dataframe.agg(date_format(min(col(fieldName)), format)).first.get(0).toString
  }

  def flattenSchema(df:DataFrame) : DataFrame = {
    def func(schema: StructType, prefix: String = null) : Array[Column] = {
      schema.fields.flatMap(f => {
        val colName = if (prefix == null) f.name else (prefix + "." + f.name)

        f.dataType match {
          case st: StructType => func(st, colName)
          case _ => Array(col(colName).as(colName.replace(".","_")))
        }
      })
    }
    val schema = func(df.schema)
    df.select(schema:_*)
  }

  def dropStructArray(df:DataFrame) : DataFrame = {
    def func(schema: StructType) : Array[Column] = {
      schema.fields.flatMap {
        case StructField(name, ArrayType(structType: StructType, _), _, _) => Nil
        case StructField(name, _, _, _) => Array(col(name))
      }
    }
    val schema = func(df.schema)
    df.select(schema:_*)
  }

  def getDuplicatedRows(columns: Array[String], frequency: Int = 1)(df: DataFrame): DataFrame = {
    val w = Window.partitionBy(columns.map(col(_)):_*)

    df.select(col("*"), count(columns.toList(0)).over(w).alias("dupRow"))
               .where(s"dupRow > ${frequency}")
               .drop("dupRow")
  }

  def decryptColumn(columnName: String, key: String, keyType: String = "AES")(df: DataFrame) = {
    val decryptUDF = udf((text: String) => {
      if (text == null) {
        null
      } else {
        try {
          val aesKey = new SecretKeySpec(key.getBytes(), keyType)
          val cipher = Cipher.getInstance(keyType)
          cipher.init(Cipher.DECRYPT_MODE, aesKey)
          new String(cipher.doFinal(Base64.getDecoder().decode(text)))
          }
        catch {
          case _: Exception => text
        }
      }
    })
    df.withColumn(s"${columnName}Decrypeted", decryptUDF(col(columnName)))
  }

  def standardize(target: DataFrame)(df: DataFrame): DataFrame =  {
    val source = df
    val sourceKeys = source.schema.map{_.toString}.toSet
    val targetKeys = target.schema.map{_.toString}.toSet
    val equalKeys =  sourceKeys.intersect(targetKeys)
    val diffKeys = targetKeys -- equalKeys
    val diff = (source.schema ++ target.schema).filter(sf => diffKeys.contains(sf.toString)).toList
    var resultDF = source

    for (colName <- diff) {
      val nameColum  = colName.name
      val typeSource = source.schema(nameColum).dataType
      val typetarget = target.schema(nameColum).dataType
      if ((typetarget == StringType) && (typeSource == DataTypes.createArrayType(StringType,true))) {
        resultDF = resultDF.withColumn(nameColum, to_json(col(nameColum)))
      } else {
        resultDF = resultDF.withColumn(nameColum, col(nameColum).cast(typetarget))
      }
    }
    resultDF
  }

  def validateSchema(df: DataFrame, schema: StructType): Boolean = {
    val actualSchema = df.schema
    val actualColumns = actualSchema.map(field => (field.name, field.dataType))
    val expectedColumns = schema.map(field => (field.name, field.dataType))
    actualColumns == expectedColumns
  }

  def adjustSchema(schema: StructType)(df: DataFrame): DataFrame = {
    if (!validateSchema(df, schema)) {
      val actualSchema = df.schema

      val actualColumns = actualSchema.map(field => field.name)
      val expectedColumns = schema.map(field => field.name)

      val missingColumns = expectedColumns.diff(actualColumns)
      val extraColumns = actualColumns.diff(expectedColumns)

      var validatedDF = df

      for (expectedField <- schema.fields) {
        val columnName = expectedField.name
        val expectedDataType = expectedField.dataType

        val actualField = actualSchema.find(_.name == columnName)

        actualField match {
          case Some(field) if field.dataType != expectedDataType =>
            validatedDF = validatedDF.withColumn(columnName, col(columnName).cast(expectedDataType))
            println(s"[INFO] Casting column '$columnName' from ${field.dataType} to $expectedDataType")
          case None if expectedColumns.contains(columnName) =>
            println(s"[INFO] Column '$columnName' is missing from the given dataframe")
          case _ =>
        }
      }

      for (extraColumn <- extraColumns) {
        validatedDF = validatedDF.drop(extraColumn)
        println(s"[INFO] Dropping column '$extraColumn'")
      }

      validatedDF
    } else {
      df
    }
  }

  def replaceWithBoolean(
    columns: List[String],
    trueValues: List[String] = List("yes"),
    falseValues: List[String] = List("no")
  )(df: DataFrame): DataFrame = {
    val transformedDF = columns.foldLeft(df) { (accDF, colName) =>
      val trueValuesExpr = trueValues.map(value => s"'$value'").mkString(", ")
      val falseValuesExpr = falseValues.map(value => s"'$value'").mkString(", ")

      accDF.withColumn(
        colName,
        expr(
          s"CASE WHEN LOWER($colName) IN ($trueValuesExpr) THEN true " +
          s"WHEN LOWER($colName) IN ($falseValuesExpr) THEN false " +
          s"ELSE " +
          s"CAST(NULL AS BOOLEAN) END")
      )
    }
    transformedDF
  }

  def checkFilters(filters: List[Column], checkFiltersColumn: String = "isConsidered")(df: DataFrame): DataFrame = {
    val combinedFilterCondition = coalesce(filters.reduce((a, b) => a && b), lit(false))
    df.withColumn(checkFiltersColumn, combinedFilterCondition)
  }

  implicit class DataFrameExtensions(dataframe: DataFrame) {

    def convertBase64toUUID(columnName: String): DataFrame = {
      dataframe.transform(DataFrameUtils.convertBase64toUUID(columnName))
    }

    def convertUUIDtoBase64(columnName: String): DataFrame = {
      dataframe.transform(DataFrameUtils.convertUUIDtoBase64(columnName))
    }

    def convertGUIDtoUUID(columnName: String): DataFrame = {
      dataframe.transform(DataFrameUtils.convertGUIDtoUUID(columnName))
    }

    def convertGuidColumns(): DataFrame = dataframe.transform(DataFrameUtils.convertGuidColumns)

    def enforceColumn(columnName: String, columnType: String, defaultValue: Column): DataFrame = {
      dataframe.transform(DataFrameUtils.enforceColumn(columnName, columnType, defaultValue))
    }

    def enforceColumn(columnName: String, columnType: DataType, defaultValue: Column): DataFrame = {
      dataframe.transform(DataFrameUtils.enforceColumn(columnName, columnType, defaultValue))
    }

    def enforceColumns(columnType: DataType, defaultValue: Column, fields:String*): DataFrame = {
      dataframe.transform(DataFrameUtils.enforceColumns(columnType, defaultValue, fields:_*))
    }

    def trimValues(columns: String*): DataFrame = {
      dataframe.transform(DataFrameUtils.trimValues(columns:_*))
    }

    def lowerCaseValues(columns: String*): DataFrame = {
      dataframe.transform(DataFrameUtils.lowerCaseValues(columns:_*))
    }

    def getDuplicatedRows(columns: Array[String], frequency: Int = 1): DataFrame = {
      dataframe.transform(DataFrameUtils.getDuplicatedRows(columns, frequency))
    }

    def decryptColumn(columnName: String, key: String, keyType: String = "AES"): DataFrame = {
      dataframe.transform(DataFrameUtils.decryptColumn(columnName, key, keyType))
    }

    def flattenSchema(): DataFrame = dataframe.transform(DataFrameUtils.flattenSchema)

    def dropStructArray(): DataFrame = dataframe.transform(DataFrameUtils.dropStructArray)

    def standardize(target: DataFrame): DataFrame = dataframe.transform(DataFrameUtils.standardize(target))

    def adjustSchema(schema: StructType): DataFrame = dataframe.transform(DataFrameUtils.adjustSchema(schema))

    def replaceWithBoolean(columns: List[String], trueValues: List[String] = List("yes"), falseValues: List[String] = List("no")): DataFrame = {
      dataframe.transform(DataFrameUtils.replaceWithBoolean(columns, trueValues, falseValues))
    }

    def checkFilters(filters: List[Column], checkFiltersColumn: String = "isConsidered"): DataFrame = {
      dataframe.transform(DataFrameUtils.checkFilters(filters, checkFiltersColumn))
    }

  }
}
