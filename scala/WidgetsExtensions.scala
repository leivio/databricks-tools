// Databricks notebook source
implicit class WidgetsExtensions(val widgets: com.databricks.dbutils_v1.WidgetsUtils) {
  import scala.util.{Failure, Success, Try}
  import spark.implicits._
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  
  
  def exists(name: String):Boolean = {
    Try(widgets.get(name)) match {
      case Success(result) => true
      case Failure(exception) => false
    }
  }
  
  def getOrElse(name: String, default: String) = {
    if(exists(name)) widgets.get(name)
    else default
  }
}
