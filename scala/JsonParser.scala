// Databricks notebook source
package com.tools.utils

object JsonParser extends Serializable {
  import org.json._
  import collection.JavaConverters._
  import java.util.Base64
  import java.util.UUID
  import java.nio.charset.StandardCharsets
  import java.nio.ByteBuffer

  def parse(json: String): String = {
    val obj = new JSONObject(json)
    val uuidsJson = new JSONObject("{}")
    val parsedJson = convertBSon(obj, obj, "", uuidsJson)
    mergeJsons(parsedJson, uuidsJson).toString
  }

  def mergeJsons(mainJson: JSONObject, mergedJson: JSONObject): JSONObject = {
    val keys: Iterator[String] = mergedJson.keys().asScala
    while (keys.hasNext) {
      var key = keys.next
      mainJson.put(key, mergedJson.get(key))
    }
    mainJson
  }

  def convertBase64(base64Str: String): String = {
    val bytes = Base64.getDecoder().decode(base64Str)
    val buffer = ByteBuffer.wrap(bytes)
    val uuid = new UUID(buffer.getLong(), buffer.getLong())
    uuid.toString()
  }

  def convertBSon(json: JSONObject, target: JSONObject, keyId: String = "", uuidJson: JSONObject): JSONObject = {
    val keys: Iterator[String] = json.keys().asScala
    while (keys.hasNext) {
      var key = keys.next
      if ((key == "$binary") || (key == "$date")) {
        try{
          if (key == "$binary") {
            val base64Id = json.getJSONObject("$binary").getString("base64")
            target.put(keyId, base64Id)
            uuidJson.put(s"${keyId}Uuid", convertBase64(base64Id))
          } else {
            target.put(keyId, json.getString("$date"))
          }
         }
         catch {
           case e: JSONException => println("object json not found")
           case _: Throwable => println("json bad format")
         }
      }
      else {
        if (json.get(key).isInstanceOf[JSONObject]) {
          val objm = json.getJSONObject(key)
          if (objm != null) {
            convertBSon(objm, json, key, uuidJson)
          }
        }
        else {
          if (json.get(key).isInstanceOf[JSONArray]) {
            val objm = json.getJSONArray(key)
            var index = 0;
            if(objm.length > 0 && objm.get(0).isInstanceOf[JSONObject]) {
              for (index <- 0 until objm.length) {
                val jArrayItem = objm.getJSONObject(index)
                convertBSon(jArrayItem, json, key, uuidJson)
              }
            }
          }
        }
      }
    }
    target
  }
}
