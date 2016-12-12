/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.predictionio.data.storage.elasticsearch

import org.apache.predictionio.data.storage.{PEvents, Event}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

// TODO for elasticsearch
import org.apache.spark.SparkConf
import org.elasticsearch.spark._


class ESPEvents extends PEvents {

  override
  def find(
    appId: Int,
    channelId: Option[Int] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    targetEntityId: Option[Option[String]] = None
    )(sc: SparkContext): RDD[Event] = {

    // TODO Spark Configuration
    val conf = new SparkConf().setAppName("").setMaster("")
    conf.set("es.index.auto.create", "true")

    // TODO sc is used as SparkContext
    // Spark Configuration not needed?

    val rdd = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]],
      classOf[Text], classOf[MapWritable]
    ).map {
      case (key, row) => ??? // なんかわからんけどEventの型に変換
    }

    rdd
  }

  override
  def write(events: RDD[Event], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit = {
    ???
  }

  override
  def delete(eventIds: RDD[String], appId: Int, channelId: Option[Int])(sc: SparkContext): Unit = {
    ???
  }

}
