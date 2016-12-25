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

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.{Event, LEvents, StorageClientConfig}

import scala.concurrent.{Future, ExecutionContext}

class ESLEvents(config: StorageClientConfig)
  extends LEvents with Logging {

  override
  def init(appId: Int, channelId: Option[Int]): Boolean = {
    // check namespace exist
    true
  }

  override
  def remove(appId: Int, channelId: Option[Int] = None): Boolean = {
    true
  }

  override
  def close(): Unit = {
    // client.admin.close()
    // client.connection.close()
  }

  override
  def futureInsert(
    event: Event, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext):
    Future[String] = {
    Future {
      "TODO"
    }
  }

  override
  def futureGet(
    eventId: String, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext):
    Future[Option[Event]] = {
      Future {
        None
      }
    }

  override
  def futureDelete(
    eventId: String, appId: Int)(implicit ec: ExecutionContext):
    Future[Boolean] = {
    Future {
      true
    }
  }

  override
  def futureFind(appId: Int)(implicit ec: ExecutionContext):
  Future[Iterator[Event]] = {
    Future {
      List()
    }
  }

}
