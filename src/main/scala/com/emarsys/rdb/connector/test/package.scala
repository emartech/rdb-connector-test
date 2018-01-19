package com.emarsys.rdb.connector

import java.util.UUID

package object test {
  def uuidGenerate = UUID.randomUUID().toString.replace("-","")
}
