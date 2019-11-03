/*
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.minio.spark.select.util

import java.net.URI

object S3URI {
  private[select] def toAmazonS3URI(
    location: String): COSURI = {
    val uri = new URI(location)
    val uriScheme = uri.getScheme
    uriScheme match {
      case "cos" =>
        new COSURI(uri)
      case "s3a" | "s3n" =>
        new COSURI(new URI("s3", uri.getUserInfo, uri.getHost, uri.getPort, uri.getPath,
          uri.getQuery, uri.getFragment))
      case other =>
        throw new IllegalArgumentException(s"Unrecognized scheme $other; expected s3, or s3a or s3n")
    }
  }
}
