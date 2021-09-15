---
{
    "title": "json_unquote",
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# json_unquote
## Description
### Syntax

`VARCHAR json_unquote(VARCHAR)`


Remove the double quotation marks of the value, and output the special escape characters.

## example

```
MySQL> SELECT json_unquote(NULL);
+--------------------+----------------------+
| json_quote('null') | json_quote('"null"') |
+--------------------+----------------------+
| "null"             | "\"null\""           |
+--------------------+----------------------+

MySQL> SELECT json_unquote("");
+------------------+
| json_unquote('') |
+------------------+
|                  |
+------------------+

MySQL> SELECT json_unquote('"\\t\\u0032"');
+--------------------------------+
| json_unquote('\"\\t\\u0032\"') |
+--------------------------------+
| 	2                             |
+--------------------------------+

MySQL> select json_unquote('["a", "b", "c"]');
+---------------------------------------+
| json_unquote('[\"a\", \"b\", \"c\"]') |
+---------------------------------------+
| ["a", "b", "c"]                       |
+---------------------------------------+


MySQL> SELECT JSON_UNQUOTE('"\\u6211\\u0036"');
+------------------------------------+
| json_unquote('\"\\u6211\\u0036\"') |
+------------------------------------+
| 我6                                |
+------------------------------------+
```
## keyword
json_unquote
