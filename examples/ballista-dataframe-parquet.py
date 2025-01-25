# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# %%
from datafusion import SessionContext
from datafusion import udf, functions as f
import pyarrow.compute as pc
import pyarrow

ctx = SessionContext(url = "df://localhost:50050")
df = ctx.read_parquet("/Users/marko/TMP/yellow_tripdata_2021-01.parquet").aggregate(
    [f.col("passenger_count")], [f.count_star()]
)
df.show()


def to_miles(km_data):
    conversation_rate_multiplier = 0.62137119
    return pc.multiply(km_data, conversation_rate_multiplier)    

to_miles_udf = udf(to_miles, [pyarrow.float64()], pyarrow.float64(), "stable")

df = df.select(to_miles_udf(f.col("passenger_count")), f.col("passenger_count"))
df.show()
# %%
