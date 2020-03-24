<!--
   Copyright 2020 Viseca Card Services SA

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Watermark Generation

This package covers strategies to generate watermarks for streams that do not provide watermarks by themselves (Kafka topics integrated by meand of the `FlinkKafkaConsumer` class is such a case).

We include `WatermarkAssigner` for
* batch structured streams that need to emit watermarks in close succession of a finished batch load, instead of to wait for the next batch to start


