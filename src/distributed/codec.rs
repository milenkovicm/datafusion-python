// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ScalarUDF;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::FromProtoError;
use prost::Message;
use pyo3::{PyResult, Python};
use serde::UdfProto;
use std::fmt::Debug;
use std::sync::Arc;

use crate::distributed::pickle::CloudPickle;
use crate::udf::PythonUDF;

pub struct PyLogicalCodec {
    inner: BallistaLogicalExtensionCodec,
    cloudpickle: CloudPickle,
}

impl PyLogicalCodec {
    pub fn try_new(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            inner: BallistaLogicalExtensionCodec::default(),
            cloudpickle: CloudPickle::try_new(py)?,
        })
    }
}

impl Default for PyLogicalCodec {
    fn default() -> Self {
        Python::with_gil(|py| Self::try_new(py).expect("py logical codec created"))
    }
}

impl Debug for PyLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyLogicalCodec").finish()
    }
}

impl LogicalExtensionCodec for PyLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &datafusion::sql::TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &datafusion::sql::TableReference,
        node: std::sync::Arc<dyn datafusion::catalog::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &datafusion::prelude::SessionContext,
    ) -> datafusion::error::Result<
        std::sync::Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    > {
        self.inner.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: std::sync::Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_file_format(buf, node)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> datafusion::common::Result<Arc<ScalarUDF>> {
        if !buf.is_empty() {
            let udf_proto: UdfProto =
                UdfProto::decode(buf).map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let func = Python::with_gil(|py| {
                self.cloudpickle
                    .unpickle(py, &udf_proto.blob)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))
            });
            let volatility = (&udf_proto.volatility()).into();
            let return_type = (&udf_proto.result_type.unwrap_or_default()).try_into()?;
            let input_types: datafusion::common::Result<Vec<DataType>> = udf_proto
                .input_types
                .iter()
                .map(|t| {
                    t.try_into()
                        .map_err(|e: FromProtoError| DataFusionError::Execution(e.to_string()))
                })
                .collect();

            let function = PythonUDF::new(name, input_types?, return_type, volatility, func?);
            let function = ScalarUDF::new_from_impl(function);

            Ok(function.into())
        } else {
            self.inner.try_decode_udf(name, buf)
        }
    }

    fn try_encode_udf(
        &self,
        node: &ScalarUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        match node.inner().as_any().downcast_ref::<PythonUDF>() {
            Some(udf) => {
                let data = Python::with_gil(|py| {
                    self.cloudpickle
                        .pickle(py, &udf.func)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                })?;
                let udf_proto = UdfProto::try_from_udf(
                    &node.signature().volatility,
                    &udf.input_types,
                    &udf.return_type,
                    data,
                )?;

                let mut data = udf_proto.encode_to_vec();

                buf.append(&mut data);
                Ok(())
            }
            None => self.inner.try_encode_udf(node, buf),
        }
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> datafusion::error::Result<std::sync::Arc<datafusion::logical_expr::AggregateUDF>> {
        self.inner.try_decode_udaf(name, buf)
    }

    fn try_encode_udaf(
        &self,
        node: &datafusion::logical_expr::AggregateUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_udaf(node, buf)
    }

    fn try_decode_udwf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> datafusion::error::Result<std::sync::Arc<datafusion::logical_expr::WindowUDF>> {
        self.inner.try_decode_udwf(name, buf)
    }

    fn try_encode_udwf(
        &self,
        node: &datafusion::logical_expr::WindowUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode_udwf(node, buf)
    }
}

pub struct PyPhysicalCodec {
    inner: BallistaPhysicalExtensionCodec,
    cloudpickle: CloudPickle,
}

impl Default for PyPhysicalCodec {
    fn default() -> Self {
        Python::with_gil(|py| Self::try_new(py).expect("py logical codec created"))
    }
}

impl Debug for PyPhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyPhysicalCodec").finish()
    }
}

impl PyPhysicalCodec {
    pub fn try_new(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            inner: BallistaPhysicalExtensionCodec::default(),
            cloudpickle: CloudPickle::try_new(py)?,
        })
    }
}

impl PhysicalExtensionCodec for PyPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>],
        registry: &dyn datafusion::execution::FunctionRegistry,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        self.inner.try_decode(buf, inputs, registry)
    }

    fn try_encode(
        &self,
        node: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        self.inner.try_encode(node, buf)
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> datafusion::common::Result<Arc<ScalarUDF>> {
        if !buf.is_empty() {
            let udf_proto: UdfProto =
                UdfProto::decode(buf).map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let func = Python::with_gil(|py| {
                self.cloudpickle
                    .unpickle(py, &udf_proto.blob)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))
            });

            let volatility = (&udf_proto.volatility()).into();
            let return_type = (&udf_proto.result_type.unwrap_or_default()).try_into()?;
            let input_types: datafusion::common::Result<Vec<DataType>> = udf_proto
                .input_types
                .iter()
                .map(|t| {
                    t.try_into()
                        .map_err(|e: FromProtoError| DataFusionError::Execution(e.to_string()))
                })
                .collect();

            let function = PythonUDF::new(name, input_types?, return_type, volatility, func?);
            let function = ScalarUDF::new_from_impl(function);

            Ok(function.into())
        } else {
            self.inner.try_decode_udf(name, buf)
        }
    }

    fn try_encode_udf(
        &self,
        node: &ScalarUDF,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        match node.inner().as_any().downcast_ref::<PythonUDF>() {
            Some(udf) => {
                let data = Python::with_gil(|py| {
                    self.cloudpickle
                        .pickle(py, &udf.func)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                })?;
                let udf_proto = UdfProto::try_from_udf(
                    &node.signature().volatility,
                    &udf.input_types,
                    &udf.return_type,
                    data,
                )?;

                let mut data = udf_proto.encode_to_vec();

                buf.append(&mut data);

                Ok(())
            }
            None => self.inner.try_encode_udf(node, buf),
        }
    }
}

pub mod serde {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::Result;
    use datafusion_proto::protobuf::ToProtoError;

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct UdfProto {
        #[prost(enumeration = "Volatility", tag = 1)]
        pub volatility: i32,
        #[prost(message, repeated, tag = 2)]
        pub input_types:
            ::prost::alloc::vec::Vec<datafusion_proto::generated::datafusion_common::ArrowType>,
        #[prost(message, tag = 3)]
        pub result_type: Option<datafusion_proto::generated::datafusion_common::ArrowType>,
        #[prost(bytes, tag = 5)]
        pub blob: Vec<u8>,
    }

    impl UdfProto {
        pub fn try_from_udf(
            volatility: &datafusion::logical_expr::Volatility,
            input_types: &[DataType],
            result_type: &DataType,
            blob: Vec<u8>,
        ) -> Result<UdfProto> {
            let volatility: Volatility = volatility.into();
            let return_type = result_type.try_into()?;
            let input_types: Result<
                Vec<datafusion_proto::generated::datafusion_common::ArrowType>,
            > = input_types
                .into_iter()
                .map(|a| a.try_into().map_err(|e: ToProtoError| e.into()))
                .collect();

            Ok(UdfProto {
                volatility: volatility.into(),
                result_type: Some(return_type),
                input_types: input_types?,
                blob,
            })
        }
    }

    #[derive(Clone, Debug, ::prost::Enumeration)]
    pub enum Volatility {
        Volatile = 0,
        Immutable = 1,
        Stable = 2,
    }

    impl From<&datafusion::logical_expr::Volatility> for Volatility {
        fn from(value: &datafusion::logical_expr::Volatility) -> Self {
            match value {
                datafusion::logical_expr::Volatility::Immutable => Volatility::Immutable,
                datafusion::logical_expr::Volatility::Stable => Volatility::Stable,
                datafusion::logical_expr::Volatility::Volatile => Volatility::Volatile,
            }
        }
    }

    impl From<&Volatility> for datafusion::logical_expr::Volatility {
        fn from(value: &Volatility) -> Self {
            match value {
                Volatility::Volatile => datafusion::logical_expr::Volatility::Volatile,
                Volatility::Immutable => datafusion::logical_expr::Volatility::Immutable,
                Volatility::Stable => datafusion::logical_expr::Volatility::Stable,
            }
        }
    }
}
