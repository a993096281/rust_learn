// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_PROXY_GET: ::grpcio::Method<super::proxy::GetRequest, super::proxy::GetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/rawproxy.proxy/Get",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PROXY_PUT: ::grpcio::Method<super::proxy::PutRequest, super::proxy::PutResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/rawproxy.proxy/Put",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PROXY_DELETE: ::grpcio::Method<super::proxy::DeleteRequest, super::proxy::DeleteResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/rawproxy.proxy/Delete",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_PROXY_SCAN: ::grpcio::Method<super::proxy::ScanRequest, super::proxy::ScanResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/rawproxy.proxy/Scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct ProxyClient {
    client: ::grpcio::Client,
}

impl ProxyClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ProxyClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_opt(&self, req: &super::proxy::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::proxy::GetResponse> {
        self.client.unary_call(&METHOD_PROXY_GET, req, opt)
    }

    pub fn get(&self, req: &super::proxy::GetRequest) -> ::grpcio::Result<super::proxy::GetResponse> {
        self.get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_async_opt(&self, req: &super::proxy::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::GetResponse>> {
        self.client.unary_call_async(&METHOD_PROXY_GET, req, opt)
    }

    pub fn get_async(&self, req: &super::proxy::GetRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::GetResponse>> {
        self.get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_opt(&self, req: &super::proxy::PutRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::proxy::PutResponse> {
        self.client.unary_call(&METHOD_PROXY_PUT, req, opt)
    }

    pub fn put(&self, req: &super::proxy::PutRequest) -> ::grpcio::Result<super::proxy::PutResponse> {
        self.put_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_async_opt(&self, req: &super::proxy::PutRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::PutResponse>> {
        self.client.unary_call_async(&METHOD_PROXY_PUT, req, opt)
    }

    pub fn put_async(&self, req: &super::proxy::PutRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::PutResponse>> {
        self.put_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_opt(&self, req: &super::proxy::DeleteRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::proxy::DeleteResponse> {
        self.client.unary_call(&METHOD_PROXY_DELETE, req, opt)
    }

    pub fn delete(&self, req: &super::proxy::DeleteRequest) -> ::grpcio::Result<super::proxy::DeleteResponse> {
        self.delete_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_async_opt(&self, req: &super::proxy::DeleteRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::DeleteResponse>> {
        self.client.unary_call_async(&METHOD_PROXY_DELETE, req, opt)
    }

    pub fn delete_async(&self, req: &super::proxy::DeleteRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::DeleteResponse>> {
        self.delete_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::proxy::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::proxy::ScanResponse> {
        self.client.unary_call(&METHOD_PROXY_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::proxy::ScanRequest) -> ::grpcio::Result<super::proxy::ScanResponse> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_async_opt(&self, req: &super::proxy::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::ScanResponse>> {
        self.client.unary_call_async(&METHOD_PROXY_SCAN, req, opt)
    }

    pub fn scan_async(&self, req: &super::proxy::ScanRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::proxy::ScanResponse>> {
        self.scan_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Proxy {
    fn get(&mut self, ctx: ::grpcio::RpcContext, req: super::proxy::GetRequest, sink: ::grpcio::UnarySink<super::proxy::GetResponse>);
    fn put(&mut self, ctx: ::grpcio::RpcContext, req: super::proxy::PutRequest, sink: ::grpcio::UnarySink<super::proxy::PutResponse>);
    fn delete(&mut self, ctx: ::grpcio::RpcContext, req: super::proxy::DeleteRequest, sink: ::grpcio::UnarySink<super::proxy::DeleteResponse>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::proxy::ScanRequest, sink: ::grpcio::UnarySink<super::proxy::ScanResponse>);
}

pub fn create_proxy<S: Proxy + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PROXY_GET, move |ctx, req, resp| {
        instance.get(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PROXY_PUT, move |ctx, req, resp| {
        instance.put(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PROXY_DELETE, move |ctx, req, resp| {
        instance.delete(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_PROXY_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
