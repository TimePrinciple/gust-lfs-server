use crate::config::Configuration;
use crate::content_store::ContentStore;
use crate::database::mysql;
use anyhow::Result;
use axum::handler::Handler;
use axum::http::header::{ACCEPT, AUTHORIZATION};
use bytes::{BufMut, BytesMut};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

use axum::body::Body;
use axum::extract::{BodyStream, Path, Query, State};
use axum::http::{header::HeaderMap, Request, Response, StatusCode};
use axum::routing::{get, post};
use axum::{Router, Server};
use serde::{Deserialize, Serialize};
use tower_http::validate_request::ValidateRequestHeaderLayer;

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVars {
    pub oid: String,
    pub size: i64,
    pub user: String,
    pub password: String,
    pub repo: String,
    pub authorization: String,
}

impl RequestVars {
    async fn download_link(&self, config: Arc<Mutex<Configuration>>) -> String {
        self.internal_link("objects".to_string(), config).await
    }

    async fn upload_link(&self, config: Arc<Mutex<Configuration>>, use_tus: bool) -> String {
        self.internal_link("objects".to_string(), config).await
    }

    async fn internal_link(&self, subpath: String, config: Arc<Mutex<Configuration>>) -> String {
        let mut path = PathBuf::new();

        let user = &self.user;
        if user.len() > 0 {
            path.push(user);
        }

        let repo = &self.repo;
        if repo.len() > 0 {
            path.push(repo);
        }

        let config = config.lock().await;
        path.push(&config.ext_origin);

        path.push(&subpath);
        path.push(&self.oid);

        format!("{}", path.into_os_string().into_string().unwrap())
    }

    async fn verify_link(&self, config: Arc<Mutex<Configuration>>) -> String {
        let path = format!("/verify/{}", &self.oid);
        let config = config.lock().await;
        format!("{}{}", config.ext_origin, path)
    }
}

#[derive(Serialize, Deserialize)]
pub struct BatchVars {
    pub transfers: Vec<String>,
    pub operation: String,
    pub objects: Vec<RequestVars>,
}

#[derive(Debug)]
pub struct MetaObject {
    pub oid: String,
    pub size: i64,
    pub exist: bool,
}

#[derive(Serialize, Deserialize)]
pub struct BatchResponse {
    pub transfer: String,
    pub objects: Vec<Representation>,
}

#[derive(Serialize, Deserialize)]
pub struct Link {
    pub href: String,
    pub header: HashMap<String, String>,
    pub expires_at: f64,
}

#[derive(Serialize, Deserialize)]
pub struct ObjectError {
    pub code: i64,
    pub message: String,
}

#[derive(Serialize, Deserialize)]
pub struct Representation {
    pub oid: String,
    pub size: i64,
    pub actions: HashMap<String, Link>,
    pub error: ObjectError,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Lock {
    pub id: String,
    pub path: String,
    pub owner: User,
    pub locked_at: f64,
}

#[derive(Serialize, Deserialize)]
pub struct LockRequest {
    pub path: String,
}

pub struct LockResponse {
    pub lock: Lock,
    pub message: String,
}

pub struct UnlockRequest {
    pub force: bool,
}

pub struct UnlockResponse {
    pub lock: Lock,
    pub message: String,
}

pub struct LockList {
    pub locks: Vec<Lock>,
    pub next_cursor: String,
    pub message: String,
}

pub struct VerifiableLockRequest {
    pub cursor: String,
    pub limit: i64,
}

pub struct VerifiableLockList {
    pub ours: Vec<Lock>,
    pub theirs: Vec<Lock>,
    pub next_cursor: String,
    pub message: String,
}

#[derive(Clone)]
struct AppState {
    config: Arc<Mutex<Configuration>>,
    db_storage: Arc<Mutex<mysql::meta_storage::MysqlStorage>>,
}

pub async fn lfs_server() -> Result<(), Box<dyn std::error::Error>> {
    // load env variables
    // let host = env::var("HOST").expect("HOST is not set in .env file");
    // let port = env::var("PORT").expect("PORT is not set in .env file");
    let server_url = format!("{}:{}", "127.0.0.1", "9999");

    let state = AppState {
        config: Arc::new(Mutex::new(Configuration::init())),
        db_storage: Arc::new(Mutex::new(mysql::init().await)),
    };

    let app = Router::new()
        .route("/:user/:repo/objects/batch", get(batch_handler))
        .route(
            "/:user/:repo/objects/:oid",
            get(logged_method_router).put(put_handler),
        )
        .route("/:user/:repo/objects", post(post_handler))
        .route(
            "/:user/:repo/locks",
            get(locks_handler).post(create_lock_handler),
        )
        .route("/:user/:repo/locks/verify", post(locks_verify_handler))
        .route("/:user/:repo/locks/:id/unlock", post(delete_lock_handler))
        .route("/objects/batch", post(batch_handler))
        .route("/objects/:oid", get(method_router).put(put_handler))
        .route("/objects", post(post_handler))
        .route("/verify/:oid", post(verify_handler))
        // .route("/:user/:repo/objects/:oid", get(get_meta_handler)
        //     .route_layer(ValidateRequestHeaderLayer::accept("application/vnd.git-lfs+json")))
        .with_state(state);

    let addr = SocketAddr::from_str(&server_url).unwrap();
    Server::bind(&addr).serve(app.into_make_service()).await?;

    Ok(())
}

async fn batch_handler(
    state: State<AppState>,
    headers: HeaderMap,
    Path((user, repo)): Path<(String, String)>,
    mut stream: BodyStream,
) -> Result<Response<Body>, (StatusCode, String)> {
    // Extract the body to `BatchVars`.
    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buffer.put(chunk.unwrap());
    }
    let mut batch_vars: BatchVars = serde_json::from_slice(buffer.freeze().as_ref()).unwrap();

    let auth = headers.get(AUTHORIZATION);
    let auth = match auth {
        Some(val) => val.to_str().unwrap(),
        None => "",
    };

    let bvo = &mut batch_vars.objects;
    for req in bvo {
        req.user = user.to_string();
        req.repo = repo.to_string();
        req.authorization = auth.to_string();
    }

    let mut response_objects = Vec::<Representation>::new();
    let mut use_tus = false;
    let db = Arc::clone(&state.db_storage);
    let config = Arc::clone(&state.config);
    let config = config.lock().await;

    if batch_vars.operation == "upload" && config.is_using_tus() {
        for tran in batch_vars.transfers {
            if tran == "tus" {
                use_tus = true;
                break;
            }
        }
    }

    let content_store = ContentStore::new(config.content_path.to_owned()).await;
    let db = db.lock().await;
    for object in batch_vars.objects {
        let meta = db.get(&object).await;
        let found = meta.is_some();
        let mut meta = meta.unwrap();
        if found && content_store.exist(&meta).await {
            let conf = Arc::clone(&state.config);
            response_objects.push(represent(&object, &meta, true, false, false, conf).await);
            continue;
        }

        // Not found
        if batch_vars.operation == "upload" {
            meta = db.put(&object).await;
            let conf = Arc::clone(&state.config);
            response_objects.push(represent(&object, &meta, false, true, use_tus, conf).await);
        } else {
            let rep = Representation {
                oid: object.oid.to_owned(),
                size: object.size,
                actions: HashMap::new(),
                error: ObjectError {
                    code: 404,
                    message: "Not found".to_owned(),
                },
            };
            response_objects.push(rep);
        }
    }

    let mut batch_response = BatchResponse {
        transfer: "".to_string(),
        objects: response_objects,
    };

    if use_tus {
        batch_response.transfer = "tus".to_string();
    }

    let json = serde_json::to_string(&batch_response).unwrap();
    let body = Body::from(json);
    let mut resp = Response::builder();
    resp = resp.header("Content-Type", "application/vnd.git-lfs+json");

    let resp = resp.body(body).unwrap();
    Ok(resp)
}

async fn logged_method_router(
    state: State<AppState>,
    headers: HeaderMap,
    Path((user, repo, oid)): Path<(String, String, String)>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let h = headers.get(ACCEPT);
    let h = match h {
        Some(val) => val.to_str().unwrap(),
        None => "",
    };

    let auth = headers.get(AUTHORIZATION);
    let auth = match auth {
        Some(val) => val.to_str().unwrap(),
        None => "",
    };

    let db = Arc::clone(&state.db_storage);
    let config = Arc::clone(&state.config);

    // Load request parameters into struct.
    let request_vars = RequestVars {
        oid: oid.to_owned(),
        size: 0,
        user: user.to_owned(),
        password: "".to_owned(),
        repo: repo.to_owned(),
        authorization: auth.to_owned(),
    };

    if h == "application/vnd.git-lfs" {
        get_content_handler(request_vars, db, config).await
    } else if h == "application/vnd.git-lfs+json" {
        get_meta_handler(request_vars, db, config).await
    } else {
        Err((
            StatusCode::NOT_ACCEPTABLE,
            String::from("Header not acceptable!"),
        ))
    }
}

async fn method_router(headers: HeaderMap, Path(oid): Path<String>) {
    // let h = headers[ACCEPT].to_str().unwrap();

    // if h == "application/vnd.git-lfs" {
    //     get_content_handler().await
    // } else if h == "application/vnd.git-lfs+json" {
    //     get_meta_handler().await
    // } else {
    //     "Error".to_owned()
    // }
}

async fn get_content_handler(
    request_vars: RequestVars,
    db: Arc<Mutex<mysql::meta_storage::MysqlStorage>>,
    config: Arc<Mutex<Configuration>>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let db = db.lock().await;
    let meta = db.get(&request_vars).await.unwrap();

    let content_store = ContentStore::new(config.lock().await.content_path.to_owned()).await;
    let mut file = content_store.get(&meta, 0).await;

    let mut buffer = String::new();
    file.read_to_string(&mut buffer).unwrap();
    let mut bytes = BytesMut::new();
    bytes.put(buffer.as_bytes());
    let resp = Response::builder();
    let body = Body::from(bytes.freeze());
    Ok(resp.body(body).unwrap())
}

async fn get_meta_handler(
    request_vars: RequestVars,
    db: Arc<Mutex<mysql::meta_storage::MysqlStorage>>,
    config: Arc<Mutex<Configuration>>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let db = db.lock().await;
    let meta = db.get(&request_vars).await.unwrap();

    let resp = Response::builder();
    let rep = represent(&request_vars, &meta, true, false, false, config).await;
    let json = serde_json::to_string(&rep).unwrap();
    let body = Body::from(json);

    Ok(resp.body(body).unwrap())
}

async fn put_handler(
    state: State<AppState>,
    headers: HeaderMap,
    Path((user, repo, oid)): Path<(String, String, String)>,
    mut stream: BodyStream,
) -> Result<Response<Body>, (StatusCode, String)> {
    let auth = headers.get(AUTHORIZATION);
    let auth = match auth {
        Some(val) => val.to_str().unwrap(),
        None => "",
    };

    // Load request parameters into struct.
    let request_vars = RequestVars {
        oid: oid.to_owned(),
        size: 0,
        user: user.to_owned(),
        password: "".to_owned(),
        repo: repo.to_owned(),
        authorization: auth.to_owned(),
    };

    let db = Arc::clone(&state.db_storage);
    let config = Arc::clone(&state.config);

    let db = db.lock().await;
    let meta = db.get(&request_vars).await.unwrap();

    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buffer.put(chunk.unwrap());
    }

    let content_store = ContentStore::new(config.lock().await.content_path.to_owned()).await;
    let ok = content_store.put(&meta, buffer.freeze().as_ref()).await;
    if !ok {
        db.delete(&request_vars).await;
        return Err((
            StatusCode::NOT_ACCEPTABLE,
            String::from("Header not acceptable!"),
        ));
    }
    let mut resp = Response::builder();
    resp = resp.header("Content-Type", "application/vnd.git-lfs");
    let resp = resp.body(Body::empty()).unwrap();

    Ok(resp)
}

async fn post_handler(
    state: State<AppState>,
    headers: HeaderMap,
    Path((user, repo)): Path<(String, String)>,
    mut stream: BodyStream,
) -> Result<Response<Body>, (StatusCode, String)> {
    let auth = headers.get(AUTHORIZATION);
    let auth = match auth {
        Some(val) => val.to_str().unwrap(),
        None => "",
    };

    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buffer.put(chunk.unwrap());
    }

    let mut request_vars: RequestVars = serde_json::from_slice(buffer.freeze().as_ref()).unwrap();

    request_vars.user = user.to_string();
    request_vars.repo = repo.to_string();
    request_vars.authorization = auth.to_string();

    let db = Arc::clone(&state.db_storage);
    let config = Arc::clone(&state.config);

    let db = db.lock().await;
    let meta = db.put(&request_vars).await;

    let mut resp = Response::builder();
    resp = resp.header("Content-Type", "application/vnd.git-lfs");
    resp = resp.status(202);

    let content_store = ContentStore::new(config.lock().await.content_path.to_owned()).await;
    if meta.exist && content_store.exist(&meta).await {
        resp = resp.status(200);
    }
    let rep = represent(&request_vars, &meta, meta.exist, true, false, config).await;
    let json = serde_json::to_string(&rep).unwrap();
    let body = Body::from(json);

    let resp = resp.body(body).unwrap();
    Ok(resp)
}

async fn locks_handler() {}

async fn locks_verify_handler() {}

async fn create_lock_handler(
    state: State<AppState>,
    headers: HeaderMap,
    Path((user, repo)): Path<(String, String)>,
    mut stream: BodyStream,
) -> Result<Response<Body>, (StatusCode, String)> {
   	// vars := mux.Vars(r)
	// repo := vars["repo"]
	// user := context.Get(r, "USER").(string)

	// dec := json.NewDecoder(r.Body)
	// enc := json.NewEncoder(w)

	// w.Header().Set("Content-Type", metaMediaType)
    let mut resp = Response::builder();
    resp = resp.header("Content-Type", "application/vnd.git-lfs+json");

    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        buffer.put(chunk.unwrap());
    }

    let mut lock_request: LockRequest = serde_json::from_slice(buffer.freeze().as_ref()).unwrap();
	// var lockRequest LockRequest
	// if err := dec.Decode(&lockRequest); err != nil {
	// 	w.WriteHeader(http.StatusBadRequest)
	// 	enc.Encode(&LockResponse{Message: err.Error()})
	// 	return
	// }


	// locks, _, err := a.metaStore.FilteredLocks(repo, lockRequest.Path, "", "1")
	// if err != nil {
	// 	w.WriteHeader(http.StatusInternalServerError)
	// 	enc.Encode(&LockResponse{Message: err.Error()})
	// 	return
	// }
	// if len(locks) > 0 {
	// 	w.WriteHeader(http.StatusConflict)
	// 	enc.Encode(&LockResponse{Message: "lock already created"})
	// 	return
	// }

	// lock := &Lock{
	// 	Id:       randomLockId(),
	// 	Path:     lockRequest.Path,
	// 	Owner:    User{Name: user},
	// 	LockedAt: time.Now(),
	// }

	// if err := a.metaStore.AddLocks(repo, *lock); err != nil {
	// 	w.WriteHeader(http.StatusInternalServerError)
	// 	enc.Encode(&LockResponse{Message: err.Error()})
	// 	return
	// }

	// w.WriteHeader(http.StatusCreated)
	// enc.Encode(&LockResponse{
	// 	Lock: lock,
	// })

	// logRequest(r, 200)
    Ok(Response::new(Body::empty()))
}

async fn delete_lock_handler() {}

async fn verify_handler() {}

async fn represent(
    rv: &RequestVars,
    meta: &MetaObject,
    download: bool,
    upload: bool,
    use_tus: bool,
    config: Arc<Mutex<Configuration>>,
) -> Representation {
    let mut rep = Representation {
        oid: meta.oid.to_owned(),
        size: meta.size,
        actions: HashMap::new(),
        error: ObjectError {
            code: 0,
            message: "".to_owned(),
        },
    };

    let mut header: HashMap<String, String> = HashMap::new();
    let mut verify_header: HashMap<String, String> = HashMap::new();

    header.insert("Accept".to_string(), "application/vnd.git-lfs".to_owned());

    if rv.authorization.len() > 0 {
        header.insert("Authorization".to_string(), rv.authorization.to_owned());
        verify_header.insert("Authorization".to_string(), rv.authorization.to_owned());
    }

    if download {
        rep.actions.insert(
            "download".to_string(),
            Link {
                href: {
                    let config = Arc::clone(&config);
                    rv.download_link(config).await
                },
                header: header.clone(),
                expires_at: 0 as f64,
            },
        );
    }

    if upload {
        rep.actions.insert(
            "upload".to_string(),
            Link {
                href: {
                    let config = Arc::clone(&config);
                    rv.upload_link(config, use_tus).await
                },
                header: header.clone(),
                expires_at: 0 as f64,
            },
        );
        if use_tus {
            rep.actions.insert(
                "verify".to_string(),
                Link {
                    href: {
                        let config = Arc::clone(&config);
                        rv.verify_link(config).await
                    },
                    header: verify_header.clone(),
                    expires_at: 0 as f64,
                },
            );
        }
    }

    rep
}

#[cfg(test)]
mod tests {
    use std::fs::write;

    use super::*;

    #[tokio::test]
    async fn test_internal_link() {
        let request_var = RequestVars {
            oid: "oid_for_test".to_string(),
            size: 0,
            user: "test_user".to_string(),
            password: "test_password".to_string(),
            repo: "test_repo".to_string(),
            authorization: "test_auth".to_string(),
        };

        // This configuration is ought to be shared across threads asyncrouslly.
        let config = Arc::new(Mutex::new(Configuration {
            listen: "test_listen".to_string(),
            host: "test_host".to_string(),
            ext_origin: "test_ext_origin".to_string(),
            meta_db: "test_meta_db".to_string(),
            content_path: "test_content_path".to_string(),
            admin_user: "test_admin_user".to_string(),
            admin_pass: "test_admin_pass".to_string(),
            cert: "test_cert".to_string(),
            key: "test_key".to_string(),
            scheme: "test_scheme".to_string(),
            public: "test_public".to_string(),
            use_tus: "test_use_tus".to_string(),
            tus_host: "test_tus_host".to_string(),
        }));

        let res = request_var
            .internal_link("test_subpath".to_string(), Arc::clone(&config))
            .await;
        println!("{:?}", res);

        let res = request_var.upload_link(Arc::clone(&config), false).await;
        println!("{:?}", res);

        let res = request_var.verify_link(Arc::clone(&config)).await;
        println!("{:?}", res);
    }

    #[tokio::test]
    async fn test_json() {
        let request_var = RequestVars {
            oid: "oid_for_test".to_string(),
            size: 0,
            user: "test_user".to_string(),
            password: "test_password".to_string(),
            repo: "test_repo".to_string(),
            authorization: "test_auth".to_string(),
        };

        let json = serde_json::to_string(&request_var).unwrap();
        write("jsonfile.txt", json).unwrap();
    }
}
