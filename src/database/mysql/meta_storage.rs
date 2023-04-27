use crate::database::entity::meta::{self};
use crate::database::entity::locks::{self};
use crate::server::{Lock, MetaObject, RequestVars, User};
use sea_orm::ActiveValue::NotSet;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseBackend, DatabaseConnection, DbErr, EntityTrait,
    InsertResult, QueryFilter, Set, Statement,
};
use std::path;
use serde::{Deserialize, Serialize};
use std::time::{UNIX_EPOCH, SystemTime};


#[derive(Debug, Default, Clone)]
pub struct MysqlStorage {
    pub connection: DatabaseConnection,
}

impl MysqlStorage {
    pub fn new(connection: DatabaseConnection) -> MysqlStorage {
        MysqlStorage { connection }
    }

    pub async fn get(&self, v: &RequestVars) -> Option<MetaObject> {
        let result = meta::Entity::find_by_id(v.oid.clone())
            .one(&self.connection)
            .await
            .unwrap();

        match result {
            Some(val) => Some(MetaObject {
                oid: val.oid,
                size: val.size,
                exist: val.exist,
            }),
            None => None,
        }
    }

    pub async fn put(&self, v: &RequestVars) -> MetaObject {
        // Check if already exist.
        let result = meta::Entity::find_by_id(v.oid.clone())
            .one(&self.connection)
            .await
            .unwrap();
        if result.is_some() {
            let result = result.unwrap();
            return MetaObject {
                oid: result.oid,
                size: result.size,
                exist: true,
            };
        }

        // Put into database if not exist.
        let meta = MetaObject {
            oid: v.oid.to_string(),
            size: v.size,
            exist: false,
        };

        let meta_to = meta::ActiveModel {
            oid: Set(meta.oid.to_owned()),
            size: Set(meta.size.to_owned()),
            exist: Set(false),
        };

        let res = meta::Entity::insert(meta_to).exec(&self.connection).await;
        assert!(res.is_ok());

        meta
    }

    pub async fn delete(&self, v: &RequestVars) -> bool {
        let res = meta::Entity::delete_by_id(v.oid.to_owned())
            .exec(&self.connection)
            .await;
        if res.is_ok() {
            true
        } else {
            false
        }
    }

    pub async fn locks(&self, repo: &String) -> Vec<Lock> {
        let result = locks::Entity::find_by_id(repo.to_owned())
            .one(&self.connection)
            .await
            .unwrap();

        let data = match result {
            Some(val) => val.data.to_owned(),
            None => "".to_string(),
        };
        
        let locks: Vec<Lock> = serde_json::from_str(&data).unwrap();

        locks
    }

    pub async fn add_locks(&self, repo: &String, locks: Vec<Lock>) -> bool {
        let result = locks::Entity::find_by_id(repo.to_owned())
            .one(&self.connection)
            .await
            .unwrap();

        match result {
            // Update
            Some(val) => {
                let d = val.data.to_owned();
                let mut locks_from_data = if d != "" {
                    let locks_from_data: Vec<Lock> = serde_json::from_str(&d).unwrap();
                    locks_from_data
                } else {
                    vec![]
                };
                let mut locks = locks;
                locks_from_data.append(&mut locks);
                let d = serde_json::to_string(&locks_from_data).unwrap();

                let mut lock_to: locks::ActiveModel = val.into();
                lock_to.data = Set(d.to_owned());
                let res = lock_to.update(&self.connection).await;
                res.is_ok()
            },
            // Insert
            None => {
                let data = serde_json::to_string(&locks).unwrap();
                let lock_to = locks::ActiveModel {
                    id: Set(repo.to_owned()),
                    data: Set(data.to_owned()),
                };
                let res = locks::Entity::insert(lock_to).exec(&self.connection).await;
                res.is_ok()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::mysql;

    // #[tokio::test]
    // async fn test_get() {
    //     let db = mysql::init().await;

    //     let request_vars = RequestVars {
    //         oid: "oid_for_test".to_string(),
    //         size: 77,
    //         user: "test_user".to_string(),
    //         password: "test_password".to_string(),
    //         repo: "test_repo".to_string(),
    //         authorization: "test_auth".to_string(),
    //     };
    //     let select_res = db.get(&request_vars).await;
    //     println!("{:?}", select_res);
    // }

    // #[tokio::test]
    // async fn test_put() {
    //     let db = mysql::init().await;

    //     let request_vars = RequestVars {
    //         oid: "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72".to_string(),
    //         size: 12,
    //         user: "test_user".to_string(),
    //         password: "test_password".to_string(),
    //         repo: "test_repo".to_string(),
    //         authorization: "test_auth".to_string(),
    //     };
    //     let insert_res = db.put(&request_vars).await;
    //     println!("{:?}", insert_res);
    // }

    // #[tokio::test]
    // async fn test_delete() {
    //     let db = mysql::init().await;

    //     let request_vars = RequestVars {
    //         oid: "oid_for_test".to_string(),
    //         size: 77,
    //         user: "test_user".to_string(),
    //         password: "test_password".to_string(),
    //         repo: "test_repo".to_string(),
    //         authorization: "test_auth".to_string(),
    //     };
    //     let delete_res = db.delete(&request_vars).await;
    //     println!("{:?}", delete_res);
    // }

    fn new_test_lock(repo: String, path: String, user: String) -> Lock {
        Lock {
            id: repo.to_owned(),
            path: path.to_owned(),
            owner: User {
                name: user.to_owned(),
            },
            locked_at: {
                let now = SystemTime::now();
                let res = now.duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
                res
            },
        }
    }

    // #[tokio::test]
    // async fn test_locks() {
    //     let test_locks = vec![
    //         new_test_lock("test1".to_string(), "test_path1".to_string(), "test1_user".to_string()),
    //         new_test_lock("test2".to_string(), "test_path2".to_string(), "test2_user".to_string()),
    //         new_test_lock("test3".to_string(), "test_path3".to_string(), "test3_user".to_string()),
    //     ];

    //     let db = mysql::init().await;

    //     db.add_locks(&"test_repo".to_string(), test_locks).await;
    // }

    #[tokio::test]
    async fn test_get_locks() {
        let db = mysql::init().await;
        let locks = db.locks(&"test_repo".to_string()).await;

        println!("{:?}", locks);
    }
}
