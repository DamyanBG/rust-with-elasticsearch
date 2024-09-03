use elasticsearch::{ auth::Credentials, http::{ request::{Body, JsonBody, NdBody}, response::Response, transport::Transport }, indices::{ IndicesCreateParts, IndicesExistsParts }, Elasticsearch, IndexParts, SearchParts };
use std::error::Error;
use serde_json::{ json, Value };
use serde::{ Deserialize, Serialize };
use dotenv::dotenv;
use std::env;

struct Config {
    api_key: String,
    api_key_id: String,
    cloud_id: String,
}

struct ElSearch {
    client: Elasticsearch,
}

impl ElSearch {
    fn new_from_localhost(host: &str) -> Self {
        let transport = Transport::single_node(host).unwrap();
        let es_client = Elasticsearch::new(transport);
        ElSearch {
            client: es_client
        }
    }

    fn new_from_cloudhost(config: &Config) -> Self {
        let api_key = &config.api_key;
        let api_key_id = &config.api_key_id;
        let cloud_id = &config.cloud_id;

        let credentials = Credentials::ApiKey(api_key_id.to_string(), api_key.to_string());
        let transport = Transport::cloud(cloud_id, credentials).unwrap();

        let es_client = Elasticsearch::new(transport);

        ElSearch {
            client: es_client
        }
    }

    async fn search(&self, index_name: &str, body: &Value) -> Result<Response, Box<dyn Error>> {
        let response = self.client
            .search(SearchParts::Index(&[index_name]))
            .body(body)
            .send()
            .await?;
        Ok(response)
    }

    async fn add_document(&self, index_name: &str, body: &Value) -> Result<Response, Box<dyn Error>> {
        let response = self.client
            .index(IndexParts::Index(index_name))
            .body(body)
            .send()
            .await?;
        Ok(response)
    }

    async fn check_index_exists(&self, index_name: &str) -> Result<Response, Box<dyn Error>> {
        let response = self.client
            .indices()
            .exists(IndicesExistsParts::Index(&[index_name]))
            .send()
            .await?;
        Ok(response)
    }

    async fn create_index(&self, index_name: &str, body: &Value) -> Result<Response, Box<dyn Error>> {
        let response: Response = self.client
            .indices()
            .create(IndicesCreateParts::Index(index_name))
            .body(body)
            .send()
            .await?;
        Ok(response)
    }

    async fn bulk_create_by_index(&self, index_name: &str, operations: Vec<Value>) -> Result<Response, Box<dyn Error>> {
        let mut bulk_body = Vec::<JsonBody::<Value>>::new();

        for operation in operations {
            let jsonbody = JsonBody::new(operation);
            let create_instruction = json!({
                "create": {}
            });
            let create_instr_jsonbody = JsonBody::new(create_instruction);
            bulk_body.push(create_instr_jsonbody);
            bulk_body.push(jsonbody);
        }

        let response = self.client
            .bulk(elasticsearch::BulkParts::Index(index_name))
            .body(bulk_body)
            .send()
            .await?;

        Ok(response)
    }
    
}
 
fn get_product_mapping() -> Value {
    json!({
        "mappings": {
            "properties": {
                "name": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "description": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "category": {
                    "type": "keyword"
                },
                "brand": {
                    "type": "keyword"
                },
                "price": {
                    "type": "float"
                },
                "rating": {
                    "type": "float"
                }
            }
        }
    })
}

fn create_client() -> Result<Elasticsearch, Box<dyn Error>> {
    let transport = Transport::single_node("http://localhost:9200")?;
    Ok(Elasticsearch::new(transport))
}

async fn search(client: &Elasticsearch, index_name: &str, body: &Value) -> Result<Response, Box<dyn Error>> {
    let response = client
        .search(SearchParts::Index(&[index_name]))
        .body(body)
        .send()
        .await?;
    Ok(response)
}

async fn create_index(index_name: &str, client: &Elasticsearch, body: &Value) -> Result<Response, Box<dyn Error>> {
    let response: Response = client
        .indices()
        .create(IndicesCreateParts::Index(index_name))
        .body(body)
        .send()
        .await?;
    Ok(response)
}

async fn check_index_exists(index_name: &str, client: &Elasticsearch) -> Result<Response, Box<dyn Error>> {
    let response = client
        .indices()
        .exists(IndicesExistsParts::Index(&[index_name]))
        .send()
        .await?;
    Ok(response)
}

async fn add_document(client: &Elasticsearch, index_name: &str, body: &Value) -> Result<Response, Box<dyn Error>> {
    let response = client
        .index(IndexParts::Index(index_name))
        .body(body)
        .send()
        .await?;
    Ok(response)
}

#[derive(Debug, Serialize, Deserialize)]
struct Product {
    brand: String,
    category: String,
    description: String,
    name: String,
    price: f64,
    rating: f64,
}

fn generate_product_data() -> Vec<Value> {
    vec![
        json!({
            "name": "Smartphone",
            "description": "A smartphone with a high-resolution screen.",
            "category": "Electronics",
            "brand": "TechBrand",
            "price": 699.99,
            "rating": 4.5
        }),
        json!({
            "name": "Laptop",
            "description": "A powerful laptop for professionals.",
            "category": "Computers",
            "brand": "CompTech",
            "price": 1299.99,
            "rating": 4.7
        }),
        json!({
            "name": "Headphones",
            "description": "Noise-cancelling over-ear headphones.",
            "category": "Audio",
            "brand": "SoundMax",
            "price": 199.99,
            "rating": 4.3
        }),
        json!({
            "name": "Smartwatch",
            "description": "A stylish smartwatch with fitness tracking.",
            "category": "Wearables",
            "brand": "WristTech",
            "price": 299.99,
            "rating": 4.2
        }),
        json!({
            "name": "Tablet",
            "description": "A lightweight tablet with a 10-inch display.",
            "category": "Tablets",
            "brand": "TabBrand",
            "price": 499.99,
            "rating": 4.4
        }),
        json!({
            "name": "Gaming Console",
            "description": "A next-gen gaming console with 4K resolution.",
            "category": "Gaming",
            "brand": "GameBox",
            "price": 499.99,
            "rating": 4.8
        }),
        json!({
            "name": "Wireless Speaker",
            "description": "A portable wireless speaker with deep bass.",
            "category": "Audio",
            "brand": "SoundWave",
            "price": 149.99,
            "rating": 4.6
        })
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let cloud_id = env::var("CLOUD_ID")?;
    let api_key = env::var("API_KEY")?;
    let api_key_id = env::var("API_KEY_ID")?;

    let config = Config {
        api_key,
        api_key_id,
        cloud_id
    };

    let product_index_name = "products";

    let es = ElSearch::new_from_cloudhost(&config);
    
    let is_existing_resp = es.check_index_exists(product_index_name).await?;
    let index_exists = is_existing_resp.status_code().is_success();

    if index_exists {
        println!("Index {} exists!", product_index_name);
    } else {
        println!("Index {} does not exists! Proceed with creating", product_index_name);
        let product_mapping = get_product_mapping();
        let create_resp = es.create_index(product_index_name,  &product_mapping).await?;

        if create_resp.status_code().is_success() {
            println!("Index created successfully.");
        } else {
            println!("Failed to create index: {:?}", create_resp.text().await?);
        }
    }

    // let product_doc = json!({
    //     "name": "UltraSoft Sonic Toothbrush",
    //     "description": "A high-frequency sonic toothbrush designed for gentle yet effective cleaning, featuring multiple brushing modes and a long-lasting battery.",
    //     "category": "Health & Personal Care",
    //     "brand": "SonicCare",
    //     "price": 99.99,
    //     "rating": 4.7
    // });

    // let add_resp: Response = es.add_document(product_index_name, &product_doc).await?;

    // println!("{:?}", add_resp);


    // // Search code
    // let query = json!(
    //     {
    //         "query": {
    //             "multi_match": {
    //             "query": "toothbrush",
    //             "fields": ["name", "description"]
    //             }
    //         }
    //     }
    // );

    // let search_resp = es.search(product_index_name, &query).await?;

    // let resp_body = search_resp.json::<Value>().await?;

    // for hit in resp_body["hits"]["hits"].as_array().unwrap() {
    //     let product: Product = serde_json::from_value(hit["_source"].clone())?;
    //     println!("{:?}", product);
    // }

    // Bulk operation code
    let products = generate_product_data();

    let bulk_resp = es.bulk_create_by_index(product_index_name, products).await?;

    let bulk_resp_body  = bulk_resp.json::<Value>().await?;

    println!("{}", bulk_resp_body);


    Ok(())
}
