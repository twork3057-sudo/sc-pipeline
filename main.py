import json, logging, hashlib, os, requests
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import psycopg2
from psycopg2 import pool
import redis

# ---------- Helpers ----------
def now_iso(): return datetime.now(timezone.utc).isoformat()
def sha(d): return hashlib.sha256(json.dumps(d, sort_keys=True, separators=(",",":")).encode()).hexdigest()

REQUIRED = ["event_id","event_type","event_ts","source_system","payload"]

class ParseDQ(beam.DoFn):
    def __init__(self, domain): self.domain = domain
    def process(self, b):
        ingest = now_iso()
        try:
            raw = json.loads(b.decode("utf-8"))
        except Exception as ex:
            yield beam.pvalue.TaggedOutput("dlq", {"reason":"json_parse","error":str(ex),"ingest_ts":ingest,"envelope":None})
            return
        errs=[]
        for f in REQUIRED:
            if f not in raw: errs.append(f"missing:{f}")
        if "payload" in raw and not isinstance(raw["payload"], dict):
            errs.append("payload_not_object")
        if errs:
            yield beam.pvalue.TaggedOutput("dlq", {"reason":"dq_fail","error":",".join(errs),"ingest_ts":ingest,"envelope":raw})
            return
        out = {
            "domain": self.domain,
            "event_id": raw["event_id"], "event_type": raw["event_type"],
            "event_ts": raw["event_ts"], "source_system": raw["source_system"],
            "schema_version": raw.get("schema_version","v1"),
            "ingest_ts": ingest, "dq_status":"PASS", "dq_errors":"",
            "payload": raw["payload"]
        }
        yield out

# Improved Redis connection handling and error recovery
class ResolvePK(beam.DoFn):
    def __init__(self, redis_host, id_service_url): 
        self.redis_host = redis_host
        self.id_service_url = id_service_url
        self.redis_client = None
        
    def setup(self):
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host, 
                port=6379, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test connection
            self.redis_client.ping()
        except Exception as e:
            logging.warning(f"Redis connection failed: {e}. Will fall back to direct ID service calls.")
            self.redis_client = None
    
    def _key(self, domain, src_sys, src_id): 
        return f"{domain}:{src_sys}:{src_id}"
    
    def _mint(self, domain, src_sys, src_id):
        try:
            r = requests.post(
                self.id_service_url, 
                json={"domain":domain,"source_system":src_sys,"source_id":src_id}, 
                timeout=10,
                headers={'Authorization': f'Bearer {self._get_token()}'}  
# Added auth
            )
            r.raise_for_status()
            return r.json()["pk"]
        except Exception as e:
            logging.error(f"ID service call failed: {e}")
            raise
    
    def _get_token(self):
        # FIXED: Get proper service account token for authentication
        from google.auth import default
        from google.auth.transport.requests import Request
        
        credentials, _ = default()
        credentials.refresh(Request())
        return credentials.token
    
    def process(self, row):
        d = row["domain"]
        src_sys = row["source_system"]
        src_id = row["payload"].get("source_id") or row["payload"].get("code") or row["payload"].get("id")
        
        if not src_id:
            yield beam.pvalue.TaggedOutput("dlq", {**row, "dq_status":"FAIL","dq_errors":"missing source_id"})
            return
            
        k = self._key(d, src_sys, str(src_id).strip())
        pk = None
        
        # Try Redis cache first
        if self.redis_client:
            try:
                pk = self.redis_client.get(k)
            except Exception as e:
                logging.warning(f"Redis lookup failed: {e}")
        
        # If not in cache, mint new ID
        if not pk:
            try:
                pk = self._mint(d, src_sys, src_id)
                # Cache the result
                if self.redis_client:
                    try:
                        self.redis_client.set(k, pk, ex=86400)
                    except Exception as e:
                        logging.warning(f"Redis cache set failed: {e}")
            except Exception as e:
                yield beam.pvalue.TaggedOutput("dlq", {**row, "dq_status":"FAIL","dq_errors":f"id_resolution_failed:{str(e)}"})
                return
        
        # Attach enterprise key to row
        if d == "supplier":   row["supplier_pk"] = pk
        elif d == "material": row["material_pk"] = pk
        elif d == "plant":    row["location_pk"] = pk
        
        yield row

# Improved AlloyDB connection pooling and error handling
class UpsertAlloyDB(beam.DoFn):
    def __init__(self, host, port, user, password, db):
        self.cfg = dict(host=host, port=int(port), user=user, password=password, dbname=db)
        self.pool = None
        
    def setup(self):
        try:
            # Used connection pooling
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                1, 5,  # min and max connections
                **self.cfg
            )
            logging.info("AlloyDB connection pool created successfully")
        except Exception as e:
            logging.error(f"Failed to create AlloyDB connection pool: {e}")
            raise
            
        self.sqls = {
            "supplier": """
                INSERT INTO supplier_rm (supplier_pk, canonical_name, tax_id, country, risk_score, is_active, record_hash, last_update_ts)
                VALUES (%(supplier_pk)s, %(canonical_name)s, %(tax_id)s, %(country)s, %(risk_score)s, %(is_active)s, %(record_hash)s, now())
                ON CONFLICT (supplier_pk) DO UPDATE SET
                  canonical_name=EXCLUDED.canonical_name, tax_id=EXCLUDED.tax_id, country=EXCLUDED.country,
                  risk_score=EXCLUDED.risk_score, is_active=EXCLUDED.is_active, record_hash=EXCLUDED.record_hash,
                  last_update_ts=now()
                WHERE supplier_rm.record_hash <> EXCLUDED.record_hash
            """,
            "material": """
                INSERT INTO material_rm (material_pk, material_number, canonical_desc, base_uom, product_hierarchy, abc_class, record_hash, last_update_ts)
                VALUES (%(material_pk)s, %(material_number)s, %(canonical_desc)s, %(base_uom)s, %(product_hierarchy)s, %(abc_class)s, %(record_hash)s, now())
                ON CONFLICT (material_pk) DO UPDATE SET
                  material_number=EXCLUDED.material_number, canonical_desc=EXCLUDED.canonical_desc, base_uom=EXCLUDED.base_uom,
                  product_hierarchy=EXCLUDED.product_hierarchy, abc_class=EXCLUDED.abc_class, record_hash=EXCLUDED.record_hash,
                  last_update_ts=now()
                WHERE material_rm.record_hash <> EXCLUDED.record_hash
            """,
            "plant": """
                INSERT INTO plant_rm (location_pk, plant_code, name, region, timezone, record_hash, last_update_ts)
                VALUES (%(location_pk)s, %(plant_code)s, %(name)s, %(region)s, %(timezone)s, %(record_hash)s, now())
                ON CONFLICT (location_pk) DO UPDATE SET
                  plant_code=EXCLUDED.plant_code, name=EXCLUDED.name, region=EXCLUDED.region, timezone=EXCLUDED.timezone,
                  record_hash=EXCLUDED.record_hash, last_update_ts=now()
                WHERE plant_rm.record_hash <> EXCLUDED.record_hash
            """
        }
    
    def _map(self, row):
        d = row["domain"]
        p = row["payload"]
        if d == "supplier":
            rec = {
              "supplier_pk": row["supplier_pk"],
              "canonical_name": p.get("name"), 
              "tax_id": p.get("tax_id"),
              "country": p.get("country"), 
              "risk_score": p.get("risk_score","Unknown"),
              "is_active": bool(p.get("is_active", True))
            }
            rec["record_hash"] = sha(rec)
            return d, rec
        elif d == "material":
            rec = {
              "material_pk": row["material_pk"], 
              "material_number": p.get("material_number"),
              "canonical_desc": p.get("canonical_desc") or p.get("description"),
              "base_uom": p.get("base_uom"), 
              "product_hierarchy": p.get("product_hierarchy"),
              "abc_class": p.get("abc_class")
            }
            rec["record_hash"] = sha(rec)
            return d, rec
        elif d == "plant":
            rec = {
              "location_pk": row["location_pk"], 
              "plant_code": p.get("plant_code"),
              "name": p.get("name"), 
              "region": p.get("region"), 
              "timezone": p.get("timezone")
            }
            rec["record_hash"] = sha(rec)
            return d, rec
    
    def process(self, row):
        if not self.pool:
            logging.error("No database connection pool available")
            return
            
        conn = None
        try:
            d, rec = self._map(row)
            conn = self.pool.getconn()
            with conn.cursor() as cur:
                cur.execute(self.sqls[d], rec)
                conn.commit()
            yield rec
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"AlloyDB upsert failed: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)
    
    def teardown(self):
        if self.pool:
            self.pool.closeall()

# Rest of the run() function but added some improved error handling, might 
# need to add more.
def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--supplier_sub", required=True)
    parser.add_argument("--material_sub", required=True)
    parser.add_argument("--plant_sub", required=True)
    parser.add_argument("--redis_host", required=True)
    parser.add_argument("--id_service_url", required=True)
    parser.add_argument("--bq_supplier_bronze", required=True)
    parser.add_argument("--bq_material_bronze", required=True)
    parser.add_argument("--bq_plant_bronze", required=True)
    parser.add_argument("--bq_supplier_dlq", required=True)
    parser.add_argument("--bq_material_dlq", required=True)
    parser.add_argument("--bq_plant_dlq", required=True)
    parser.add_argument("--gcs_temp", required=True)
    parser.add_argument("--adb_host", required=True)
    parser.add_argument("--adb_port", default="5432")
    parser.add_argument("--adb_user", required=True)
    parser.add_argument("--adb_password", required=True)
    parser.add_argument("--adb_db", default="supplychain")
    args, beam_args = parser.parse_known_args()

    opts = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    opts.view_as(SetupOptions).save_main_session=True
    opts.view_as(StandardOptions).streaming=True

    def domain_branch(pcoll, domain, bronze_table, dlq_table):
        parsed = pcoll | f"{domain}:ParseDQ" >> beam.ParDo(ParseDQ(domain)).with_outputs("dlq", main="ok")
        ok, dlq = parsed.ok, parsed.dlq

        resolved = ok | f"{domain}:ResolvePK" >> beam.ParDo(ResolvePK(args.redis_host, args.id_service_url)).with_outputs("dlq", main="ok")
        good, bad = resolved.ok, resolved.dlq

        _ = good | f"{domain}:WriteBronze" >> WriteToBigQuery(
            table=bronze_table, custom_gcs_temp_location=args.gcs_temp,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        _ = good | f"{domain}:UpsertAlloyDB" >> beam.ParDo(
            UpsertAlloyDB(args.adb_host, args.adb_port, args.adb_user, args.adb_password, args.adb_db)
        )

        dlq_all = ((dlq, bad) | f"{domain}:FlattenDLQ" >> beam.Flatten())
        _ = dlq_all | f"{domain}:WriteDLQ" >> WriteToBigQuery(
            table=dlq_table, custom_gcs_temp_location=args.gcs_temp,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

    with beam.Pipeline(options=opts) as p:
        sup = p | "ReadSupplier" >> ReadFromPubSub(subscription=args.supplier_sub)
        mat = p | "ReadMaterial" >> ReadFromPubSub(subscription=args.material_sub)
        pla = p | "ReadPlant"    >> ReadFromPubSub(subscription=args.plant_sub)

        domain_branch(sup, "supplier", args.bq_supplier_bronze, args.bq_supplier_dlq)
        domain_branch(mat, "material", args.bq_material_bronze, args.bq_material_dlq)
        domain_branch(pla, "plant",    args.bq_plant_bronze,    args.bq_plant_dlq)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
