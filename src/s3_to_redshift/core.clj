(ns s3-to-redshift.core
  (:require [taoensso.timbre :as log]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.edn :as edn]
            [korma
             [core :as k]
             [db :as db]]
            [amazonica.core :as aws]
            [amazonica.aws
             [s3 :as s3]]
            [cheshire.core :as json]
            [schema.core :as schema :refer [defschema Str Int Bool optional-key]])
  (:import [java.util UUID])
  (:gen-class))

;; create table s3_loaded_file(table_name varchar, url varchar(2048), create_time timestamp default sysdate, primary key (table_name, url));
(defn- creds->redshift
  "return credentials in a format usable by the redshift COPY command"
  ([]
   (creds->redshift nil))
  ([creds]
   (let [{:keys [AWSAccessKeyId AWSSecretKey sessionToken]}
         (-> creds aws/get-credentials .getCredentials bean)]
     (cond->
      (format "aws_access_key_id=%s;aws_secret_access_key=%s" AWSAccessKeyId AWSSecretKey)

      sessionToken
      (str ";token=" sessionToken)))))

(defn- ->s3-url [bucket path]
  (str "s3://" bucket "/" (s/replace path #"^/+" "")))

(def initial-state {})

(defschema Config
  {;; defaults to environment credentials
   (optional-key :credentials) {:access-key Str :secret-key Str}
   ;; S3 settings
   :bucket Str
   :prefix Str
   :filter-re Str
   (optional-key :manifest-bucket) Str
   (optional-key :manifest-prefix) Str
   ;; redshift settings
   :seen-table Str
   :redshift {:host Str :port Int :user Str :password Str :db Str}
   :redshift-table Str
   (optional-key :redshift-options) (schema/either Str [Str])
   :max-files-per-manifest Int})

(defschema State
  {(optional-key :seen-files) #{Str}
   (optional-key :objects) [{:key Str schema/Keyword schema/Any}]
   (optional-key :manifests) [{:entries [{:url Str :mandatory Bool}]}]
   (optional-key :manifest-urls) [Str]})

(schema/defn ^:always-validate seen-file-set :- State [state :- State {:keys [seen-table redshift redshift-table]} :- Config]
  (db/with-db (db/postgres redshift)
    (assoc state
      :seen-files
      (->> (k/select
            seen-table
            (k/fields :url)
            (k/where {:table_name redshift-table}))
           (map :url)
           set))))

(schema/defn ^:always-validate find-matching-files :- State
  [{:keys [seen-files] :as state} :- State
   {:keys [bucket prefix filter-re credentials]} :- Config]
  (log/infof "starting find-matching-files")
  (let [opts {:bucket-name bucket :prefix prefix}
        credentials (or credentials {:endpoint nil})
        filter-re (when filter-re
                    (re-pattern filter-re))
        objects (loop [marker   nil
                       results  nil]
                  (let [{:keys [next-marker truncated? object-summaries]}
                        (s3/list-objects credentials (assoc opts :marker marker))
                        objects (cond->>
                                 object-summaries

                                 filter-re
                                 (filter (comp (partial re-matches filter-re)
                                               (partial ->s3-url bucket)
                                               :key))

                                 seen-files
                                 (remove (comp seen-files
                                               (partial ->s3-url bucket)
                                               :key)))]
                    (if (not truncated?)
                      (concat results objects)
                      (recur next-marker (concat results objects)))))]
    (log/infof "found %d files" (count objects))
    (assoc state
      :objects objects)))

(schema/defn ^:always-validate make-manifests :- State [{:keys [objects] :as state} :- State {:keys [bucket max-files-per-manifest]} :- Config]
  (let [batches (partition-all max-files-per-manifest objects)]
    (assoc state
      :manifests (for [objs batches]
                   {:entries
                    (->> objs
                         (map :key)
                         (map (partial str "s3://" bucket "/"))
                         (map #(hash-map :url % :mandatory true)))}))))

(schema/defn ^:always-validate store-manifests! :- State
  [{:keys [manifests] :as state} :- State
   {:keys [credentials
           manifest-bucket manifest-prefix
           bucket prefix]} :- Config]
  {:post [(= (count manifests) (count (:manifest-urls %)))]}
  (assoc state
    :manifest-urls (doall
                    (for [manifest manifests]
                      (let [manifest-bucket (or manifest-bucket bucket)
                            manifest-prefix (or manifest-prefix prefix)
                            ^String manifest-str (json/generate-string manifest)
                            manifest-is (io/input-stream (.getBytes manifest-str "utf-8"))
                            manifest-key (str (s/replace manifest-prefix #"/+$" "") "/" "manifest-" (UUID/randomUUID))
                            manifest-url (->s3-url manifest-bucket manifest-key)]
                        (s3/put-object
                         (or credentials {:endpoint nil})
                         :bucket-name manifest-bucket
                         :key manifest-key
                         :input-stream manifest-is)
                        (log/infof "created manifest %s with %d files in it" manifest-url (count (:entries manifest)))
                        manifest-url)))))

(schema/defn ^:always-validate run-copy! :- State
  [{:keys [manifest-urls manifests] :as state} :- State
   {:keys [redshift credentials redshift-table redshift-options seen-table]} :- Config]
  {:pre [(= (count manifest-urls) (count manifests))]}
  (reduce
   (fn [state [manifest-url manifest]]
     (let [redshift-options (cond
                             (string? redshift-options) redshift-options
                             (coll? redshift-options) (s/join " " redshift-options)
                             (nil? redshift-options) "")]
       (db/with-db (db/postgres redshift)
         (db/transaction
          (log/infof "copying %d files using manifest %s" (count (:entries manifest)) manifest-url)

          (k/exec-raw
           (format "COPY %s FROM '%s' CREDENTIALS '%s' MANIFEST %s"
                   redshift-table
                   manifest-url
                   (creds->redshift credentials)
                   redshift-options))

          (log/infof "done with manifest %s, updating the status table" manifest-url)

          (k/insert
           seen-table
           (k/values (->> (:entries manifest)
                          (map #(hash-map :url (:url %) :table_name redshift-table)))))))
       state))
   state
   (zipmap manifest-urls manifests)))

(defmacro config->
  "thread the state through forms while also passing config as the second arg"
  [config x & forms]
  `(-> ~x
       ~@(map #(if (list? %)
                 (concat % [config])
                 (list % config))
              forms)))

(defn run-with-config [config]
  (let [config (if (string? config)
                 (edn/read-string (slurp config))
                 config)
        _ (schema/validate Config config)
        state (config-> config initial-state
                        seen-file-set
                        find-matching-files
                        make-manifests
                        store-manifests!
                        run-copy!)]
    state))

(defn -main [config & args]
  (when-not config
    (throw (IllegalArgumentException. "Please specify a config file")))
  (run-with-config config))
