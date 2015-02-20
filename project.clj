(defproject s3-to-redshift "0.1.0-SNAPSHOT"
  :description "Load from S3 to Redshift"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/tools.cli "0.3.1"]
                 [amazonica "0.3.14" :exclusions [com.fasterxml.jackson.core/jackson-core]]
                 [org.slf4j/jcl-over-slf4j "1.7.7"]
                 [cheshire "5.4.0"]
                 [com.vgeshel/korma "0.3.0-RC5-vg4"]
                 [postgresql/postgresql "9.0-801.jdbc4"]
                 [slingshot "0.10.3"]
                 [clj-time "0.7.0"]
                 [prismatic/schema "0.3.7"]
                 [com.taoensso/timbre "3.4.0"]])
