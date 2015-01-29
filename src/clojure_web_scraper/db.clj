(ns clojure-web-scraper.db
  (:require [clojure.java.jdbc :as j]
            [clojure.java.io :as io]))

(def db 
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname "resources/eskom.db"
   })

(j/query db
  ["select * from suburb where name = 'Bakensklip'"])

;(def import-file
;  (take 3000
;        (drop 24055 (line-seq (io/reader (io/resource "insert_suburbs.txt"))))))

;(for [line import-file]
;  (j/execute! db [line]))

;import-file

;(j/execute! db ["insert into suburb (suburb_id, name, tot_id, municipality_id) values(2968,\"Belmont\",165,71);"])