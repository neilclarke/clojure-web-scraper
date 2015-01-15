(defproject eskom-scraper "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [enlive "1.1.5"]
                 [cheshire "5.4.0"]
                 [clj-time "0.8.0"]]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :aot [eskom-scraper.core]
  :main eskom-scraper.core)
