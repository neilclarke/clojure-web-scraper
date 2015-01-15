(ns eskom-scraper.core
  (:require [net.cgrand.enlive-html :as html])
  (:require [cheshire.core :as cheshire])
  (:require [clj-time.core :as t])
  (:require [clj-time.format :as f])
  (:require [clj-time.local :as l]))

(def base-url "http://loadshedding.eskom.co.za/LoadShedding/")
(def municipalities-path "GetMunicipalities/?Id=")
(def suburbs-path "GetSurburbData/?pageSize=1000&pageNum=1&searchTerm=&id=")
(def loadshed-day-date-format "dd MMM YYYY")

(def province-names
  {1 "Eastern Cape" 2 "Free State" 3 "Gauteng" 4 "KwaZulu-Natal" 5 "Limpopo" 6 "Mpumalanga" 7 "North West" 8 "Northern Cape" 9 "Western Cape"})

; change this to hitting the web when it's ready
(def sched-source
  (slurp "/home/neil/personal/workspaces/clojure/eskom-scraper/resources/bakensklip-schedule.html"))

(def schedules
  #{0 453 2030 586 410 2475 443 249 2156 70 218 648 774 580 1230 164 282 468 756
    830 154 454 770 2226 397 490 763 269 165 1980 615 1590 415 239 77 405 897 776
    1968 267 319 329 819 464 307 290 1144 3852 2145 234 1470 242 394 159 540 495
    407 2970 825 1476 483 174 331 284 708 256 657 785 954 656 241 314 420 4293
    1431 304 401 600 810 1000 108 156 308 365 1386 500 168 3021 1815 292 216 498
    375 1650 640 247 328 391 990 474 528 303 1232 522 162 426 477 351 243 131 413
    800 369 258 250 616 1155 2385 538 411 1540 323 248 3300 494 2805 261 1908 1725
    489 166 447 252 325 146 228 125 312 1148 663 332 330 544 642 435 1743 484 984
    625 870 1272 1100 566 82 492 926 452 339 431 462 546 1749 289 730 699 1076 1113
    732 324 617 513 705 83 455 449 45 78 480 123 807 795 441 320 288 81 636 423 574
    87 160 696 738 486 336 660 272 114 1236 628 655 318 1092 579 1078 861 960 379
    4290 1479 246 2304 820 390 84})

(defn -fetch-url [url]
  (with-open [inputstream (-> (java.net.URL. url)
                            .openConnection
                            (doto (.setRequestProperty "User-Agent" "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0"))
                            .getContent)]
    (html/html-resource inputstream)))

(defn -read-json[url]
  (cheshire/parse-string 
    (apply str 
           (-> 
             (-fetch-url url)
             first
             :content
             first
             :content
             ))))

(defn -read-munis [id]   
  (-read-json (str base-url municipalities-path id)))

(defn -read-suburbs [id]
  (-read-json (str base-url suburbs-path id)))

(defn muni-map [province-id]
  (let 
    [muni-json (-read-munis province-id)
     muni-names (map #(get %1 "Text") muni-json)
     muni-ids (map #(get %1 "Value") muni-json)]
    (zipmap muni-ids muni-names)))

(defn munis-with-provinces 
  [province-id]
  (let [province-name (province-names province-id)
        munis (muni-map province-id)]
    (for 
      [m (vec munis)]
      (flatten [province-id province-name m]))))

(defn suburb-map [muni-id]
  (let 
    [suburb-json ((-read-suburbs muni-id) "Results")
     suburb-names (map #(get %1 "text") suburb-json)
     suburb-ids (map #(get %1 "id") suburb-json)]
    (zipmap suburb-ids suburb-names)))

(defn tot-set [muni-id]
  (let 
    [suburb-json ((-read-suburbs muni-id) "Results")]
    (set (map #(get %1 "Tot") suburb-json))))

(defn suburb-map2 [muni-id]
  (let 
    [suburb-json ((-read-suburbs muni-id) "Results")]
    (into [] (map #(into [] (vals %1)) suburb-json))))

;((-read-suburbs 9) "Results")
;(tot-set 9)


;gets a set of all "Tot" values for all suburbs across the country
(apply clojure.set/union 
       (let 
         [province-ids (keys province-names)
          muni-ids (flatten 
                     (for [province-id province-ids] 
                       (->
                         (muni-map province-id)
                         (keys)
                         )))]
         (for [muni-id muni-ids] 
           (tot-set muni-id))))



(suburb-map2 9)

(defn -to-dt [date-str]  
  (f/parse 
    (f/formatter loadshed-day-date-format) 
    (str (apply str (drop 5 date-str)) " " (-> (l/local-now) (l/format-local-time :year)))))

(defn -select-loadshed-days[loadshed-resource]
  (html/select 
    (html/select loadshed-resource [[:div.scheduleDay (html/has [:a])]]) [:div.dayMonth]))

(defn -select-loadshed-times[loadshed-resource]
  (html/select 
    (html/select loadshed-resource [[:div.scheduleDay (html/has [:a])]]) [:a]))

(defn loadshed-schedule 
  [schedule-html]  
  (let [html-source (html/html-resource (java.io.StringReader. schedule-html))
        loadshed-days (map -to-dt
                           (map clojure.string/trim 
                                (map html/text 
                                     (-select-loadshed-days html-source))))
        
        loadshed-times (map html/text 
                            (-select-loadshed-times html-source))]
    (zipmap loadshed-days loadshed-times)))

;(f/show-formatters)

(loadshed-schedule sched-source)

(-fetch-url (str base-url suburbs-path 45))

(muni-map 9)

(suburb-map 9)

(cheshire/generate-string (suburb-map 21))

(cheshire/generate-stream (suburb-map 9) (clojure.java.io/writer "/home/neil/personal/workspaces/clojure/eskom-scraper/resources/suburbs.json"))

;"21666":"Bakensklip"
;http://loadshedding.eskom.co.za/LoadShedding/GetScheduleM/21666/1/Western%20Cape/159


(spit "/home/neil/personal/workspaces/clojure/eskom-scraper/resources/municipalities.txt" "Province ID,Province,Municipality ID,Municipality\n")

(for [province-id (keys province-names)]
  (spit "/home/neil/personal/workspaces/clojure/eskom-scraper/resources/municipalities.txt" 
        (str (clojure.string/join "\n" 
                          (map #(str (clojure.string/join "," %1)) 
                               (munis-with-provinces province-id))) "\n") :append true))

