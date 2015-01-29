(ns clojure-web-scraper.core
  (:require [net.cgrand.enlive-html :as html]
            [clojure.java.jdbc :as j]
            [cheshire.core :as cheshire]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.local :as l]
            [clojure.java.io :as io])
  ;(:require [cheshire.core :as cheshire])
  ;(:require [clj-time.core :as t])
  ;(:require [clj-time.format :as f])
  ;(:require [clj-time.local :as l])
  ;(:require [clojure.java.io :as io])
  )

(def base-url "http://loadshedding.eskom.co.za/LoadShedding")
(def municipalities-path "/GetMunicipalities/?Id=")
(def suburbs-path "/GetSurburbData/?pageSize=1000&pageNum=1&searchTerm=&id=")

;http://loadshedding.eskom.co.za/LoadShedding/GetScheduleM/21666/1/Western%20Cape/159
(defn- make-schedule-path 
  "builds the URL format used to access a single load-shedding schedule for a suburb"
  ([suburb-id seriousness province-name tot-id]
  (let [encoded-province-name (clojure.string/replace province-name " " "%20")] ;encoding hack
    (->>
      [base-url "GetScheduleM" suburb-id seriousness encoded-province-name tot-id]
      (interpose "/")
      (apply str))))
  ([[suburb-id province-name tot-id] seriousness]
    (let [encoded-province-name (clojure.string/replace province-name " " "%20")] ;encoding hack
    (->>
      [base-url "GetScheduleM" suburb-id seriousness encoded-province-name tot-id]
      (interpose "/")
      (apply str)))))

(def loadshed-day-date-format "dd MMM YYYY")

(def province-names
  {1 "Eastern Cape" 2 "Free State" 3 "Gauteng" 4 "KwaZulu-Natal" 5 "Limpopo" 6 "Mpumalanga" 7 "North West" 8 "Northern Cape" 9 "Western Cape"})

; change this to hitting the web when it's ready
(def sched-source
  (slurp (io/file (io/resource "bakensklip-schedule.html"))))

; this collection of schedules might be wrong
;(def schedules
;  #{0 453 2030 586 410 2475 443 249 2156 70 218 648 774 580 1230 164 282 468 756
;    830 154 454 770 2226 397 490 763 269 165 1980 615 1590 415 239 77 405 897 776
;    1968 267 319 329 819 464 307 290 1144 3852 2145 234 1470 242 394 159 540 495
;    407 2970 825 1476 483 174 331 284 708 256 657 785 954 656 241 314 420 4293
;    1431 304 401 600 810 1000 108 156 308 365 1386 500 168 3021 1815 292 216 498
;    375 1650 640 247 328 391 990 474 528 303 1232 522 162 426 477 351 243 131 413
;    800 369 258 250 616 1155 2385 538 411 1540 323 248 3300 494 2805 261 1908 1725
;    489 166 447 252 325 146 228 125 312 1148 663 332 330 544 642 435 1743 484 984
;    625 870 1272 1100 566 82 492 926 452 339 431 462 546 1749 289 730 699 1076 1113
;    732 324 617 513 705 83 455 449 45 78 480 123 807 795 441 320 288 81 636 423 574
;    87 160 696 738 486 336 660 272 114 1236 628 655 318 1092 579 1078 861 960 379
;    4290 1479 246 2304 820 390 84})

(defn- fetch-url [url]
  (with-open [inputstream (-> (java.net.URL. url)
                            .openConnection
                            (doto (.setRequestProperty "User-Agent" "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0"))
                            .getContent)]
    (html/html-resource inputstream)))

(defn- read-json[url]
  (cheshire/parse-string 
    (apply str 
           (-> 
             (fetch-url url)
             first
             :content
             first
             :content
             ))))

(defn- read-munis [id]   
  (read-json (str base-url municipalities-path id)))

(defn- read-suburbs [id]
  (read-json (str base-url suburbs-path id)))

(defn muni-map [province-id]
  (let 
    [muni-json (read-munis province-id)
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
    [suburb-json ((read-suburbs muni-id) "Results")
     suburb-names (map #(get %1 "text") suburb-json)
     suburb-ids (map #(get %1 "id") suburb-json)]
    (zipmap suburb-ids suburb-names)))

(defn tot-set [muni-id]
  (let 
    [suburb-json ((read-suburbs muni-id) "Results")]
    (set (map #(get %1 "Tot") suburb-json))))

(defn suburb-map2 
  "Returns a vector of suburb tuples - ID, Name, schedule-id"
  [muni-id]
  (let 
    [suburb-json ((read-suburbs muni-id) "Results")]
    (into [] (map #(into [] (vals %1)) suburb-json))))


;(let [muni-id 9
;      province-name "Western Cape"]
;  (map #(make-schedule-path [(first %1) province-name (nth %1 2)] 1) (suburb-map2 muni-id)))


;(fetch-url "http://loadshedding.eskom.co.za/LoadShedding/GetScheduleM/67993/1/Western%20Cape/159")

;((read-suburbs 9) "Results")
;(tot-set 9)


;gets a set of all "Tot" values for all suburbs across the country
;(apply clojure.set/union 
;       (let 
;         [province-ids (keys province-names)
;          muni-ids (flatten 
;                     (for [province-id province-ids] 
;                       (->
;                         (muni-map province-id)
;                         (keys)
;                         )))]
;         (for [muni-id muni-ids] 
;           (tot-set muni-id))))



;(suburb-map2 9)

(defn- to-dt [date-str]  
  (f/parse 
    (f/formatter loadshed-day-date-format) 
    (str (apply str (drop 5 date-str)) " " (-> (l/local-now) (l/format-local-time :year)))))

(defn- select-loadshed-days[loadshed-resource]
  (html/select 
    (html/select loadshed-resource [[:div.scheduleDay (html/has [:a])]]) [:div.dayMonth]))

(defn- select-loadshed-times[loadshed-resource]
  (html/select 
    (html/select loadshed-resource [[:div.scheduleDay (html/has [:a])]]) [:a]))

(defn- make-times[[date start-end-times]]
(let 
  [coerced-times (map #(map (fn[x](read-string x)) %) (map #(clojure.string/split %1 #":") (clojure.string/split start-end-times #" - ")))
   build-new-date (fn[time] 
                    (apply t/date-time (concat [(t/year date) (t/month date) (t/day date)] time)))]  
  (map build-new-date coerced-times)
))

(defn loadshed-schedule 
  [schedule-html]  
  (let [html-source (html/html-resource (java.io.StringReader. schedule-html))
        loadshed-days (map to-dt
                           (map clojure.string/trim 
                                (map html/text 
                                     (select-loadshed-days html-source))))
        
        loadshed-times (map html/text
                            (select-loadshed-times html-source))
        schedule-tuples (map #(into [] [(key %1) (val %1)]) (zipmap loadshed-days loadshed-times))]
    (map make-times schedule-tuples)
    ))

;(loadshed-schedule (slurp "http://loadshedding.eskom.co.za/LoadShedding/GetScheduleM/67993/1/Western%20Cape/159")) 

(def bakensklip-schedule (loadshed-schedule (slurp (make-schedule-path [15268 "Western Cape" 162] 1))))

bakensklip-schedule

;(apply t/date-time [1986 10 14 4 3 27 456])

;(f/show-formatters)

;(loadshed-schedule sched-source)

;(fetch-url (str base-url suburbs-path 45))

;(muni-map 9)

;(suburb-map 9)

;(cheshire/generate-string (suburb-map 21))

;(cheshire/generate-stream (suburb-map 9) (clojure.java.io/writer (io/resource "suburbs.json")))

;"21666":"Bakensklip"
;http://loadshedding.eskom.co.za/LoadShedding/GetScheduleM/21666/1/Western%20Cape/159

;create the INSERT statement for all suburbs (you little beauty!)
;(spit (io/file (io/resource "insert_suburbs.txt")) "")
;(for [muni-idx (range 1 263)]
;  (spit (io/file (io/resource "insert_suburbs.txt"))
;        (apply str 
;               (map #(str "insert into suburb (suburb_id, name, tot_id, municipality_id) values(" %1 ");\n")
;                    (map #(clojure.string/join "," [(first %1) (str "\"" (nth %1 1) "\"") (nth %1 2) (nth %1 3)]) 
;                         (map #(conj %1 muni-idx) (suburb-map2 muni-idx))))) :append true))

;(map #(str "insert into suburb (suburb_id, name, tot_id, municipality_id) values(" (clojure.string/join "," %1) ");" ) (suburb-map2 9))

;create the CSV file for all municipalities across all provinces
;(spit (io/file (io/resource "municipalities.txt")) "Province ID,Province,Municipality ID,Municipality\n")
;(for [province-id (keys province-names)]
;  (spit (io/file (io/resource "municipalities.txt")) 
;        (str (clojure.string/join "\n" 
;                          (map #(str (clojure.string/join "," %1)) 
;                               (munis-with-provinces province-id))) "\n") :append true))

;create the INSERT statement for all provinces
;(for [province-id (keys province-names)]
;  (spit (io/file (io/resource "insert_provinces.txt")) 
;        (str (clojure.string/join "\n" 
;                                  (map #(str "insert into municipality (province_id, municipality_id, name) values (" (clojure.string/join "," [(first %1) (nth %1 2) (str "\"" (nth %1 3) "\"" )]) ");") 
;                                       (munis-with-provinces province-id))) "\n") :append true))
