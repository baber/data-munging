(ns event-data-munger.core
  (:gen-class)
    (:use
      [clojure.walk :only [keywordize-keys]]
      [clj-http.client :as clj-http :rename {get http-get}]
      [cheshire.core :as cheshire]
  )
  )



(defn create-strategy-mappings [events] (into {} (for [x (filter #(= "createDelinquencyCase" (:eventType %)) events)]
             [(:accountNumber x) (:strategy x)] ) )  )

(defn create-session-mappings [events] (into {} (for [x (filter #(= "login" (:eventType %)) events)]
             [(:accountNumber x) (:sessionId x)] ) )  )


(defn propagate-data [data-key lookup event]
    (let [account-number (:accountNumber event)]
    (assoc event data-key (get lookup account-number))
    )
  )

(defn process-events [esUrl]
  (let [events (keywordize-keys(map :_source (:hits (:hits (:body (http-get esUrl {:as :json}))) )) )
        account-to-strategy (create-strategy-mappings events)
        account-to-session (create-session-mappings events)
        ]
    (map
     (comp
      (partial propagate-data :strategy account-to-strategy)
      (partial propagate-data :sessionId account-to-session)) events)

    )
  )


(defn -main
  "Fetches events from ElasticSearch and propagates session id and strategy through all of
  them and then writes them to a file."
  [& args]
  (let [modified-events (process-events (first args))]
    (generate-stream modified-events (clojure.java.io/writer (second args)))
    )
  )


;; (-main "http://localhost:9200/collections/events/_search?size=500" "/Users/baberkhalil/tmp/events.json")
